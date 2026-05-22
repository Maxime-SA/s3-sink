use crate::envelopes::{ToUpload, UploadResult};
use crate::error::SinkError;
use crate::files::FileRegistry;
use crate::json_serializer::JsonSerializer;
use crate::kafka_consumer::{CustomContext, init_kafka_consumer};
use crate::offset_registry::OffsetRegistry;
use crate::record::{StreamId, StreamIdCache};
use crate::stats::Stats;
use crate::uploader::Uploader;
use crate::{BoxFuture, Result, RouterStrategy, SinkConfig, TimersConfig, TopicConfig};
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use std::collections::HashMap;
use std::fs;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::time::{Interval, interval};
use tracing::{debug_span, error, info};

type S3UploadPool = FuturesUnordered<BoxFuture>;

pub struct Sink<'a> {
    config: &'a SinkConfig, // configuration for the sink connector, how can we update this at runtime
    file_registry: FileRegistry, // file registry for active file writers
    offset_registry: OffsetRegistry, // commit registry to track offsets that have been uploaded
    upload_pool: S3UploadPool, // pool of futures that upload files to S3
    timer_interrupts: TimerInterrupts, // timer interrupts to handle specific tasks
    stream_ids: StreamIdCache, // StreamIds cache
    topics_config: TopicConfigCache, // topic name and config cache
    stats: Stats,
}
impl<'a> Sink<'a> {
    pub fn new(config: &'a SinkConfig) -> Self {
        info!("initializing FileRegistry");
        let file_registry = FileRegistry::new(
            &config.files.scratch_directory,
            config.files.compression_level,
        );

        info!("initializing OffsetRegistry");
        let offset_registry = OffsetRegistry::new();

        info!("initializing S3UploadPool");
        let upload_pool = FuturesUnordered::new();

        info!("initializing TimerInterrupts");
        let timer_interrupts = TimerInterrupts::new(&config.timers);

        info!("initializing StreamIdCache");
        let stream_ids = StreamIdCache::new();

        info!("initializing TopicConfigCache");
        let topics_config = TopicConfigCache::new(config);

        info!("initializing Stats");
        let stats: Stats = Stats::new();

        Self {
            config,
            file_registry,
            offset_registry,
            topics_config,
            upload_pool,
            timer_interrupts,
            stream_ids,
            stats,
        }
    }

    pub async fn event_loop<U: Uploader>(mut self, uploader: U) -> Result<()> {
        let mut serializer: JsonSerializer = JsonSerializer::new();

        let consumer = init_kafka_consumer(&self.config.kafka)?;

        loop {
            select! {
                /*
                select! is a macro which polls the async expressions, first one to be ready wins, others are canceled which need to be idempotent to not lose state

                default behaviour: randomly poll the async expressions one after the other
                'biased' behaviour: polls the async expressions sequentially
                 */
                biased;

                // 1. an upload to S3 has completed
                Some(result) = self.upload_pool.next() => {
                    let _span = debug_span!("upload_result").entered();
                    self.process_upload_result(result, &uploader)?;
                }

                // 2. timer interrupt to commit offsets
                _ = self.timer_interrupts.commit_tick.tick() => {
                    let _span = debug_span!("commit_tick").entered();
                    self.process_commit_tick(&consumer)?;
                }

                // 3. timer interrupt to upload any dormant files
                _ = self.timer_interrupts.upload_tick.tick() => {
                    let _span = debug_span!("upload_tick").entered();
                    self.process_upload_tick(&uploader)?;
                }

                // 4. timer interrupt to review topic ingestion budget
                // _ = self.timer_interrupts.fairness_scheduler_tick.tick() => {
                //     self.process_fairness_scheduler_tick(&consumer)?;
                // }

                // 5. process Kafka record
                maybe_next_record = consumer.recv() => {
                    let msg = maybe_next_record?;
                    let _span = debug_span!(
                        "process_record",
                        topic = msg.topic(),
                        payload_size_b = msg.payload().map_or(0, |p| p.len()),
                    ).entered();
                    self.process_record(&msg, &mut serializer, &uploader)?;
                }
            }
        }
    }

    fn process_commit_tick(&mut self, consumer: &StreamConsumer<CustomContext>) -> Result<()> {
        self.stats.print_report(
            self.file_registry.active_file_count(),
            self.upload_pool.len() as u64,
        );

        let offsets_to_commit = self.offset_registry.committable_offsets()?;
        if offsets_to_commit.count() > 0 {
            consumer.commit(&offsets_to_commit, rdkafka::consumer::CommitMode::Async)?;
        }
        Ok(())
    }

    fn process_upload_tick<U: Uploader>(&mut self, uploader: &U) -> Result<()> {
        let cut_off =
            Instant::now() - Duration::from_millis(self.config.uploads.max_active_file_timeout_ms);

        for id in self.file_registry.files_older_than(cut_off) {
            self.seal_and_upload(&id, uploader)?;
        }

        Ok(())
    }

    fn process_fairness_scheduler_tick(
        &mut self,
        consumer: &StreamConsumer<CustomContext>,
    ) -> Result<()> {
        todo!()
    }

    fn process_upload_result<U: Uploader>(
        &mut self,
        result: UploadResult,
        uploader: &U,
    ) -> Result<()> {
        match result {
            // can we add backoff here or a max retry?
            UploadResult::Failure(to_upload, sink_error) => {
                error!("UploadResult::Failure: {:?}", sink_error);
                self.stats.uploads_failed += 1;
                self.upload_pool.push(uploader.upload(to_upload));
            }
            UploadResult::Success(file_to_gc, offsets) => {
                self.stats.uploads_ok += 1;
                self.offset_registry.add_uploaded(offsets);
                let _ = fs::remove_file(file_to_gc);
            }
        }
        Ok(())
    }

    /*
    Simple rate limiter for the number of in-flight uploads.
    Purpose is to limit the amount of memory used for uploads.
     */
    fn ready_to_upload(&self, raw_size_b: u64) -> bool {
        raw_size_b >= self.config.files.target_file_size_b
            && (self.upload_pool.len() as u64) < self.config.uploads.max_concurrent_uploads
    }

    fn process_record<U: Uploader>(
        &mut self,
        record: &BorrowedMessage<'_>,
        serializer: &mut JsonSerializer,
        uploader: &U,
    ) -> Result<()> {
        let (topic_ref, topic_config) = self.topics_config.get_by_topic_name(record.topic())?;

        let stream_id = self.stream_ids.get(record, &topic_config.router);

        self.offset_registry.add_consumed(
            &stream_id,
            topic_ref,
            record.partition(),
            record.offset(),
        );

        if let Some(bytes) = serializer.serialize(record, &topic_config.decoder)? {
            self.stats.records += 1;
            self.stats.bytes += bytes.len() as u64;

            self.file_registry.write_all(&stream_id, bytes)?;

            if self.ready_to_upload(self.file_registry.raw_file_size_b(&stream_id)?) {
                self.seal_and_upload(&stream_id, uploader)?;
                self.stats.seals += 1;
            }
        }

        // todo: fairness scheduler

        Ok(())
    }

    fn seal_and_upload<U: Uploader>(&mut self, id: &StreamId, uploader: &U) -> Result<()> {
        let sealed_file = self.file_registry.seal(id)?;
        let sealed_offsets = self.offset_registry.seal(id)?;

        let router = self.topics_config.get_router_by_stream_id(id)?;

        self.upload_pool.push(uploader.upload(ToUpload::new(
            router.partition_spec(id),
            sealed_file,
            sealed_offsets,
        )));

        Ok(())
    }
}

struct TimerInterrupts {
    fairness_scheduler_tick: Interval,
    commit_tick: Interval,
    upload_tick: Interval,
}
impl TimerInterrupts {
    fn new(config: &TimersConfig) -> Self {
        // manage per-topic consumption budget
        let fairness_scheduler_tick =
            interval(Duration::from_millis(config.fairness_scheduler_tick_ms));

        // commit accumulated offsets
        let commit_tick = interval(Duration::from_millis(config.commit_tick_ms));

        // upload dormant files
        let upload_tick = interval(Duration::from_millis(config.upload_tick_ms));

        TimerInterrupts {
            fairness_scheduler_tick,
            commit_tick,
            upload_tick,
        }
    }
}

struct TopicConfigCache {
    topic_cache: HashMap<Rc<str>, TopicConfig>, // map of topic_name -> topic_config
    id_cache: HashMap<StreamId, TopicConfig>,   // map of stream_id -> topic_config
}
impl TopicConfigCache {
    fn new(config: &SinkConfig) -> Self {
        let mut topic_cache = HashMap::new();

        for (topic_config, topics) in &config.kafka.input_topics {
            for topic in topics {
                topic_cache.insert(topic.clone(), *topic_config);
            }
        }

        Self {
            topic_cache,
            id_cache: HashMap::new(),
        }
    }

    fn get_by_topic_name(&self, topic_name: &str) -> Result<(&Rc<str>, &TopicConfig)> {
        self.topic_cache.get_key_value(topic_name).ok_or_else(|| {
            SinkError::Configuration(format!("missing topic configuration for '{topic_name}'"))
        })
    }

    fn get_router_by_stream_id(&self, id: &StreamId) -> Result<&RouterStrategy> {
        self.id_cache.get(id).map(|v| &v.router).ok_or_else(|| {
            SinkError::Configuration(format!("missing topic configuration for '{id}'"))
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
