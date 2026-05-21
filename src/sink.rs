use crate::envelopes::{ToUpload, UploadResult};
use crate::error::SinkError;
use crate::files::FileRegistry;
use crate::json_serializer::JsonSerializer;
use crate::kafka_consumer::{SpecialContext, init_kafka_consumer};
use crate::offset_registry::OffsetRegistry;
use crate::record::StreamId;
use crate::uploader::Uploader;
use crate::{BoxFuture, Result, SinkConfig, TimersConfig, TopicConfig};
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use std::collections::HashMap;
use std::fs;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::time::{Interval, interval};

pub struct Sink<'a> {
    config: &'a SinkConfig, // configuration for the sink connector, how can we update this at runtime
    topics_config: HashMap<&'a str, &'a TopicConfig>,
    file_registry: FileRegistry<'a>, // file registry for active file writers
    offset_registry: OffsetRegistry, // commit registry to track offsets that have been uploaded
    upload_ftrs: FuturesUnordered<BoxFuture>, // pool of futures that upload files to S3
    timer_interrupts: TimerInterrupts, // timer interrupts to handle specific tasks
}

impl<'a> Sink<'a> {
    pub fn new(config: &'a SinkConfig) -> Self {
        let file_registry = FileRegistry::new(
            config.files.scratch_directory.as_path(),
            config.files.compression_level,
        );

        let offset_registry = OffsetRegistry::new();

        let topics_config =
            config
                .kafka
                .input_topics
                .iter()
                .fold(HashMap::new(), |mut acc, (configs, topics)| {
                    topics.iter().for_each(|topic| {
                        acc.insert(topic.as_str(), configs);
                    });
                    acc
                });

        let upload_ftrs = FuturesUnordered::new();

        let timer_interrupts = TimerInterrupts::new(&config.timers);

        Self {
            config,
            file_registry,
            offset_registry,
            topics_config,
            upload_ftrs,
            timer_interrupts,
        }
    }

    pub fn run<U: Uploader>(self, uploader: U) -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(runtime.block_on(self.event_loop(uploader))?)
    }

    async fn event_loop<U: Uploader>(mut self, uploader: U) -> Result<()> {
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
                Some(result) = self.upload_ftrs.next() => {
                    self.process_upload_result(result, &uploader)?;
                }

                // 2. timer interrupt to commit offsets
                _ = self.timer_interrupts.commit_tick.tick() => {
                    self.process_commit_tick(&consumer)?;
                }

                // 3. timer interrupt to upload any dormant files
                _ = self.timer_interrupts.upload_tick.tick() => {
                    self.process_upload_tick(&uploader)?;
                }

                // 4. timer interrupt to review topic ingestion budget
                // _ = self.timer_interrupts.fairness_scheduler_tick.tick() => {
                //     self.process_fairness_scheduler_tick(&consumer)?;
                // }

                // 5. process Kafka record
                maybe_next_record = consumer.recv() => {
                    self.process_record(&maybe_next_record?, &mut serializer, &uploader)?;
                }
            }
        }
    }

    // need to review this method, how can we track committed offsets until we have confirmation from Kafka that they have been committed
    fn process_commit_tick(&mut self, consumer: &StreamConsumer<SpecialContext>) -> Result<()> {
        let offsets_to_commit = self.offset_registry.commit()?;
        consumer.commit(&offsets_to_commit, rdkafka::consumer::CommitMode::Async)?;
        Ok(())
    }

    fn process_upload_tick<U: Uploader>(&mut self, uploader: &U) -> Result<()> {
        let cut_off =
            Instant::now() - Duration::from_mins(self.config.uploads.max_active_file_timeout_m);

        for id in self.file_registry.files_older_than(cut_off) {
            self.seal_and_upload(&id, uploader)?;
        }

        Ok(())
    }

    fn process_fairness_scheduler_tick(
        &mut self,
        consumer: &StreamConsumer<SpecialContext>,
    ) -> Result<()> {
        todo!()
    }

    fn process_upload_result<U: Uploader>(
        &mut self,
        result: UploadResult,
        uploader: &U,
    ) -> Result<()> {
        match result {
            UploadResult::Failure(to_upload) => self.upload_ftrs.push(uploader.upload(to_upload)),
            UploadResult::Success(file_to_gc, offsets) => {
                self.offset_registry.add_uploaded(offsets);
                fs::remove_file(file_to_gc)?;
            }
        }
        Ok(())
    }

    /*
    Simple rate limiter for the number of in-flight uploads.
    Purpose is to limit the amount of memory used for uploads.
     */
    fn ready_to_upload(&self, raw_size_b: usize) -> bool {
        raw_size_b >= self.config.files.target_file_size_b
            && self.upload_ftrs.len() < self.config.uploads.max_concurrent_uploads
    }

    fn process_record<U: Uploader>(
        &mut self,
        record: &BorrowedMessage<'_>,
        serializer: &mut JsonSerializer,
        uploader: &U,
    ) -> Result<()> {
        let topic_name = record.topic();

        let topic_config = self.topics_config.get(topic_name).copied().ok_or_else(|| {
            SinkError::ConfigurationError(format!("missing topic configuration for '{topic_name}'"))
        })?;

        let stream_id = &topic_config.router.id(record);

        self.offset_registry.add_consumed(
            &stream_id,
            record.topic(),
            record.partition(),
            record.offset(),
        );

        if let Some(bytes) = serializer.serialize(record, &topic_config.decoder)? {
            self.file_registry.write_all(stream_id, bytes)?;

            if self.ready_to_upload(self.file_registry.file_size(stream_id)?) {
                self.seal_and_upload(stream_id, uploader)?;
            }
        }

        // Topic fairness scheduler ...

        Ok(())
    }

    fn seal_and_upload<U: Uploader>(&mut self, id: &StreamId, uploader: &U) -> Result<()> {
        let sealed_file = self.file_registry.seal(&id)?;
        let sealed_offsets = self.offset_registry.seal(&id)?;

        self.upload_ftrs
            .push(uploader.upload(ToUpload::new(sealed_file, sealed_offsets)));

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

#[cfg(test)]
mod test {
    use super::*;
}
