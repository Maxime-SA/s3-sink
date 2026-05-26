use crate::cache::{Cache, StreamId};
use crate::envelopes::{ToUpload, UploadResult};
use crate::error::SinkError;
use crate::files::FileRegistry;
use crate::json_serializer::JsonSerializer;
use crate::kafka_consumer::{CustomContext, init_kafka_consumer};
use crate::offset_registry::OffsetRegistry;
use crate::stats::Stats;
use crate::timer_interrupts::TimerInterrupts;
use crate::uploader::Uploader;
use crate::{BoxFuture, Result, SinkConfig};
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use std::fs;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::signal::unix::SignalKind;
use tracing::{error, info, warn};

/*
Todo:
- Handle Kafka rebalance appropriately
- Separate recoverable from unrecoverable errors
- Backpressure for in-flight uploads, local files, offsets in
registry, ...
- Fairness Scheduler
*/

type S3UploadPool = FuturesUnordered<BoxFuture>;

pub struct Sink<'a> {
    config: &'a SinkConfig, // configuration for the sink connector, how can we update this at runtime
    file_registry: FileRegistry, // file registry for active file writers
    offset_registry: OffsetRegistry, // commit registry to track offsets that have been uploaded
    upload_pool: S3UploadPool, // pool of futures that upload files to S3
    timer_interrupts: TimerInterrupts, // timer interrupts to handle specific tasks
    cache: Cache,
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
        let timer_interrupts = TimerInterrupts::new(config);

        info!("initializing Cache");
        let cache = Cache::new(config);

        info!("initializing Stats");
        let stats: Stats = Stats::new();

        Self {
            config,
            file_registry,
            offset_registry,
            upload_pool,
            timer_interrupts,
            cache,
            stats,
        }
    }

    pub async fn event_loop<U: Uploader>(mut self, uploader: U) -> Result<()> {
        info!("initializing JsonSerializer");
        let mut serializer: JsonSerializer = JsonSerializer::new();

        info!("initializing StreamConsumer");
        let consumer = init_kafka_consumer(&self.config.kafka)?;

        info!("initializing ShutdownHandler");
        let mut shutdown = std::pin::pin!(Self::shutdown_signal());

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

                // 3. timer interrupt to review topic ingestion budget
                // _ = self.timer_interrupts.fairness_scheduler_tick.tick() => {
                //     self.process_fairness_scheduler_tick(&consumer)?;
                // }

                // 5. process Kafka record
                maybe_next_record = consumer.recv() => {
                    self.process_record(&maybe_next_record?, &mut serializer, &uploader)?;
                }

                // 6. shutdown signal
                _ = &mut shutdown => break,
            }
        }

        self.drain_and_commit_sync(&consumer, &uploader).await
    }

    async fn shutdown_signal() -> Result<()> {
        let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
        select! {
            _ = tokio::signal::ctrl_c() => {
                Ok(())
            }
            _ = sigterm.recv() => {
                Ok(())
            }
        }
    }

    async fn drain_and_commit_sync<U: Uploader>(
        &mut self,
        consumer: &StreamConsumer<CustomContext>,
        uploader: &U,
    ) -> Result<()> {
        info!("shutdown signal received, draining and committing");

        while let Some(result) = self.upload_pool.next().await {
            self.process_upload_result(result, uploader)?;
        }

        let offsets = self.offset_registry.committable_offsets()?;
        if offsets.count() > 0 {
            consumer.commit(&offsets, CommitMode::Sync)?;
        }

        Ok(())
    }

    fn process_commit_tick(&mut self, consumer: &StreamConsumer<CustomContext>) -> Result<()> {
        self.stats.print_report(
            self.file_registry.active_file_count(),
            self.upload_pool.len() as u64,
        );

        let offsets_to_commit = self.offset_registry.committable_offsets()?;

        if offsets_to_commit.count() > 0 {
            consumer.commit(&offsets_to_commit, CommitMode::Async)?;
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

    // fn process_fairness_scheduler_tick(
    //     &mut self,
    //     _: &StreamConsumer<CustomContext>,
    // ) -> Result<()> {
    //     todo!()
    // }

    fn process_upload_result<U: Uploader>(
        &mut self,
        result: UploadResult,
        uploader: &U,
    ) -> Result<()> {
        match result {
            // can we add backoff here or a max retry?
            UploadResult::Failure(to_upload, sink_error) => {
                error!("UploadResult::Failure: {:?}", sink_error);

                self.stats.inc_failure_uploads();

                if to_upload.retries() == 0 {
                    return Err(SinkError::S3Upload(
                        "maximum number of retries reached for S3 upload".into(),
                    ));
                }

                self.upload_pool
                    .push(uploader.upload(to_upload.decrement()));
            }
            UploadResult::Success(file_to_gc, offsets) => {
                self.stats.inc_success_uploads();
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
    fn ready_to_upload(&mut self, raw_size_b: u64) -> bool {
        let apply_backpressure =
            (self.upload_pool.len() as u64) > self.config.uploads.max_concurrent_uploads;

        if apply_backpressure {
            self.stats.inc_upload_backpressure();
            warn!("too many in-flight uploads, applying backpressure");
            false
        } else {
            raw_size_b >= self.config.files.target_file_size_b
        }
    }

    fn process_record<U: Uploader>(
        &mut self,
        record: &BorrowedMessage<'_>,
        serializer: &mut JsonSerializer,
        uploader: &U,
    ) -> Result<()> {
        let metadata = self.cache.get_or_create_record_metadata(record)?;

        self.offset_registry.add_consumed(
            &metadata.stream_id,
            metadata.topic_name,
            record.partition(),
            record.offset(),
        );

        if let Some(bytes) = serializer.serialize(record, &metadata.config.decoder)? {
            self.stats.inc_bytes_consumed(bytes.len() as u64);

            self.file_registry.write_all(&metadata.stream_id, bytes)?;

            if self.ready_to_upload(self.file_registry.raw_file_size_b(&metadata.stream_id)?) {
                self.seal_and_upload(&metadata.stream_id, uploader)?;
                self.stats.inc_files_sealed();
            }
        }

        Ok(())
    }

    fn seal_and_upload<U: Uploader>(&mut self, id: &StreamId, uploader: &U) -> Result<()> {
        let sealed_file = self.file_registry.seal(id)?;
        let sealed_offsets = self.offset_registry.seal(id)?;

        let router = self.cache.get_router(id)?;

        self.upload_pool.push(uploader.upload(ToUpload::new(
            router.partition_spec(id),
            sealed_file,
            sealed_offsets,
            self.config.uploads.max_retry,
        )));

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
