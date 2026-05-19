use crate::file_registry::FileRegistry;
use crate::json_serializer::JsonSerializer;
use crate::kafka_consumer::{SpecialContext, init_kafka_consumer};
use crate::offset_registry::OffsetRegistry;
use crate::processor::Processor;
use crate::uploader::Uploader;
use crate::{Result, SinkConfig, TimersConfig, UploadResult};
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::fs;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::time::{Interval, interval};

pub struct Sink {
    config: SinkConfig,
}

impl Sink {
    pub fn new(config: SinkConfig) -> Self {
        Self { config }
    }

    pub fn start_sink<U: Uploader>(&self, uploader: U) -> Result<()> {
        let runtime = Self::init_tokio_runtime()?;
        Ok(runtime.block_on(self.event_loop(uploader))?)
    }

    async fn event_loop<U: Uploader>(&self, uploader: U) -> Result<()> {
        // how can we update these configurations at runtime?

        // kafka stream consumer
        let consumer = init_kafka_consumer(&self.config.kafka)?;

        // registry for active files
        let mut registry = FileRegistry::new(
            self.config.files.scratch_directory.as_path(),
            self.config.files.compression_level,
        );

        // json serializer
        let mut serializer = JsonSerializer::new();

        // pool of futures that upload files to S3
        let mut upload_ftrs = FuturesUnordered::new();

        // timer interrupts
        let (mut fairness_scheduler_tick, mut commit_tick, mut upload_tick) =
            Self::init_timers(&self.config.timers);

        // record processor
        let mut processor = Processor::new(
            uploader,
            &self.config.kafka.input_topics,
            self.config.files.target_file_size_b,
            self.config.uploads.max_concurrent_uploads,
        );

        // registry to track offsets that can be safely committed
        let mut commit_registry = OffsetRegistry::new();

        loop {
            select! {
                /*
                select! is a macro which polls the async expressions, first one to be ready wins, others are canceled which need to be idempotent to not lose state

                default behaviour: randomly poll the async expressions one after the other
                'biased' behaviour: polls the async expressions sequentially
                 */
                biased;

                // 1. an upload to S3 has completed
                Some(result) = upload_ftrs.next() => {
                    Self::process_upload_result(result, &mut commit_registry)?;
                }

                // 2. timer interrupt to commit offsets
                _ = commit_tick.tick() => {
                    todo!()
                }

                // 3. timer interrupt to upload any dormant files
                _ = upload_tick.tick() => {
                    todo!()
                }

                // 4. timer interrupt to review topic ingestion budget
                _ = fairness_scheduler_tick.tick() => {
                    processor.reset_ingestion_budgets(&consumer);
                }

                // 5. process Kafka record
                maybe_next_record = consumer.recv() => {
                    processor.process(&maybe_next_record?, &mut registry, &mut upload_ftrs, &mut serializer)?;
                }

            }
        }
    }

    fn commit_offsets(
        consumer: &StreamConsumer<SpecialContext>,
        registry: &mut OffsetRegistry,
    ) -> Result<()> {
        let offsets = registry.offsets();

        let mut topic_partition_list = TopicPartitionList::new();

        for ((topic, partition), offsets) in offsets.iter() {
            topic_partition_list.add
        }

        consumer.commit(topic_partition_list, rdkafka::consumer::CommitMode::Async);

        Ok(())
    }

    fn process_upload_result(
        result: Result<UploadResult>,
        registry: &mut OffsetRegistry,
    ) -> Result<()> {
        if let Ok(upload_result) = result {
            let (file_to_gc, offsets_to_commit) = upload_result.into_parts();

            // accumulate offsets
            registry.combine(offsets_to_commit);

            // garbage collect
            fs::remove_file(file_to_gc)?;
        } else {
            // we need to keep track of the SealedFile or it will be lost
        }

        Ok(())
    }

    fn init_timers(config: &TimersConfig) -> (Interval, Interval, Interval) {
        // manage per-topic consumption budget
        let fairness_scheduler_tick =
            interval(Duration::from_millis(config.fairness_scheduler_tick_ms));

        // commit accumulated offsets
        let commit_tick = interval(Duration::from_millis(config.commit_tick_ms));

        // upload dormant files
        let upload_tick = interval(Duration::from_millis(config.upload_tick_ms));

        (fairness_scheduler_tick, commit_tick, upload_tick)
    }

    fn init_tokio_runtime() -> Result<Runtime> {
        Ok(tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?)
    }
}
