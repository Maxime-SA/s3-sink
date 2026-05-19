use crate::file_io::FilesState;
use crate::kafka_consumer::init_kafka_consumer;
use crate::processor::Processor;
use crate::uploader::Uploader;
use crate::{Result, SinkConfig, TimersConfig};
use futures::stream::{FuturesUnordered, StreamExt};
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

        // local files state
        let mut files_state = FilesState::new(
            self.config.files.scratch_directory.as_path(),
            self.config.files.compression_level,
        );

        /*
        3 timer interrupts:
            - fairness_scheduler: manage per-topic consumption budget
            - commit: commit accumulated offsets
            - upload: upload dormant files
         */
        let (mut fairness_scheduler_tick, mut commit_tick, mut upload_tick) =
            Self::init_timers(&self.config.timers);

        // pool of futures that upload files to S3
        let mut upload_ftrs = FuturesUnordered::new();

        // record processor
        let mut processor = Processor::new(
            uploader,
            &self.config.kafka.input_topics,
            self.config.files.target_file_size_bytes,
            self.config.uploads.max_concurrent_uploads,
        );

        loop {
            select! {
                /*
                select! is a macro which polls the async expressions, first one to be ready wins, others are canceled which need to be idempotent to not lose state

                default behaviour: randomly poll the async expressions one after the other
                'biased' behaviour: polls the async expressions sequentially
                 */
                biased;

                // 1. an upload to S3 has completed
                Some(upload_result) = upload_ftrs.next() => {
                    todo!()
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
                    processor.process(&maybe_next_record?, &mut files_state, &mut upload_ftrs)?;
                }

            }
        }
    }

    fn init_timers(config: &TimersConfig) -> (Interval, Interval, Interval) {
        let fairness_scheduler_tick =
            interval(Duration::from_millis(config.fairness_scheduler_tick_ms));

        let commit_tick = interval(Duration::from_millis(config.commit_tick_ms));

        let upload_tick = interval(Duration::from_millis(config.upload_tick_ms));

        (fairness_scheduler_tick, commit_tick, upload_tick)
    }

    fn init_tokio_runtime() -> Result<Runtime> {
        Ok(tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?)
    }
}
