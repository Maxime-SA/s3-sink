use crate::file_registry::FileRegistry;
use crate::kafka_consumer::{SpecialContext, init_kafka_consumer};
use crate::offset_registry::{OffsetRegistry, TopicOffsets};
use crate::processor::Processor;
use crate::uploader::Uploader;
use crate::{BoxFuture, Result, SinkConfig, TimersConfig, UploadResult};
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::cmp::Reverse;
use std::fs;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::time::{Interval, interval};

pub struct Sink<'a, U>
where
    U: Uploader,
{
    config: &'a SinkConfig, // configuration for the sink connector, how can we update this at runtime
    file_registry: FileRegistry<'a>, // file registry for active file writers
    commit_registry: OffsetRegistry, // commit registry to track consumed and committable offsets
    processor: Processor<'a, U>, // record processor
    upload_ftrs: FuturesUnordered<BoxFuture>, // pool of futures that upload files to S3
    timer_interrupts: TimerInterrupts, // timer interrupts to handle specific tasks
}

impl<'a, U> Sink<'a, U>
where
    U: Uploader,
{
    pub fn new(config: &'a SinkConfig, uploader: U) -> Self {
        let file_registry = FileRegistry::new(
            config.files.scratch_directory.as_path(),
            config.files.compression_level,
        );

        let commit_registry = OffsetRegistry::new();

        let processor = Processor::new(
            uploader,
            &config.kafka.input_topics,
            config.files.target_file_size_b,
            config.uploads.max_concurrent_uploads,
        );

        let upload_ftrs = FuturesUnordered::new();

        let timer_interrupts = TimerInterrupts::new(&config.timers);

        Self {
            config,
            file_registry,
            commit_registry,
            processor,
            upload_ftrs,
            timer_interrupts,
        }
    }

    pub fn run(self) -> Result<()> {
        let runtime = Self::init_tokio_runtime()?;
        Ok(runtime.block_on(self.event_loop())?)
    }

    async fn event_loop(mut self) -> Result<()> {
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
                    self.process_upload_completion(result)?;
                }

                // 2. timer interrupt to commit offsets
                _ = self.timer_interrupts.commit_tick.tick() => {
                    self.commit_offsets(&consumer)?;
                }

                // 3. timer interrupt to upload any dormant files
                _ = self.timer_interrupts.upload_tick.tick() => {
                    todo!()
                }

                // 4. timer interrupt to review topic ingestion budget
                _ = self.timer_interrupts.fairness_scheduler_tick.tick() => {
                    self.processor.reset_ingestion_budgets(&consumer);
                }

                // 5. process Kafka record
                maybe_next_record = consumer.recv() => {
                    self.processor.process_record(&maybe_next_record?, &mut self.file_registry, &mut self.upload_ftrs)?;
                }

            }
        }
    }

    fn commit_offsets(&mut self, consumer: &StreamConsumer<SpecialContext>) -> Result<()> {
        let mut offsets_to_commit = TopicPartitionList::new();

        let mut keys_to_remove = vec![];

        let uploaded_offsets = self.commit_registry.get_mut_offsets();

        for ((topic, partition), offsets) in &mut *uploaded_offsets {
            // continue if offsets is empty
            let Some(Reverse(first)) = offsets.peek().copied() else {
                keys_to_remove.push((topic.clone(), *partition));
                continue;
            };

            /*
            Find the first contiguous offset which is not present in the offsets that we have uploaded.
            This is the offset at which a consumer should restart on crash.
             */
            let mut next_expected = first;
            while let Some(Reverse(offset)) = offsets.peek()
                && *offset == next_expected
            {
                offsets.pop();
                next_expected += 1;
            }

            offsets_to_commit.add_partition_offset(
                topic,
                *partition,
                rdkafka::Offset::Offset(next_expected),
            )?;

            if offsets.is_empty() {
                keys_to_remove.push((topic.clone(), *partition));
            }
        }

        for key in keys_to_remove {
            uploaded_offsets.remove(&key);
        }

        consumer.commit(&offsets_to_commit, rdkafka::consumer::CommitMode::Async)?;

        Ok(())
    }

    fn process_upload_completion(&mut self, result: Result<UploadResult>) -> Result<()> {
        let Ok(upload_result) = result else {
            // we need to track SealedFile or otherwise we will lose the file and the offsets
            return Ok(());
        };

        let (file_to_gc, offsets_to_commit) = upload_result.into_parts();

        self.commit_registry.combine(offsets_to_commit);

        fs::remove_file(file_to_gc)?;

        Ok(())
    }

    fn init_tokio_runtime() -> Result<Runtime> {
        Ok(tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?)
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
