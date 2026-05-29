use crate::envelopes::{SealedFile, ToUpload};
use crate::error::SinkError;
use crate::files::FileRegistry;
use crate::json_serializer::JsonSerializer;
use crate::kafka_consumer::{CustomContext, init_kafka_consumer};
use crate::state_machine::{Request, Response, StateMachine};
use crate::stats::Stats;
use crate::timer_interrupts::TimerInterrupts;
use crate::uploader::Uploader;
use crate::{BoxFuture, Result, S3Upload, SinkConfig};
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use std::fs;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info};

/*
Todo:
- Handle Kafka rebalance appropriately
- Separate recoverable from unrecoverable errors
- Backpressure for in-flight uploads, local files, offsets in
registry, ...
- Fairness Scheduler
*/

type S3UploadPool = FuturesUnordered<BoxFuture>;

pub struct Sink;
impl Sink {
    pub async fn start<U: Uploader>(config: &SinkConfig, uploader: U) -> Result<()> {
        info!("initializing FileRegistry");
        let mut file_registry = FileRegistry::new(
            &config.files.scratch_directory,
            config.files.compression_level,
        );

        info!("initializing S3UploadPool");
        let mut upload_pool: S3UploadPool = FuturesUnordered::new();

        info!("initializing TimerInterrupts");
        let mut timer_interrupts = TimerInterrupts::new(config);

        info!("initializing JsonSerializer");
        let mut serializer = JsonSerializer::new();

        info!("initializing ShutdownHandler");
        let mut shutdown = std::pin::pin!(Self::shutdown_signal());

        info!("initializing Stats");
        let mut stats: Stats = Stats::new();

        info!("initializing StateMachine");
        let mut state_machine = StateMachine::new(config);

        info!("initializing RebalanceChannel");
        let (rebalance_tx, mut rebalance_rx) = tokio::sync::mpsc::unbounded_channel();

        info!("initializing StreamConsumer");
        let consumer = init_kafka_consumer(&config.kafka, rebalance_tx)?;

        'event_loop: loop {
            let request = select! {
                /*
                select! is a macro which polls the async expressions, first one to be ready wins, others are canceled which need to be idempotent to not lose state

                default behaviour: randomly poll the async expressions one after the other
                'biased' behaviour: polls the async expressions sequentially
                 */
                biased;

                // 1. an upload to S3 has completed
                Some(result) = upload_pool.next() => Request::UploadCompletion(result),

                // 2. timer interrupt to commit offsets
                _ = timer_interrupts.commit_tick.tick() => Request::CommitTick(consumer.assignment()?),

                // 3. timer interrupt to upload any dormant files
                _ = timer_interrupts.upload_tick.tick() => Request::UploadTick,

                // 4. timer interrupt to review topic ingestion budget
                // _ = timer_interrupts.fairness_scheduler_tick.tick() => Request::FairnessSchedulerTick,

                // 5. process Kafka record
                maybe_next_record = consumer.recv() => Request::ProcessRecord { record: maybe_next_record?, serializer:  &mut serializer},

                // 6. shutdown signal
                _ = &mut shutdown => Request::ShutdownSignal,
            };

            // check rebalance assignment channel
            Self::handle_rebalance(&mut rebalance_rx, &mut state_machine)?;

            for response in state_machine.handle(request) {
                match response {
                    Response::WriteFile(stream_id) => {
                        let payload = serializer.get_payload();

                        stats.inc_bytes_consumed(payload.len() as u64);

                        file_registry.write_all(stream_id, payload)?
                    }

                    Response::SealAndUpload {
                        id,
                        bytes_consumed,
                        records_consumed,
                        offsets_consumed,
                        created_at,
                        retries,
                    } => {
                        stats.inc_files_sealed();

                        let active_file = file_registry.seal(&id)?;

                        let sealed_file = SealedFile::new(
                            active_file,
                            bytes_consumed,
                            records_consumed,
                            offsets_consumed,
                            created_at,
                        );

                        let object_key = S3Upload::partition_spec(&id);

                        let to_upload = ToUpload::new(object_key, sealed_file, retries);

                        upload_pool.push(uploader.upload(to_upload));
                    }

                    Response::RetryUpload(to_upload) => {
                        stats.inc_failure_uploads();

                        upload_pool.push(uploader.upload(to_upload));
                    }

                    Response::CommitAsync(tpl) => {
                        stats.print_report(
                            file_registry.active_file_count(),
                            upload_pool.len() as u64,
                        );

                        Self::handle_commit(&tpl, &consumer, CommitMode::Async)?
                    }

                    Response::CommitSync(tpl) => {
                        Self::handle_commit(&tpl, &consumer, CommitMode::Sync)?
                    }

                    Response::DeleteFile(path_buf) => {
                        stats.inc_success_uploads();

                        let _ = fs::remove_file(path_buf);
                    }

                    Response::DrainAndShutdown => break 'event_loop,

                    Response::Fatal(sink_error) => Self::handle_fatal_error(sink_error)?,
                }
            }
        }

        Self::drain_and_shutdown(&consumer, &mut state_machine, &mut upload_pool).await?;

        stats.print_report(file_registry.active_file_count(), upload_pool.len() as u64);

        Ok(())
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

    async fn drain_and_shutdown(
        consumer: &StreamConsumer<CustomContext>,
        state_machine: &mut StateMachine,
        upload_pool: &mut FuturesUnordered<BoxFuture>,
    ) -> Result<()> {
        info!("draining upload pool and shutting down");

        // does not retry failed uploads during shutdown phase - should we?
        while let Some(upload_result) = upload_pool.next().await {
            for response in state_machine.handle(Request::UploadCompletion(upload_result)) {
                match response {
                    Response::Fatal(sink_error) => Self::handle_fatal_error(sink_error)?,
                    _ => (),
                }
            }
        }

        for response in state_machine.handle(Request::FinalCommit(consumer.assignment()?)) {
            match response {
                Response::CommitSync(tpl) => {
                    Self::handle_commit(&tpl, &consumer, CommitMode::Sync)?
                }
                Response::Fatal(sink_error) => Self::handle_fatal_error(sink_error)?,
                _ => (),
            }
        }

        Ok(())
    }

    fn handle_rebalance(
        rx: &mut UnboundedReceiver<Vec<(String, i32)>>,
        state_machine: &mut StateMachine,
    ) -> Result<()> {
        while let Ok(partitions_assigned) = rx.try_recv() {
            let _ = state_machine.handle(Request::PartitionsAssigned(partitions_assigned));
        }
        Ok(())
    }

    fn handle_commit(
        topic_partition_list: &TopicPartitionList,
        consumer: &StreamConsumer<CustomContext>,
        mode: CommitMode,
    ) -> Result<()> {
        if topic_partition_list.count() > 0 {
            consumer.commit(topic_partition_list, mode)?;
        }
        Ok(())
    }

    fn handle_fatal_error(sink_error: SinkError) -> Result<()> {
        error!("fatal error occurred in StateMachine: {sink_error:?}");
        Err(sink_error)
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
