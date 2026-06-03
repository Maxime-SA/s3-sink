use crate::error::SinkError;
use crate::files::FileRegistry;
use crate::kafka_consumer::{CustomContext, init_kafka_consumer};
use crate::key_generator::{KeyGenerator, S3Partitioner};
use crate::state_machine::{Request, Response, StateMachine, StateMachineConfiguration};
use crate::timer_interrupts::TimerInterrupts;
use crate::uploader::Uploader;
use crate::{BoxFuture, Result, SinkConfig};
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use std::fs;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tracing::{error, info};

/*
Todo:
- Separate recoverable from unrecoverable errors
- Backpressure for in-flight uploads, local files disk space, offsets in
registry, ...
- Fairness Scheduler
*/

type S3UploadPool = FuturesUnordered<BoxFuture>;

pub struct Sink;
impl Sink {
    pub async fn start<U, F>(config: &SinkConfig, uploader: U, file_registry: F) -> Result<()>
    where
        U: Uploader,
        F: FileRegistry,
    {
        info!("initializing S3UploadPool");
        let mut upload_pool: S3UploadPool = FuturesUnordered::new();

        info!("initializing TimerInterrupts");
        let mut timer_interrupts = TimerInterrupts::new(config);

        info!("initializing ShutdownHandler");
        let mut shutdown = std::pin::pin!(Self::shutdown_signal());

        info!("initializing RebalanceChannel");
        let (rebalance_tx, mut rebalance_rx) = tokio::sync::mpsc::unbounded_channel();

        info!("initializing StreamConsumer");
        let consumer = init_kafka_consumer(&config.kafka, rebalance_tx)?;

        info!("initializing StateMachine");
        let state_machine_config = StateMachineConfiguration {
            max_active_file_timeout_ms: config.uploads.max_active_file_timeout_ms,
            max_concurrent_uploads: config.uploads.max_concurrent_uploads,
            max_uploads_retry: config.uploads.max_uploads_retry,
            target_file_size_b: config.files.target_file_size_b,
        };
        let mut state_machine = StateMachine::new(
            &config.kafka.input_topics,
            file_registry,
            S3Partitioner,
            state_machine_config,
        );

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
                maybe_next_record = consumer.recv() => Request::ProcessRecord(maybe_next_record?),

                // 6. shutdown signal
                _ = &mut shutdown => Request::ShutdownSignal,
            };

            // process any pending rebalances
            while let Ok(partitions_assigned) = rebalance_rx.try_recv() {
                let _ = state_machine
                    .handle::<BorrowedMessage>(Request::PartitionsAssigned(partitions_assigned));
            }

            for response in state_machine.handle(request) {
                match response {
                    Response::RecordConsumed => (),

                    Response::ReadyForUpload(to_upload) => {
                        upload_pool.push(uploader.upload(to_upload));
                    }

                    Response::CommitAsync(tpl) => {
                        Self::handle_commit(&tpl, &consumer, CommitMode::Async)?
                    }

                    Response::CommitSync(tpl) => {
                        Self::handle_commit(&tpl, &consumer, CommitMode::Sync)?
                    }

                    Response::DeleteFile(path_buf) => {
                        let _ = fs::remove_file(path_buf);
                    }

                    Response::DrainAndShutdown => break 'event_loop,

                    Response::Fatal(sink_error) => Self::handle_fatal_error(sink_error)?,
                }
            }
        }

        Self::drain_and_shutdown(&consumer, &mut state_machine, &mut upload_pool).await?;

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

    async fn drain_and_shutdown<F: FileRegistry, K: KeyGenerator>(
        consumer: &StreamConsumer<CustomContext>,
        state_machine: &mut StateMachine<F, K>,
        upload_pool: &mut FuturesUnordered<BoxFuture>,
    ) -> Result<()> {
        info!("draining upload pool and shutting down");

        while let Some(upload_result) = upload_pool.next().await {
            for response in
                state_machine.handle::<BorrowedMessage>(Request::UploadCompletion(upload_result))
            {
                match response {
                    Response::Fatal(sink_error) => Self::handle_fatal_error(sink_error)?,
                    _ => (),
                }
            }
        }

        for response in
            state_machine.handle::<BorrowedMessage>(Request::FinalCommit(consumer.assignment()?))
        {
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
