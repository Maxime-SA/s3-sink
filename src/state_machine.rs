use rdkafka::{Message, TopicPartitionList, message::BorrowedMessage};
use std::{
    borrow::Borrow,
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    time::{Duration, Instant},
};
use tracing::error;

use crate::{
    Result, SinkConfig,
    cache::Cache,
    data_model::{StreamId, TopicId},
    envelopes::{ToUpload, UploadResult},
    error::SinkError,
    json_serializer::JsonSerializer,
};

struct StreamState {
    bytes_consumed: u64,
    records_consumed: u64,
    offsets_consumed: HashMap<TopicId, Vec<i64>>,
    created_at: Instant,
}
impl Default for StreamState {
    fn default() -> Self {
        StreamState {
            bytes_consumed: 0,
            records_consumed: 0,
            offsets_consumed: HashMap::new(),
            created_at: Instant::now(),
        }
    }
}

pub enum Request<'a, 'b> {
    NewRecord {
        record: BorrowedMessage<'a>,
        serializer: &'b mut JsonSerializer,
    },
    UploadTick,
    CommitTick,
    // FairnessSchedulerTick,
    UploadCompletion(UploadResult),
    FinalCommit,
    ShutdownSignal,
}

pub enum Response {
    WriteFile(StreamId),
    SealAndUpload {
        id: StreamId,
        bytes_consumed: u64,
        records_consumed: u64,
        offsets_consumed: HashMap<TopicId, Vec<i64>>,
        created_at: Instant,
        retries: u64,
    },
    RetryUpload(ToUpload),
    CommitAsync(TopicPartitionList),
    CommitSync(TopicPartitionList),
    DeleteFile(PathBuf),
    Shutdown,
    Fatal(SinkError),
}

pub struct StateMachine {
    // memoization to prevent redundant allocations
    cache: Cache,

    // stream tracking
    streams: HashMap<StreamId, StreamState>,

    // global offsets tracking
    offsets_uploaded: HashMap<TopicId, BTreeSet<i64>>,
    offsets_watermark: HashMap<TopicId, i64>, // offset at which we can safely say that all lower offsets have been committed

    // backpressure control
    in_flight_uploads: u64,

    // configuration
    max_active_file_timeout_ms: u64,
    max_concurrent_uploads: u64,
    max_uploads_retry: u64,
    target_file_size_b: u64,

    // buf to prevent allocation on every record
    responses: Vec<Response>,
}

impl StateMachine {
    pub fn new(config: &SinkConfig) -> Self {
        Self {
            cache: Cache::new(&config.kafka.input_topics),

            streams: HashMap::new(),

            offsets_uploaded: HashMap::new(),
            offsets_watermark: HashMap::new(),

            in_flight_uploads: 0,

            max_active_file_timeout_ms: config.uploads.max_active_file_timeout_ms,
            max_concurrent_uploads: config.uploads.max_concurrent_uploads,
            max_uploads_retry: config.uploads.max_retry,
            target_file_size_b: config.files.target_file_size_b,

            responses: Vec::with_capacity(16),
        }
    }

    pub fn handle(&mut self, request: Request) -> std::vec::Drain<'_, Response> {
        self.responses.clear();

        match request {
            Request::NewRecord { record, serializer } => self.handle_new_record(record, serializer),
            Request::CommitTick => self.handle_commit_tick(),
            Request::UploadTick => self.handle_upload_tick(),
            Request::UploadCompletion(upload_result) => {
                self.handle_upload_completion(upload_result)
            }
            Request::FinalCommit => self.handle_final_commit(),
            Request::ShutdownSignal => self.responses.push(Response::Shutdown),
        }

        self.responses.drain(..)
    }

    fn handle_new_record<M: Message>(&mut self, record: M, serializer: &mut JsonSerializer) {
        let Some(metadata) = self.cache.get_or_create_record_metadata(&record) else {
            let err = SinkError::Configuration(format!(
                "missing topic configuration for '{}'",
                record.topic()
            ));
            self.responses.push(Response::Fatal(err));
            return;
        };

        let record_partition = record.partition();
        let record_offset = record.offset();

        // get or create stream state
        let stream_state = self.streams.entry(metadata.stream_id.clone()).or_default();

        // add consumed offset
        stream_state
            .offsets_consumed
            .entry(TopicId(metadata.topic_name.clone(), record_partition))
            .or_default()
            .push(record_offset);

        // add watermark offset if not present
        self.offsets_watermark
            .entry(TopicId(metadata.topic_name, record_partition))
            .or_insert(record_offset);

        // +1 records consumed
        stream_state.records_consumed += 1;

        match serializer.serialize(&record, &metadata.config.decoder) {
            Ok(Some(payload)) => {
                // +X bytes consumed
                stream_state.bytes_consumed += payload.len() as u64;

                // write to active file
                self.responses
                    .push(Response::WriteFile(metadata.stream_id.clone()));

                // should seal and upload?
                if stream_state.bytes_consumed >= self.target_file_size_b
                    && self.in_flight_uploads < self.max_concurrent_uploads
                {
                    // inc in_flight uploads
                    self.in_flight_uploads += 1;

                    // remove stream state
                    let stream_state = self.streams.remove(&metadata.stream_id).unwrap();

                    // seal and upload
                    self.responses.push(Response::SealAndUpload {
                        id: metadata.stream_id.clone(),
                        bytes_consumed: stream_state.bytes_consumed,
                        records_consumed: stream_state.records_consumed,
                        offsets_consumed: stream_state.offsets_consumed,
                        created_at: stream_state.created_at,
                        retries: self.max_uploads_retry,
                    });
                }
            }
            Ok(None) => (),
            Err(sink_error) => self.responses.push(Response::Fatal(sink_error)),
        }
    }

    fn handle_final_commit(&mut self) {
        match self.make_topic_partition_list() {
            Ok(tpl) => self.responses.push(Response::CommitSync(tpl)),
            Err(sink_error) => self.responses.push(Response::Fatal(sink_error)),
        }
    }

    fn handle_commit_tick(&mut self) {
        match self.make_topic_partition_list() {
            Ok(tpl) => self.responses.push(Response::CommitAsync(tpl)),
            Err(sink_error) => self.responses.push(Response::Fatal(sink_error)),
        }
    }

    fn handle_upload_tick(&mut self) {
        let cut_off = Instant::now() - Duration::from_millis(self.max_active_file_timeout_ms);

        for (id, state) in self
            .streams
            .extract_if(|_, state| state.created_at <= cut_off)
        {
            self.in_flight_uploads += 1;

            self.responses.push(Response::SealAndUpload {
                id,
                bytes_consumed: state.bytes_consumed,
                records_consumed: state.records_consumed,
                offsets_consumed: state.offsets_consumed,
                created_at: state.created_at,
                retries: self.max_uploads_retry,
            })
        }
    }

    fn handle_upload_completion(&mut self, upload_result: UploadResult) {
        match upload_result {
            // can we add backoff here or a max retry?
            UploadResult::Failure(to_upload, sink_error) => {
                error!("UploadResult::Failure: {:?}", sink_error);

                let cmd = if to_upload.retries() > 0 {
                    Response::RetryUpload(to_upload.decrement())
                } else {
                    Response::Fatal(SinkError::S3Upload(
                        "maximum number of retries reached for S3 upload".into(),
                    ))
                };

                self.responses.push(cmd);
            }
            UploadResult::Success(file_to_gc, offsets) => {
                // add offsets to uploaded tracker
                for (topic_id, offsets) in offsets {
                    self.offsets_uploaded
                        .entry(topic_id)
                        .or_default()
                        .extend(offsets);
                }

                // -1 in flight uploads
                self.in_flight_uploads -= 1;

                self.responses.push(Response::DeleteFile(file_to_gc));
            }
        }
    }

    fn make_topic_partition_list(&mut self) -> Result<TopicPartitionList> {
        let mut result = TopicPartitionList::new();

        let mut keys_for_gc = vec![];

        for (TopicId(topic_name, partition), offsets) in &mut self.offsets_uploaded {
            let key = TopicId(topic_name.clone(), *partition);

            let Some(&watermark) = self.offsets_watermark.get(&key) else {
                continue;
            };

            /*
            Find the first contiguous offset which is not present in the offsets that we have uploaded.
            This is the offset at which a consumer should restart on crash.
             */
            let mut offset_to_commit = watermark;
            for &offset in offsets.range(watermark..) {
                if offset == offset_to_commit {
                    offset_to_commit += 1;
                } else {
                    break;
                }
            }

            if offset_to_commit > watermark {
                result.add_partition_offset(
                    topic_name.borrow(),
                    *partition,
                    rdkafka::Offset::Offset(offset_to_commit),
                )?;

                self.offsets_watermark.insert(key, offset_to_commit);

                // garbage collect any redundant offsets
                let new = offsets.split_off(&offset_to_commit);
                *offsets = new;
            }

            // track redundant topic partition keys
            if offsets.is_empty() {
                keys_for_gc.push(TopicId(topic_name.clone(), *partition));
            }
        }

        // garbage collect any redundant topic partition keys
        for key in keys_for_gc {
            self.offsets_uploaded.remove(&key);
        }

        Ok(result)
    }
}
