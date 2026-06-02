use crate::{
    Result, TopicConfig, TopicName,
    cache::Cache,
    data_model::{StreamId, TopicId},
    envelopes::{ToUpload, UploadResult},
    error::SinkError,
    json_serializer::JsonSerializer,
};
use rdkafka::{Message, TopicPartitionList};
use std::{
    borrow::Borrow,
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    rc::Rc,
    time::{Duration, Instant},
};
use tracing::error;

#[derive(PartialEq, Debug, Clone)]
struct StreamState {
    bytes_consumed: u64,
    records_consumed: u64,
    offsets_consumed: HashMap<TopicId, Vec<i64>>,
    created_at: Instant,
}
impl StreamState {
    fn new(now: Instant) -> Self {
        StreamState {
            bytes_consumed: 0,
            records_consumed: 0,
            offsets_consumed: HashMap::new(),
            created_at: now,
        }
    }
}

pub enum Request<'a, M: Message> {
    ProcessRecord {
        record: M,
        serializer: &'a mut JsonSerializer,
    },
    UploadTick,
    CommitTick(TopicPartitionList),
    // FairnessSchedulerTick,
    UploadCompletion(UploadResult),
    FinalCommit(TopicPartitionList),
    PartitionsAssigned(Vec<(String, i32)>),
    ShutdownSignal,
}

#[derive(PartialEq, Debug)]
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
    DrainAndShutdown,
    Fatal(SinkError),
}

pub struct StateMachineConfiguration {
    pub max_active_file_timeout_ms: u64,
    pub max_concurrent_uploads: u64,
    pub max_uploads_retry: u64,
    pub target_file_size_b: u64,
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
    config: StateMachineConfiguration,

    // buf to prevent allocation on every record
    responses: Vec<Response>,
}

impl StateMachine {
    pub fn new(
        input_topics: &Vec<(TopicConfig, Vec<TopicName>)>,
        config: StateMachineConfiguration,
    ) -> Self {
        Self {
            cache: Cache::new(input_topics),
            streams: HashMap::new(),
            offsets_uploaded: HashMap::new(),
            offsets_watermark: HashMap::new(),
            in_flight_uploads: 0,
            config,
            responses: Vec::with_capacity(16),
        }
    }

    pub fn handle<M: Message>(&mut self, request: Request<'_, M>) -> std::vec::Drain<'_, Response> {
        self.handle_inner(request, || Instant::now())
    }

    fn handle_inner<M: Message>(
        &mut self,
        request: Request<'_, M>,
        now: impl Fn() -> Instant,
    ) -> std::vec::Drain<'_, Response> {
        self.responses.clear();

        match request {
            Request::ProcessRecord { record, serializer } => {
                self.handle_process_record(record, serializer, now())
            }
            Request::CommitTick(current_assignment) => self.handle_commit_tick(current_assignment),
            Request::UploadTick => self.handle_upload_tick(now()),
            Request::UploadCompletion(upload_result) => {
                self.handle_upload_completion(upload_result)
            }
            Request::FinalCommit(current_assignment) => {
                self.handle_final_commit(current_assignment)
            }
            Request::PartitionsAssigned(partitions) => self.handle_partitions_assigned(partitions),
            Request::ShutdownSignal => self.handle_shutdown_signal(),
        }

        self.responses.drain(..)
    }

    fn handle_partitions_assigned(&mut self, partitions: Vec<(String, i32)>) {
        for (topic_name, partition) in partitions {
            let topic_id = TopicId(TopicName(Rc::from(topic_name)), partition);
            self.offsets_watermark.remove(&topic_id);
            self.offsets_uploaded.remove(&topic_id);
        }
    }

    fn handle_shutdown_signal(&mut self) {
        for (id, state) in self.streams.drain() {
            self.in_flight_uploads += 1;

            self.responses.push(Response::SealAndUpload {
                id,
                bytes_consumed: state.bytes_consumed,
                records_consumed: state.records_consumed,
                offsets_consumed: state.offsets_consumed,
                created_at: state.created_at,
                retries: self.config.max_uploads_retry,
            })
        }
        self.responses.push(Response::DrainAndShutdown);
    }

    fn handle_process_record<M: Message>(
        &mut self,
        record: M,
        serializer: &mut JsonSerializer,
        now: Instant,
    ) {
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
        let stream_state = self
            .streams
            .entry(metadata.stream_id.clone())
            .or_insert_with(|| StreamState::new(now));

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
                if stream_state.bytes_consumed >= self.config.target_file_size_b
                    && self.in_flight_uploads < self.config.max_concurrent_uploads
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
                        retries: self.config.max_uploads_retry,
                    });
                }
            }
            Ok(None) => (),
            Err(sink_error) => self.responses.push(Response::Fatal(sink_error)),
        }
    }

    fn handle_final_commit(&mut self, current_assignment: TopicPartitionList) {
        match self.make_topic_partition_list(current_assignment) {
            Ok(tpl) => self.responses.push(Response::CommitSync(tpl)),
            Err(sink_error) => self.responses.push(Response::Fatal(sink_error)),
        }
    }

    fn handle_commit_tick(&mut self, current_assignment: TopicPartitionList) {
        match self.make_topic_partition_list(current_assignment) {
            Ok(tpl) => self.responses.push(Response::CommitAsync(tpl)),
            Err(sink_error) => self.responses.push(Response::Fatal(sink_error)),
        }
    }

    fn handle_upload_tick(&mut self, now: Instant) {
        let cut_off = now - Duration::from_millis(self.config.max_active_file_timeout_ms);

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
                retries: self.config.max_uploads_retry,
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

    fn make_topic_partition_list(
        &mut self,
        current_assignment: TopicPartitionList,
    ) -> Result<TopicPartitionList> {
        let mut result = TopicPartitionList::new();

        let mut keys_for_gc = vec![];

        for (TopicId(topic_name, partition), offsets) in &mut self.offsets_uploaded {
            let key = TopicId(topic_name.clone(), *partition);

            /*
            garbage collect any partitions we no longer own
             */
            if current_assignment
                .find_partition(topic_name.borrow(), *partition)
                .is_none()
            {
                keys_for_gc.push(TopicId(topic_name.clone(), *partition));
                continue;
            }

            /*
            guard against a partition was just assigned, a late upload completion inserts stale offsets, but no new record has been consumed yet
             */
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
            }

            // garbage collect any redundant offsets
            let new = offsets.split_off(&offset_to_commit);
            *offsets = new;

            // garbage collect empty topic
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        RecordDecoder, RouterStrategy,
        test_utils::{make_owned_headers, make_owned_message},
    };
    use rdkafka::message::OwnedMessage;

    fn make_state_machine(
        input_topics: Option<Vec<(TopicConfig, Vec<TopicName>)>>,
        max_concurrent_uploads: Option<u64>,
    ) -> StateMachine {
        let config = StateMachineConfiguration {
            max_active_file_timeout_ms: 1000,
            max_concurrent_uploads: max_concurrent_uploads.unwrap_or(3),
            max_uploads_retry: 3,
            target_file_size_b: 128,
        };

        let default_input_topics = vec![(
            TopicConfig {
                decoder: RecordDecoder::JsonSchemaDecoder,
                router: RouterStrategy::TopicVersion,
            },
            vec![
                TopicName(Rc::from("topic-a")),
                TopicName(Rc::from("topic-b")),
                TopicName(Rc::from("topic-c")),
            ],
        )];

        StateMachine::new(&input_topics.unwrap_or(default_input_topics), config)
    }

    fn make_record(
        topic: &str,
        partition: i32,
        offset: i64,
        data_opt: Option<&str>,
    ) -> OwnedMessage {
        let payload = data_opt.map(|data| {
            let mut result = vec![];
            // magic bytes
            result.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]);
            // data bytes
            result.extend_from_slice(data.as_bytes());
            result
        });

        let headers = make_owned_headers(vec![
            ("schema_name".into(), topic.into()),
            ("schema_version".into(), "1.0.0".into()),
        ]);

        make_owned_message(
            Some(topic),
            payload,
            Some(headers),
            Some(partition),
            Some(offset),
        )
    }

    fn make_topic_id(topic_name: &str, partition: i32) -> TopicId {
        TopicId(TopicName(Rc::from(topic_name)), partition)
    }

    mod process_record {
        use super::*;

        #[test]
        fn test_process_records_for_same_stream() {
            // set-up
            let now = Instant::now();

            let serializer = &mut JsonSerializer::new();

            let payload = "{\"id\":15,\"event\":\"test\"}";

            let mut sm = make_state_machine(None, None);

            let mut expected_stream_state = StreamState::new(now);

            let mut expected_watermark = HashMap::new();

            // first record
            let topic_id = make_topic_id("topic-a", 0);

            let record = make_record("topic-a", 0, 0, Some(payload));

            let actual_responses: Vec<Response> = sm
                .handle_inner(
                    Request::ProcessRecord {
                        record: record.clone(),
                        serializer,
                    },
                    || now,
                )
                .collect();

            let metadata = sm.cache.get_or_create_record_metadata(&record).unwrap();

            let actual_stream_state = sm.streams.get(&metadata.stream_id).unwrap().clone();

            let expected_bytes_consumed = serializer
                .serialize(&record, &metadata.config.decoder)
                .unwrap()
                .unwrap()
                .len() as u64;

            expected_stream_state.bytes_consumed += expected_bytes_consumed;

            expected_stream_state.records_consumed += 1;

            expected_stream_state
                .offsets_consumed
                .insert(topic_id.clone(), vec![0]);

            expected_watermark.insert(topic_id.clone(), 0);

            assert_eq!(sm.streams.len(), 1);

            assert_eq!(
                actual_responses,
                vec![Response::WriteFile(metadata.stream_id.clone())]
            );

            assert_eq!(actual_stream_state, expected_stream_state);

            assert_eq!(sm.offsets_watermark, expected_watermark);

            assert_eq!(sm.in_flight_uploads, 0);

            // second record - same partition
            let record = make_record("topic-a", 0, 1, Some(payload));

            let actual_responses: Vec<Response> = sm
                .handle_inner(
                    Request::ProcessRecord {
                        record: record.clone(),
                        serializer,
                    },
                    || now,
                )
                .collect();

            expected_stream_state.bytes_consumed += expected_bytes_consumed;

            expected_stream_state.records_consumed += 1;

            expected_stream_state
                .offsets_consumed
                .entry(topic_id.clone())
                .or_default()
                .push(1);

            assert_eq!(
                actual_responses,
                vec![
                    Response::WriteFile(metadata.stream_id.clone()),
                    Response::SealAndUpload {
                        id: metadata.stream_id.clone(),
                        bytes_consumed: expected_bytes_consumed * 2,
                        records_consumed: 2,
                        offsets_consumed: expected_stream_state.offsets_consumed.clone(),
                        created_at: now,
                        retries: 3
                    }
                ]
            );

            assert_eq!(sm.offsets_watermark, expected_watermark);

            assert_eq!(sm.in_flight_uploads, 1);

            // sealing and uploading removes the stream state
            expected_stream_state.offsets_consumed.remove(&topic_id);
            assert_eq!(sm.streams.len(), 0);

            // third record - different partition
            let record = make_record("topic-a", 1, 0, Some(payload));

            let topic_id = make_topic_id("topic-a", 1);

            let actual_responses: Vec<Response> = sm
                .handle_inner(
                    Request::ProcessRecord {
                        record: record.clone(),
                        serializer,
                    },
                    || now,
                )
                .collect();

            let actual_stream_state = sm.streams.get(&metadata.stream_id).unwrap().clone();

            expected_stream_state.bytes_consumed = expected_bytes_consumed;

            expected_stream_state.records_consumed = 1;

            expected_stream_state
                .offsets_consumed
                .entry(topic_id.clone())
                .or_default()
                .push(0);

            expected_watermark.insert(topic_id.clone(), 0);

            assert_eq!(sm.streams.len(), 1);

            assert_eq!(
                actual_responses,
                vec![Response::WriteFile(metadata.stream_id.clone())]
            );

            assert_eq!(sm.offsets_watermark, expected_watermark);

            assert_eq!(actual_stream_state, expected_stream_state);

            assert_eq!(sm.in_flight_uploads, 1);
        }

        #[test]
        fn test_process_record_with_null_payload() {
            // set-up
            let now = Instant::now();

            let topic_id = make_topic_id("topic-a", 0);

            let mut sm = make_state_machine(None, None);

            let mut expected_stream_state = StreamState::new(now);

            let mut expected_watermark = HashMap::new();

            // process record
            let record = make_record("topic-a", 0, 0, None);

            let actual_responses: Vec<Response> = sm
                .handle_inner(
                    Request::ProcessRecord {
                        record: record.clone(),
                        serializer: &mut JsonSerializer::new(),
                    },
                    || now,
                )
                .collect();

            let metadata = sm.cache.get_or_create_record_metadata(&record).unwrap();

            let actual_stream_state = sm.streams.get(&metadata.stream_id).unwrap().clone();

            expected_stream_state.bytes_consumed = 0;

            expected_stream_state.records_consumed += 1;

            expected_stream_state
                .offsets_consumed
                .insert(topic_id.clone(), vec![0]);

            expected_watermark.insert(topic_id.clone(), 0);

            assert_eq!(sm.streams.len(), 1);

            assert_eq!(actual_responses, vec![]);

            assert_eq!(actual_stream_state, expected_stream_state);

            assert_eq!(sm.offsets_watermark, expected_watermark);

            assert_eq!(sm.in_flight_uploads, 0);
        }

        #[test]
        fn test_process_record_with_missing_topic_config() {
            let mut sm = make_state_machine(None, None);

            let record = make_record("missing-topic", 0, 0, None);

            let actual_responses: Vec<Response> = sm
                .handle_inner(
                    Request::ProcessRecord {
                        record: record.clone(),
                        serializer: &mut JsonSerializer::new(),
                    },
                    || Instant::now(),
                )
                .collect();

            assert_eq!(sm.streams.len(), 0);

            assert_eq!(
                actual_responses,
                vec![Response::Fatal(SinkError::Configuration(
                    "missing topic configuration for 'missing-topic'".into()
                ))]
            );

            assert_eq!(sm.offsets_watermark, HashMap::new());

            assert_eq!(sm.in_flight_uploads, 0);
        }
    }

    mod upload_tick {}

    mod commit_tick {}

    mod upload_completion {}

    mod final_commit {}

    mod partition_assignment {}

    mod shutdown_signal {}
}
