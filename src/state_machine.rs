use crate::{
    Result, TopicConfig, TopicName,
    cache::Cache,
    data_model::{StreamId, TopicId},
    envelopes::{SealedFile, ToUpload, UploadResult},
    error::SinkError,
    files::FileRegistry,
    json_serializer::JsonSerializer,
    key_generator::KeyGenerator,
    stats::Stats,
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

pub enum Request<M: Message> {
    ProcessRecord(M),
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
    RecordConsumed,
    ReadyForUpload(ToUpload),
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

pub struct StateMachine<F: FileRegistry, K: KeyGenerator> {
    // memoization to prevent redundant allocations
    cache: Cache,

    // file registry
    file_registry: F,

    // generate object keys for storage
    key_generator: K,

    // record serializer
    serializer: JsonSerializer,

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

    // metrics
    stats: Stats,
}

impl<F: FileRegistry, K: KeyGenerator> StateMachine<F, K> {
    pub fn new(
        input_topics: &Vec<(TopicConfig, Vec<TopicName>)>,
        file_registry: F,
        key_generator: K,
        config: StateMachineConfiguration,
    ) -> Self {
        Self {
            cache: Cache::new(input_topics),
            file_registry,
            serializer: JsonSerializer::new(),
            streams: HashMap::new(),
            key_generator,
            offsets_uploaded: HashMap::new(),
            offsets_watermark: HashMap::new(),
            in_flight_uploads: 0,
            config,
            responses: Vec::with_capacity(16),
            stats: Stats::new(),
        }
    }

    pub fn handle<M: Message>(&mut self, request: Request<M>) -> std::vec::Drain<'_, Response> {
        self.handle_inner(request, || Instant::now())
    }

    fn handle_inner<M: Message>(
        &mut self,
        request: Request<M>,
        now: impl Fn() -> Instant,
    ) -> std::vec::Drain<'_, Response> {
        self.responses.clear();

        match request {
            Request::ProcessRecord(record) => self.handle_process_record(record, now()),
            Request::CommitTick(current_assignment) => {
                self.handle_commit_tick(current_assignment);

                self.stats.print_report(
                    self.file_registry.active_file_count(),
                    self.in_flight_uploads,
                );
            }
            Request::UploadTick => self.handle_upload_tick(now()),
            Request::UploadCompletion(upload_result) => {
                self.handle_upload_completion(upload_result)
            }
            Request::FinalCommit(current_assignment) => {
                self.handle_final_commit(current_assignment);

                self.stats.print_report(
                    self.file_registry.active_file_count(),
                    self.in_flight_uploads,
                );
            }
            Request::PartitionsAssigned(partitions) => self.handle_partitions_assigned(partitions),
            Request::ShutdownSignal => {
                self.handle_shutdown_signal();

                self.stats.print_report(
                    self.file_registry.active_file_count(),
                    self.in_flight_uploads,
                );
            }
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
        self.streams.drain().for_each(|(stream_id, stream_state)| {
            Self::close_and_upload(
                stream_id,
                stream_state,
                &mut self.file_registry,
                &mut self.responses,
                &mut self.in_flight_uploads,
                &self.key_generator,
                self.config.max_uploads_retry,
            );
        });

        self.responses.push(Response::DrainAndShutdown);
    }

    fn handle_process_record<M: Message>(&mut self, record: M, now: Instant) {
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

        match self.serializer.serialize(&record, &metadata.config.decoder) {
            Ok(Some(payload)) => {
                let payload_size = payload.len() as u64;

                self.stats.inc_bytes_consumed(payload_size);

                // +X bytes consumed
                stream_state.bytes_consumed += payload_size;

                // write to file registry
                if let Err(sink_error) = self
                    .file_registry
                    .write_all(metadata.stream_id.clone(), payload)
                {
                    self.responses.push(Response::Fatal(sink_error));
                    return;
                };

                // record consumed
                self.responses.push(Response::RecordConsumed);

                // should seal and upload?
                if stream_state.bytes_consumed >= self.config.target_file_size_b
                    && self.in_flight_uploads < self.config.max_concurrent_uploads
                {
                    self.stats.inc_files_sealed();

                    // remove stream state
                    let stream_state = self
                        .streams
                        .remove(&metadata.stream_id)
                        .expect("could not find stream state for upload");

                    // close and upload
                    Self::close_and_upload(
                        metadata.stream_id,
                        stream_state,
                        &mut self.file_registry,
                        &mut self.responses,
                        &mut self.in_flight_uploads,
                        &self.key_generator,
                        self.config.max_uploads_retry,
                    );
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

        self.streams
            .extract_if(|_, state| state.created_at <= cut_off)
            .for_each(|(stream_id, stream_state)| {
                self.stats.inc_files_sealed();

                Self::close_and_upload(
                    stream_id,
                    stream_state,
                    &mut self.file_registry,
                    &mut self.responses,
                    &mut self.in_flight_uploads,
                    &self.key_generator,
                    self.config.max_uploads_retry,
                );
            })
    }

    fn handle_upload_completion(&mut self, upload_result: UploadResult) {
        match upload_result {
            // can we add backoff here or a max retry?
            UploadResult::Failure(to_upload, sink_error) => {
                error!("UploadResult::Failure: {:?}", sink_error);

                self.stats.inc_failure_uploads();

                let response = if to_upload.retries() > 0 {
                    Response::ReadyForUpload(to_upload.decrement())
                } else {
                    // -1 in flight uploads
                    self.in_flight_uploads -= 1;

                    Response::Fatal(SinkError::S3Upload(
                        "maximum number of retries reached for S3 upload".into(),
                    ))
                };

                self.responses.push(response);
            }
            UploadResult::Success(file_to_gc, offsets) => {
                self.stats.inc_success_uploads();

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

    fn close_and_upload(
        stream_id: StreamId,
        stream_state: StreamState,
        file_registry: &mut F,
        responses: &mut Vec<Response>,
        in_flight_uploads: &mut u64,
        key_genertor: &K,
        max_uploads_retry: u64,
    ) {
        let (path, compressed_size_b) = match file_registry.close(&stream_id) {
            Ok(result) => result,
            Err(sink_error) => {
                responses.push(Response::Fatal(sink_error));
                return;
            }
        };

        *in_flight_uploads += 1;

        let sealed_file = SealedFile::new(
            path,
            stream_state.bytes_consumed,
            compressed_size_b,
            stream_state.records_consumed,
            stream_state.offsets_consumed,
            stream_state.created_at,
        );

        let object_key = key_genertor.key(&stream_id);

        let to_upload = ToUpload::new(object_key, sealed_file, max_uploads_retry);

        responses.push(Response::ReadyForUpload(to_upload));
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
    use rdkafka::message::{BorrowedMessage, OwnedMessage};

    struct InMemoryFileRegistry {
        files: HashMap<StreamId, Vec<u8>>,
    }
    impl InMemoryFileRegistry {
        fn new() -> Self {
            Self {
                files: HashMap::new(),
            }
        }
    }
    impl FileRegistry for InMemoryFileRegistry {
        fn active_file_count(&self) -> u64 {
            self.files.len() as u64
        }

        fn close(&mut self, id: &StreamId) -> Result<(PathBuf, u64)> {
            let file = self
                .files
                .remove(id)
                .ok_or_else(|| SinkError::FileRegistry(format!("active file '{id}' not found")))?;
            Ok(("in-memory".into(), file.len() as u64))
        }

        fn write_all(&mut self, id: StreamId, bytes: &[u8]) -> Result<()> {
            self.files.entry(id).or_default().extend_from_slice(bytes);
            Ok(())
        }
    }

    struct IdentityKeyGenerator;

    impl KeyGenerator for IdentityKeyGenerator {
        fn key(&self, stream_id: &StreamId) -> String {
            stream_id.to_string()
        }
    }

    fn make_state_machine(
        input_topics: Option<Vec<(TopicConfig, Vec<TopicName>)>>,
        max_concurrent_uploads: Option<u64>,
        target_file_size_b: Option<u64>,
    ) -> StateMachine<InMemoryFileRegistry, IdentityKeyGenerator> {
        let config = StateMachineConfiguration {
            max_active_file_timeout_ms: 1000,
            max_concurrent_uploads: max_concurrent_uploads.unwrap_or(3),
            max_uploads_retry: 3,
            target_file_size_b: target_file_size_b.unwrap_or(128),
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

        StateMachine::new(
            &input_topics.unwrap_or(default_input_topics),
            InMemoryFileRegistry::new(),
            IdentityKeyGenerator,
            config,
        )
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

            let payload = "{\"id\":15,\"event\":\"test\"}";

            let mut sm = make_state_machine(None, None, None);

            let mut expected_stream_state = StreamState::new(now);

            let mut expected_watermark = HashMap::new();

            // first record
            let topic_id = make_topic_id("topic-a", 0);

            let record = make_record("topic-a", 0, 0, Some(payload));

            let actual_responses: Vec<Response> = sm
                .handle_inner(Request::ProcessRecord(record.clone()), || now)
                .collect();

            let metadata = sm.cache.get_or_create_record_metadata(&record).unwrap();

            let actual_stream_state = sm.streams.get(&metadata.stream_id).unwrap().clone();

            let expected_bytes_consumed = JsonSerializer::new()
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

            assert_eq!(actual_responses, vec![Response::RecordConsumed]);

            assert_eq!(actual_stream_state, expected_stream_state);

            assert_eq!(sm.offsets_watermark, expected_watermark);

            assert_eq!(sm.in_flight_uploads, 0);

            // second record - same partition
            let record = make_record("topic-a", 0, 1, Some(payload));

            let actual_responses: Vec<Response> = sm
                .handle_inner(Request::ProcessRecord(record.clone()), || now)
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
                    Response::RecordConsumed,
                    Response::ReadyForUpload(ToUpload::new(
                        IdentityKeyGenerator.key(&metadata.stream_id),
                        SealedFile::new(
                            "in-memory".into(),
                            expected_bytes_consumed * 2,
                            expected_bytes_consumed * 2,
                            2,
                            expected_stream_state.offsets_consumed.clone(),
                            now
                        ),
                        3
                    ))
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
                .handle_inner(Request::ProcessRecord(record.clone()), || now)
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

            assert_eq!(actual_responses, vec![Response::RecordConsumed]);

            assert_eq!(sm.offsets_watermark, expected_watermark);

            assert_eq!(actual_stream_state, expected_stream_state);

            assert_eq!(sm.in_flight_uploads, 1);
        }

        #[test]
        fn test_process_records_for_different_stream() {
            // set-up
            let now = Instant::now();

            let first_payload = "{\"id\":15,\"event\":\"test\"}";

            let second_payload = "{\"id\":15,\"event\":\"test\"}";

            let mut sm = make_state_machine(None, None, None);

            let mut first_expected_stream_state = StreamState::new(now);

            let mut second_expected_stream_state = StreamState::new(now);

            let mut expected_watermark = HashMap::new();

            // first record
            let topic_id = make_topic_id("topic-a", 0);

            let record = make_record("topic-a", 0, 0, Some(first_payload));

            let actual_responses: Vec<Response> = sm
                .handle_inner(Request::ProcessRecord(record.clone()), || now)
                .collect();

            let metadata = sm.cache.get_or_create_record_metadata(&record).unwrap();

            let actual_stream_state = sm.streams.get(&metadata.stream_id).unwrap().clone();

            let first_expected_bytes_consumed = JsonSerializer::new()
                .serialize(&record, &metadata.config.decoder)
                .unwrap()
                .unwrap()
                .len() as u64;

            first_expected_stream_state.bytes_consumed += first_expected_bytes_consumed;

            first_expected_stream_state.records_consumed += 1;

            first_expected_stream_state
                .offsets_consumed
                .insert(topic_id.clone(), vec![0]);

            expected_watermark.insert(topic_id.clone(), 0);

            assert_eq!(sm.streams.len(), 1);

            assert_eq!(actual_responses, vec![Response::RecordConsumed]);

            assert_eq!(actual_stream_state, first_expected_stream_state);

            assert_eq!(sm.offsets_watermark, expected_watermark);

            assert_eq!(sm.in_flight_uploads, 0);

            // second record
            let topic_id = make_topic_id("topic-b", 0);

            let record = make_record("topic-b", 0, 0, Some(second_payload));

            let actual_responses: Vec<Response> = sm
                .handle_inner(Request::ProcessRecord(record.clone()), || now)
                .collect();

            let metadata = sm.cache.get_or_create_record_metadata(&record).unwrap();

            let actual_stream_state = sm.streams.get(&metadata.stream_id).unwrap().clone();

            let second_expected_bytes_consumed = JsonSerializer::new()
                .serialize(&record, &metadata.config.decoder)
                .unwrap()
                .unwrap()
                .len() as u64;

            second_expected_stream_state.bytes_consumed += second_expected_bytes_consumed;

            second_expected_stream_state.records_consumed += 1;

            second_expected_stream_state
                .offsets_consumed
                .insert(topic_id.clone(), vec![0]);

            expected_watermark.insert(topic_id.clone(), 0);

            assert_eq!(sm.streams.len(), 2);

            assert_eq!(actual_responses, vec![Response::RecordConsumed]);

            assert_eq!(actual_stream_state, second_expected_stream_state);

            assert_eq!(sm.offsets_watermark, expected_watermark);

            assert_eq!(sm.in_flight_uploads, 0);
        }

        #[test]
        fn test_process_record_with_null_payload() {
            // set-up
            let now = Instant::now();

            let topic_id = make_topic_id("topic-a", 0);

            let mut sm = make_state_machine(None, None, None);

            let mut expected_stream_state = StreamState::new(now);

            let mut expected_watermark = HashMap::new();

            // process record
            let record = make_record("topic-a", 0, 0, None);

            let actual_responses: Vec<Response> = sm
                .handle_inner(Request::ProcessRecord(record.clone()), || now)
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
            let mut sm = make_state_machine(None, None, None);

            let record = make_record("missing-topic", 0, 0, None);

            let actual_responses: Vec<Response> = sm
                .handle_inner(Request::ProcessRecord(record.clone()), || Instant::now())
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

        #[test]
        fn test_in_flight_backpressure() {
            // set-up
            let now = Instant::now();

            let payload = "{\"id\":15,\"event\":\"test\"}";

            let mut sm = make_state_machine(None, None, Some(1));

            let mut expected_stream_state = StreamState::new(now);

            let mut expected_watermark = HashMap::new();

            // set in_flight_uploads above max
            sm.in_flight_uploads = sm.config.max_concurrent_uploads;

            // first record
            let topic_id = make_topic_id("topic-a", 0);

            let record = make_record("topic-a", 0, 0, Some(payload));

            let actual_responses: Vec<Response> = sm
                .handle_inner(Request::ProcessRecord(record.clone()), || now)
                .collect();

            let metadata = sm.cache.get_or_create_record_metadata(&record).unwrap();

            let actual_stream_state = sm.streams.get(&metadata.stream_id).unwrap().clone();

            let expected_bytes_consumed = JsonSerializer::new()
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

            assert_eq!(actual_responses, vec![Response::RecordConsumed]);

            assert_eq!(actual_stream_state, expected_stream_state);

            assert_eq!(sm.offsets_watermark, expected_watermark);

            assert_eq!(sm.in_flight_uploads, sm.config.max_concurrent_uploads);
        }
    }

    mod upload_tick {
        use super::*;

        fn sort_responses(responses: &mut Vec<Response>) {
            responses.sort_by(|a, b| {
                let key = |r: &Response| match r {
                    Response::ReadyForUpload(to_upload) => to_upload.object_key().to_string(),
                    _ => String::new(),
                };
                key(a).cmp(&key(b))
            });
        }

        #[test]
        fn test_only_include_streams_before_or_on_cutoff() {
            let now = Instant::now();

            let mut sm = make_state_machine(None, None, None);

            let cut_off = now - Duration::from_millis(sm.config.max_active_file_timeout_ms);

            // stream before cutoff
            let stream_before_id = StreamId(Rc::from("before"));

            let mut stream_before = StreamState::new(cut_off - Duration::from_millis(1));

            sm.file_registry
                .write_all(stream_before_id.clone(), &vec![0; 1500])
                .unwrap();

            stream_before.bytes_consumed = 1500;
            stream_before.records_consumed = 6;
            stream_before
                .offsets_consumed
                .insert(make_topic_id("topic-a", 0), vec![0, 1, 2, 3, 4, 5]);

            // stream at cutoff
            let stream_at_id = StreamId(Rc::from("at"));

            let mut stream_at = StreamState::new(cut_off);

            sm.file_registry
                .write_all(stream_at_id.clone(), &vec![0; 10])
                .unwrap();

            stream_at.bytes_consumed = 10;
            stream_at.records_consumed = 1;
            stream_at
                .offsets_consumed
                .insert(make_topic_id("topic-b", 1), vec![50]);

            // stream after cutoff
            let stream_after_id = StreamId(Rc::from("after"));

            let mut stream_after = StreamState::new(now);

            sm.file_registry
                .write_all(stream_after_id.clone(), &vec![0; 200])
                .unwrap();

            stream_after.bytes_consumed = 200;
            stream_after.records_consumed = 3;
            stream_after
                .offsets_consumed
                .insert(make_topic_id("topic-c", 2), vec![100, 101, 102]);

            // build expected responses
            let mut expected_responses = vec![
                Response::ReadyForUpload(ToUpload::new(
                    IdentityKeyGenerator.key(&stream_before_id.clone()),
                    SealedFile::new(
                        "in-memory".into(),
                        1500,
                        1500,
                        6,
                        stream_before.offsets_consumed.clone(),
                        stream_before.created_at,
                    ),
                    3,
                )),
                Response::ReadyForUpload(ToUpload::new(
                    IdentityKeyGenerator.key(&stream_at_id.clone()),
                    SealedFile::new(
                        "in-memory".into(),
                        10,
                        10,
                        1,
                        stream_at.offsets_consumed.clone(),
                        stream_at.created_at,
                    ),
                    3,
                )),
            ];

            // insert test streams in StateMachine
            sm.streams.insert(stream_before_id, stream_before);
            sm.streams.insert(stream_at_id, stream_at);
            sm.streams
                .insert(stream_after_id.clone(), stream_after.clone());

            let mut actual_responses: Vec<Response> = sm
                .handle_inner::<BorrowedMessage>(Request::UploadTick, || now)
                .collect();

            // sort to get deterministic assert
            sort_responses(&mut actual_responses);
            sort_responses(&mut expected_responses);

            // stream_before and stream_at have been closed and uploaded
            assert_eq!(actual_responses, expected_responses);

            assert_eq!(sm.streams.len(), 1);

            assert_eq!(*sm.streams.get(&stream_after_id).unwrap(), stream_after);
        }
    }

    mod upload_completion {
        use super::*;

        #[test]
        fn test_upload_completion_success() {
            let mut sm = make_state_machine(None, None, None);

            let topic_a_0 = make_topic_id("topic-a", 0);
            let topic_a_1 = make_topic_id("topic-a", 1);
            let topic_b_0 = make_topic_id("topic-b", 0);
            let topic_b_1 = make_topic_id("topic-b", 1);

            let mut first_uploaded_offsets = HashMap::new();
            first_uploaded_offsets.insert(topic_a_0.clone(), vec![0, 1, 2, 3, 4, 5]);
            first_uploaded_offsets.insert(topic_a_1.clone(), vec![6, 7, 8]);
            first_uploaded_offsets.insert(topic_b_0.clone(), vec![100, 101, 102]);
            first_uploaded_offsets.insert(topic_b_1.clone(), vec![50, 51, 55]);

            // first upload result
            let expected_responses = vec![Response::DeleteFile("in-memory".into())];

            sm.in_flight_uploads = 1;

            let upload_result =
                UploadResult::Success("in-memory".into(), first_uploaded_offsets.clone());

            let actual_responses: Vec<Response> = sm
                .handle::<BorrowedMessage>(Request::UploadCompletion(upload_result))
                .collect();

            assert_eq!(actual_responses, expected_responses);

            assert_eq!(sm.offsets_uploaded.len(), 4);

            assert_eq!(sm.in_flight_uploads, 0);

            assert_eq!(
                sm.offsets_uploaded.get(&topic_a_0).unwrap().clone(),
                BTreeSet::from_iter(vec![0, 1, 2, 3, 4, 5])
            );

            assert_eq!(
                sm.offsets_uploaded.get(&topic_a_1).unwrap().clone(),
                BTreeSet::from_iter(vec![6, 7, 8])
            );

            assert_eq!(
                sm.offsets_uploaded.get(&topic_b_0).unwrap().clone(),
                BTreeSet::from_iter(vec![100, 101, 102])
            );

            assert_eq!(
                sm.offsets_uploaded.get(&topic_b_1).unwrap().clone(),
                BTreeSet::from_iter(vec![50, 51, 55])
            );

            // second upload result
            sm.in_flight_uploads = 1;

            let mut second_uploaded_offsets = HashMap::new();
            second_uploaded_offsets.insert(topic_b_1.clone(), vec![52, 53, 54]);

            let upload_result =
                UploadResult::Success("in-memory".into(), second_uploaded_offsets.clone());

            let actual_responses: Vec<Response> = sm
                .handle::<BorrowedMessage>(Request::UploadCompletion(upload_result))
                .collect();

            assert_eq!(actual_responses, expected_responses);

            assert_eq!(sm.offsets_uploaded.len(), 4);

            assert_eq!(sm.in_flight_uploads, 0);

            assert_eq!(
                sm.offsets_uploaded.get(&topic_a_0).unwrap().clone(),
                BTreeSet::from_iter(vec![0, 1, 2, 3, 4, 5])
            );

            assert_eq!(
                sm.offsets_uploaded.get(&topic_a_1).unwrap().clone(),
                BTreeSet::from_iter(vec![6, 7, 8])
            );

            assert_eq!(
                sm.offsets_uploaded.get(&topic_b_0).unwrap().clone(),
                BTreeSet::from_iter(vec![100, 101, 102])
            );

            assert_eq!(
                sm.offsets_uploaded.get(&topic_b_1).unwrap().clone(),
                BTreeSet::from_iter(vec![50, 51, 52, 53, 54, 55])
            );
        }

        #[test]
        fn test_upload_completion_failure_with_retry() {
            let mut sm = make_state_machine(None, None, None);

            let now = Instant::now();

            let to_upload = ToUpload::new(
                "key".into(),
                SealedFile::new("in-memory".into(), 100, 50, 5, HashMap::new(), now),
                3,
            );

            sm.in_flight_uploads = 1;

            let actual_response: Vec<Response> = sm
                .handle::<BorrowedMessage>(Request::UploadCompletion(UploadResult::failure(
                    to_upload.clone(),
                    SinkError::S3Upload("failed upload".into()),
                )))
                .collect();

            let expected_response = vec![Response::ReadyForUpload(to_upload.decrement())];

            assert_eq!(actual_response, expected_response);

            assert_eq!(sm.in_flight_uploads, 1);
        }

        #[test]
        fn test_upload_completion_failure_without_retry() {
            let mut sm = make_state_machine(None, None, None);

            let now = Instant::now();

            let to_upload = ToUpload::new(
                "key".into(),
                SealedFile::new("in-memory".into(), 100, 50, 5, HashMap::new(), now),
                0,
            );

            sm.in_flight_uploads = 1;

            let actual_response: Vec<Response> = sm
                .handle::<BorrowedMessage>(Request::UploadCompletion(UploadResult::failure(
                    to_upload.clone(),
                    SinkError::S3Upload("failed upload".into()),
                )))
                .collect();

            let expected_response = vec![Response::Fatal(SinkError::S3Upload(
                "maximum number of retries reached for S3 upload".into(),
            ))];

            assert_eq!(actual_response, expected_response);

            assert_eq!(sm.in_flight_uploads, 0);
        }
    }

    mod partition_assignment {}

    mod shutdown_signal {}

    mod final_commit {}

    mod commit_tick {}
}
