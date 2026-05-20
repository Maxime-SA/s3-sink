use s3_sink::*;
use tracing::{error, info};

const LOCAL_KAFKA_CONFIG: [(&str, &str); 14] = [
    ("bootstrap.servers", ""),
    ("group.id", "kafka-s3-sink-rust"),
    ("client.id", "kafka-s3-sink-rust"),
    ("auto.offset.reset", "earliest"),
    ("enable.auto.offset.store", "false"),
    ("enable.auto.commit", "false"),
    ("fetch.max.bytes", "134217728"),           // 128MB
    ("max.partition.fetch.bytes", "36700160"),  // 35MB
    ("receive.message.max.bytes", "157286400"), // 150MB
    ("group.protocol", "classic"),
    ("partition.assignment.strategy", "cooperative-sticky"),
    ("statistics.interval.ms", "30000"), // need to register a callback on rd_kafka_conf_set_stats_cb(),
    ("socket.keepalive.enable", "true"),
    ("security.protocol", "plaintext"), // will need to make this work with MSK
];

fn get_config() -> SinkConfig {
    let topic_config = TopicConfig {
        decoder: RecordDecoder::JsonSchemaDecoder,
        router: RecordRouter::TopicVersion,
    };

    let kafka_config = KafkaConfig {
        input_topics: vec![(topic_config, vec![String::from("topic-1")])],
        consumer_properties: LOCAL_KAFKA_CONFIG
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };

    let timers_config = TimersConfig {
        commit_tick_ms: 30000,
        upload_tick_ms: 30000,
        fairness_scheduler_tick_ms: 1000,
    };

    let files_config = FileConfig {
        scratch_directory: "./tmp".into(),
        target_file_size_b: 4096,
        compression_level: 3,
    };

    let upload_config = UploadConfig {
        max_concurrent_uploads: 50,
    };

    SinkConfig {
        version: 1,
        kafka: kafka_config,
        files: files_config,
        timers: timers_config,
        uploads: upload_config,
    }
}

fn init_logging() {
    tracing_subscriber::fmt::init();
}

fn main() {
    init_logging();

    let mock_uploader = MockUploader; // just for testing

    let config = get_config();

    let sink = Sink::new(&config, mock_uploader);

    match sink.run() {
        Ok(_) => info!("Tokio runtime exited"),
        Err(error) => error!("unexpected sink error: {:?}", error),
    };
}
