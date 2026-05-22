use std::rc::Rc;

use s3_sink::*;
use tracing::{error, info};

const LOCAL_KAFKA_CONFIG: [(&str, &str); 15] = [
    (
        "bootstrap.servers",
        "b-1.dpkafkadev.ams1av.c6.kafka.eu-west-1.amazonaws.com:9098,b-2.dpkafkadev.ams1av.c6.kafka.eu-west-1.amazonaws.com:9098,b-3.dpkafkadev.ams1av.c6.kafka.eu-west-1.amazonaws.com:9098",
    ),
    ("group.id", "s3-sink-rust"),
    ("client.id", "s3-sink-rust"),
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
    ("security.protocol", "SASL_SSL"),
    ("sasl.mechanism", "OAUTHBEARER"),
];

fn get_config() -> SinkConfig {
    let topic_config = TopicConfig {
        decoder: RecordDecoder::JsonSchemaDecoder,
        router: RouterStrategy::TopicVersion,
    };

    let kafka_config = KafkaConfig {
        input_topics: vec![(topic_config, vec![Rc::from(String::from("topic-1"))])],
        consumer_properties: LOCAL_KAFKA_CONFIG
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        principal_name: "rust-s3-sink".into(),
        region: aws_config::Region::from_static("eu-west-1"),
        token_lifetime_ms: 1000 * 60 * 15,
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
        max_active_file_timeout_ms: 1000 * 60 * 15,
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
    use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();
}

fn main() {
    init_logging();

    let config = get_config();

    let mock_uploader = MockUploader; // just for testing

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("could not build Tokio runtime");

    let sink = Sink::new(&config);

    match runtime.block_on(sink.event_loop(mock_uploader)) {
        Ok(_) => info!("sink event loop exited"),
        Err(error) => error!("sink error: {:?}", error),
    };
}
