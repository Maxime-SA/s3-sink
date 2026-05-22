use std::rc::Rc;

use aws_config::{Region, meta::region::ProvideRegion};
use s3_sink::*;
use tracing::{error, info};

const BENCH_KAFKA_CONFIG: [(&str, &str); 10] = [
    ("bootstrap.servers", "localhost:9092"),
    ("group.id", "s3-sink-bench"),
    ("client.id", "s3-sink-bench"),
    ("auto.offset.reset", "earliest"),
    ("enable.auto.offset.store", "false"),
    ("enable.auto.commit", "false"),
    ("fetch.max.bytes", "134217728"),           // 128MB
    ("max.partition.fetch.bytes", "36700160"),  // 35MB
    ("receive.message.max.bytes", "157286400"), // 150MB
    ("security.protocol", "PLAINTEXT"),
];

fn get_bench_config() -> SinkConfig {
    let topic_config = TopicConfig {
        decoder: RecordDecoder::JsonSchemaDecoder,
        router: RouterStrategy::TopicVersion,
    };

    let topics: Vec<Rc<str>> = (1..=10)
        .map(|i| Rc::from(format!("topic-{i}").as_str()))
        .collect();

    let kafka_config = KafkaConfig {
        input_topics: vec![(topic_config, topics)],
        consumer_properties: BENCH_KAFKA_CONFIG
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        principal_name: "bench".into(),
        region: aws_config::Region::from_static("us-east-1"),
        token_lifetime_ms: 0, // unused in PLAINTEXT mode
    };

    let timers_config = TimersConfig {
        commit_tick_ms: 30_000,
        upload_tick_ms: 30_000,
        fairness_scheduler_tick_ms: 1_000,
    };

    let files_config = FileConfig {
        scratch_directory: "/tmp/s3-sink-scratch".into(),
        target_file_size_b: 4 * 1024 * 1024, // 4MB
        compression_level: 3,
    };

    let upload_config = UploadConfig {
        max_concurrent_uploads: 50,
        max_active_file_timeout_ms: 1000 * 60 * 5,
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

    std::fs::create_dir_all("/tmp/s3-sink-scratch").expect("failed to create scratch directory");

    let config = get_bench_config();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("could not build Tokio runtime");

    let uploader = runtime.block_on(S3Upload::new(
        Region::from_static("eu-west-1"),
        Some("sink-output"),
        "sink-output".into(),
        None,
        None,
        None,
    ));

    let sink = Sink::new(&config);

    match runtime.block_on(sink.event_loop(uploader)) {
        Ok(_) => info!("sink event loop exited"),
        Err(error) => error!("sink error: {:?}", error),
    };
}
