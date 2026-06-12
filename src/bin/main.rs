use std::rc::Rc;

use s3_sink::*;
use tracing::{error, info};

fn get_env_var_or_panic(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("could not get env var '{name}'"))
}

fn get_env_var_or_default(name: &str, default: impl FnOnce() -> String) -> String {
    std::env::var(name).unwrap_or_else(|_| default())
}

fn get_u64_env_var_or_panic(name: &str) -> u64 {
    get_env_var_or_panic(name)
        .parse::<u64>()
        .unwrap_or_else(|_| panic!("could not get env var '{name}' as u64"))
}

fn get_config() -> SinkConfig {
    // temporary until we set-up a dynamic way to inject it
    let input_topics = {
        let mut schema_topics = vec![];

        let mut dlq_topics = vec![];

        schema_topics.append(&mut dlq_topics);

        schema_topics
            .iter()
            .map(|(config, topics)| {
                (
                    *config,
                    topics
                        .iter()
                        .map(|&topic| TopicName(Rc::from(String::from(topic))))
                        .collect(),
                )
            })
            .collect()
    };

    let consumer_properties: Vec<(String, String)> = {
        let mut static_properties: Vec<(String, String)> = [
            ("group.protocol", "classic"),
            ("auto.offset.reset", "latest"),
            ("enable.auto.offset.store", "false"),
            ("enable.auto.commit", "false"),
            ("socket.keepalive.enable", "true"),
            ("security.protocol", "sasl_ssl"),
            ("sasl.mechanism", "OAUTHBEARER"),
            ("ssl.ca.location", "/etc/ssl/certs/ca-certificates.crt"),
            ("debug", "consumer,broker,security,protocol"),
        ]
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();

        let mut dynamic_consumer_properties: Vec<(String, String)> = vec![
            ("group.id".to_string(), get_env_var_or_panic("GROUP_ID")),
            (
                "bootstrap.servers".to_string(),
                get_env_var_or_panic("BOOTSTRAP_SERVERS"),
            ),
            (
                "fetch.max.bytes".to_string(),
                get_env_var_or_panic("FETCH_MAX_BYTES"),
            ),
            (
                "max.partition.fetch.bytes".to_string(),
                get_env_var_or_panic("MAX_PARTITION_FETCH_BYTES"),
            ),
            (
                "receive.message.max.bytes".to_string(),
                get_env_var_or_panic("RECEIVE_MESSAGE_MAX_BYTES"),
            ),
            (
                "queued.min.messages".to_string(),
                get_env_var_or_panic("QUEUED_MIN_MESSAGES"),
            ),
            (
                "queued.max.messages.kbytes".to_string(),
                get_env_var_or_panic("QUEUED_MAX_MESSAGES_KBYTES"),
            ),
            (
                "statistics.interval.ms".to_string(),
                get_env_var_or_panic("KAFKA_CLIENT_STATISTICS_INTERVAL_MS"),
            ),
            (
                "client.rack".to_string(),
                get_env_var_or_panic("CLIENT_RACK"),
            ),
        ];

        static_properties.append(&mut dynamic_consumer_properties);

        static_properties
    };

    let kafka_config = KafkaConfig {
        input_topics,
        consumer_properties,
        principal_name: get_env_var_or_panic("GROUP_ID"),
        region: aws_config::Region::new(get_env_var_or_default("REGION", || {
            "eu-west-1".to_string()
        })),
    };

    let timers_config = TimersConfig {
        commit_tick_ms: get_u64_env_var_or_panic("COMMIT_TICK_MS"),
    };

    let files_config = FileConfig {
        scratch_directory: get_env_var_or_panic("SCRATCH_DIRECTORY").into(),
        target_file_size_b: get_u64_env_var_or_panic("TARGET_FILE_SIZE_B"),
        compression_level: get_u64_env_var_or_panic("COMPRESSION_LEVEL")
            .try_into()
            .unwrap(),
    };

    let upload_config = UploadConfig {
        bucket: get_env_var_or_panic("BUCKET"),
        max_uploads_retry: get_u64_env_var_or_panic("MAX_UPLOADS_RETRY"),
        max_concurrent_uploads: get_u64_env_var_or_panic("MAX_CONCURRENT_UPLOADS"),
        max_active_file_timeout_ms: get_u64_env_var_or_panic("MAX_ACTIVE_FILE_TIMEOUT_MS"),
    };

    SinkConfig {
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
        .json()
        .init();
}

fn main() {
    init_logging();

    info!("initializing SinkConfig");
    let config = get_config();

    std::fs::create_dir_all(config.files.scratch_directory.as_path())
        .expect("failed to create scratch directory");

    info!("initializing DiskFileRegistry");
    let file_registry = DiskFileRegistry::new(
        &config.files.scratch_directory,
        config.files.compression_level,
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("could not build Tokio runtime");

    runtime.block_on(async {
        info!("initializing S3Upload");
        let uploader = S3Upload::new(
            config.kafka.region.clone(),
            config.uploads.bucket.clone(),
            config.uploads.max_uploads_retry,
            None,
            None,
            None,
            None,
        )
        .await;

        match Sink::start(&config, uploader, file_registry).await {
            Ok(_) => info!("sink event loop exited"),
            Err(error) => error!("sink error: {:?}", error),
        }
    });
}
