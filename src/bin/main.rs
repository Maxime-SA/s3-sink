use s3_sink::*;
use tracing::{error, info};

fn get_env_var_or_panic(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("could not get env var '{name}'"))
}

fn get_env_var_or_default(name: &str, default: impl FnOnce() -> String) -> String {
    std::env::var(name).unwrap_or_else(|_| default())
}

fn get_u64_env_var_or_default(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .map(|value| {
            value
                .parse::<u64>()
                .unwrap_or_else(|_| panic!("could not get env var '{name}' as u64"))
        })
        .unwrap_or(default)
}

fn get_config() -> SinkConfig {
    let input_topics = vec![];

    let consumer_properties: Vec<(String, String)> = {
        let mut static_properties: Vec<(String, String)> = vec![
            ("group.protocol", "consumer"),
            ("auto.offset.reset", "earliest"),
            ("enable.auto.offset.store", "false"),
            ("enable.auto.commit", "false"),
            ("socket.keepalive.enable", "true"),
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", "OAUTHBEARER"),
        ]
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();

        let mut dynamic_consumer_properties: Vec<(String, String)> = vec![
            ("group.id".to_string(), get_env_var_or_panic("SERVICE")),
            (
                "bootstrap.servers".to_string(),
                get_env_var_or_panic("BOOTSTRAP_SERVERS"),
            ),
            (
                "fetch.max.bytes".to_string(),
                get_env_var_or_default("FETCH_MAX_BYTES", || format!("{}", 1024 * 1024 * 128)),
            ),
            (
                "max.partition.fetch.bytes".to_string(),
                get_env_var_or_default("MAX_PARTITION_FETCH_BYTES", || {
                    format!("{}", 1024 * 1024 * 32)
                }),
            ),
            (
                "receive.message.max.bytes".to_string(),
                get_env_var_or_default("RECEIVE_MESSAGE_MAX_BYTES", || {
                    format!("{}", 1024 * 1024 * 150)
                }),
            ),
            (
                "queued.min.messages".to_string(),
                get_env_var_or_default("QUEUED_MIN_MESSAGES", || format!("{}", 100_000)),
            ),
            (
                "queued.max.messages.kbytes".to_string(),
                get_env_var_or_default("QUEUED_MAX_MESSAGES_KBYTES", || format!("{}", 1024 * 64)),
            ),
            (
                "statistics.interval.ms".to_string(),
                get_env_var_or_default("KAFKA_CLIENT_STATISTICS_INTERVAL_MS", || {
                    format!("{}", 60_000)
                }),
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
        principal_name: get_env_var_or_panic("SERVICE"),
        region: aws_config::Region::new(get_env_var_or_default("REGION", || {
            "eu-west-1".to_string()
        })),
    };

    let timers_config = TimersConfig {
        commit_tick_ms: get_u64_env_var_or_default("COMMIT_TICK_MS", 1000 * 60 * 5),
        // fairness_scheduler_tick_ms: get_u64_env_var_or_default("FAIRNESS_SCHEDULER_TICK_MS", 1000 * 5),
    };

    let files_config = FileConfig {
        scratch_directory: get_env_var_or_panic("SCRATCH_DIRECTORY").into(),
        target_file_size_b: get_u64_env_var_or_default("TARGET_FILE_SIZE_B", 1024 * 1024 * 256),
        compression_level: get_u64_env_var_or_default("COMPRESSION_LEVEL", 3)
            .try_into()
            .unwrap(),
    };

    let upload_config = UploadConfig {
        bucket: get_env_var_or_panic("BUCKET"),
        max_uploads_retry: get_u64_env_var_or_default("MAX_UPLOADS_RETRY", 3),
        max_concurrent_uploads: get_u64_env_var_or_default("MAX_CONCURRENT_UPLOADS", 25),
        max_active_file_timeout_ms: get_u64_env_var_or_default(
            "MAX_ACTIVE_FILE_TIMEOUT_MS",
            1000 * 60 * 60,
        ),
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
