use crate::{RecordDecoder, RouterStrategy, cache::TopicName};
use aws_config::Region;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug)]
pub struct TopicConfig {
    pub decoder: RecordDecoder, // decoder knows how to extract Kafka value payload
    pub router: RouterStrategy, // router knows which stream to forward a record to
}

pub struct KafkaConfig {
    pub input_topics: Vec<(TopicConfig, Vec<TopicName>)>, // input topics to consume
    pub consumer_properties: Vec<(String, String)>, // consumer client properties (i.e. (key, value))
    pub region: Region,
    pub token_lifetime_ms: i64,
    pub principal_name: String,
}

pub struct FileConfig {
    pub scratch_directory: PathBuf, // directory for writing files before uploading
    pub target_file_size_b: u64,    // uncompressed target file size in bytes
    pub compression_level: i32,     // compression level used by zstd
}

pub struct TimersConfig {
    pub commit_tick_ms: u64, // frequency at which to commit offsets back to Kafka
    pub fairness_scheduler_tick_ms: u64, // frequency at which to review topic consumption budget
}

pub struct UploadConfig {
    pub max_concurrent_uploads: u64, // maximum number of concurrent uploads at any given point
    pub max_active_file_timeout_ms: u64, // maximum amount of time (milliseconds) that an active file can remain open
}

pub struct SinkConfig {
    pub version: u64,
    pub kafka: KafkaConfig,
    pub timers: TimersConfig,
    pub files: FileConfig,
    pub uploads: UploadConfig,
}
