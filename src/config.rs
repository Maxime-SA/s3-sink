use crate::{RecordDecoder, RouterStrategy, data_model::TopicName};
use aws_config::Region;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TopicConfig {
    pub decoder: RecordDecoder, // decoder knows how to extract Kafka value payload
    pub router: RouterStrategy, // router knows which stream to forward a record to
}

pub struct KafkaConfig {
    pub input_topics: Vec<(TopicConfig, Vec<TopicName>)>, // input topics to consume
    pub consumer_properties: Vec<(String, String)>, // consumer client properties (i.e. (key, value))
    pub region: Region,
    pub principal_name: String,
}

pub struct FileConfig {
    pub scratch_directory: PathBuf, // directory for writing files before uploading
    pub target_file_size_b: u64,    // uncompressed target file size in bytes
    pub compression_level: i32,     // compression level used by zstd
}

pub struct TimersConfig {
    pub commit_tick_ms: u64, // frequency at which to commit offsets back to Kafka
                             // pub fairness_scheduler_tick_ms: u64, // frequency at which to review topic consumption budget
}

pub struct UploadConfig {
    pub bucket: String,                  // top-level S3 bucket where objects should go
    pub max_uploads_retry: u64, // maximum number of times to retry an upload before crashing
    pub max_concurrent_uploads: u64, // maximum number of concurrent uploads at any given point
    pub max_active_file_timeout_ms: u64, // maximum amount of time (milliseconds) that an active file can remain open
}

pub struct SinkConfig {
    pub kafka: KafkaConfig,
    pub timers: TimersConfig,
    pub files: FileConfig,
    pub uploads: UploadConfig,
}
