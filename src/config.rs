use crate::{RecordDecoder, RecordRouter};
use std::path::PathBuf;

pub struct TopicConfig {
    pub decoder: RecordDecoder,
    pub router: RecordRouter,
}

pub struct KafkaConfig {
    pub input_topics: Vec<(TopicConfig, Vec<String>)>, // input topics to consume
    pub consumer_properties: Vec<(String, String)>, // consumer client properties (i.e. (key, value))
}

pub struct FileConfig {
    pub scratch_directory: PathBuf, // directory for writing files before uploading
    pub target_file_size_b: usize,  // uncompressed target file size in bytes
    pub compression_level: i32,     // compression level used by zstd
}

pub struct TimersConfig {
    pub upload_tick_ms: u64, // frequency at which to check for dormant files
    pub commit_tick_ms: u64, // frequency at which to commit offsets back to Kafka
    pub fairness_scheduler_tick_ms: u64, // frequency at which to review topic consumption budget
}

pub struct UploadConfig {
    pub max_concurrent_uploads: usize, // maximum number of concurrent uploads at any given point
}

pub struct SinkConfig {
    pub version: u64,
    pub kafka: KafkaConfig,
    pub timers: TimersConfig,
    pub files: FileConfig,
    pub uploads: UploadConfig,
}
