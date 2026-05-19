use std::io;
use std::result;

use rdkafka::error::KafkaError;

#[derive(Debug)]
pub enum SinkError {
    KafkaError(String),
    IOError(String),
    S3Error(String),
    ConfigurationError(String),
    FileRegistry(String),
    CatchAll(String),
}

impl From<KafkaError> for SinkError {
    fn from(value: KafkaError) -> Self {
        SinkError::KafkaError(value.to_string())
    }
}

impl From<std::io::Error> for SinkError {
    fn from(value: io::Error) -> Self {
        SinkError::IOError(value.to_string())
    }
}

pub type Result<T> = result::Result<T, SinkError>;
