use rdkafka::error::KafkaError;
use std::io;
use std::result;

#[derive(Debug, PartialEq)]
pub enum SinkError {
    Kafka(String),
    IO(String),
    Configuration(String),
    FileRegistry(String),
    OffsetRegistry(String),
    Serialization(String),
}

impl From<KafkaError> for SinkError {
    fn from(value: KafkaError) -> Self {
        SinkError::Kafka(value.to_string())
    }
}

impl From<std::io::Error> for SinkError {
    fn from(value: io::Error) -> Self {
        SinkError::IO(value.to_string())
    }
}

pub type Result<T> = result::Result<T, SinkError>;
