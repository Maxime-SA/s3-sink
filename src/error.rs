use std::result;

#[derive(Debug)]
pub enum SinkError {
    KafkaError(String),
    IcebergError(String),
    CatchAll(String),
}

pub type Result<T> = result::Result<T, SinkError>;
