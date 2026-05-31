use crate::TopicConfig;
use core::fmt;
use std::{borrow::Borrow, rc::Rc};

/*
A Stream represents a flow of records from Kafka. Each stream can contain records from multiple partitions and each partition can send records to multiple streams (i.e. many-to-many relationship).

A StreamId is derived from a record using a RouterStrategy.

A StreamId is then used to identify the active file writer and track consumed and uploaded offsets.
*/
#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct StreamId(pub Rc<str>);

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Borrow<str> for StreamId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct TopicName(pub Rc<str>);

impl Borrow<str> for TopicName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[derive(Eq, Hash, PartialEq)]
pub struct TopicId(pub TopicName, pub i32); // (topic name, partition)

#[derive(PartialEq, Debug)]
pub struct RecordMetadata {
    pub topic_name: TopicName,
    pub stream_id: StreamId,
    pub config: TopicConfig,
}
