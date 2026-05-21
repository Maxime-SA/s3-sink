use rdkafka::{Message, message::Headers};
use std::{borrow::Borrow, collections::HashSet, fmt, rc::Rc};

/*
Todo:
- Review unit tests
*/

/*
A Stream represents a flow of records from Kafka.
Each StreamId is derived from the record using a RouterStrategy.
The StreamId is used to route the record to its specific file and track offsets.
*/
#[derive(Eq, Hash, PartialEq, Clone)]
pub struct StreamId(Rc<str>);
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

/*
We cache StreamIds to avoid per-record allocation.
We use a Reference Counted (Rc) smart pointer to track multiple ownership.
*/
pub struct StreamIdCache {
    buf: String,
    cache: HashSet<StreamId>,
}
impl StreamIdCache {
    pub fn new() -> Self {
        StreamIdCache {
            buf: String::new(),
            cache: HashSet::new(),
        }
    }

    pub fn get<M: Message>(&mut self, record: &M, strategy: &RouterStrategy) -> StreamId {
        strategy.write_id(record, &mut self.buf);

        if let Some(cached) = self.cache.get(self.buf.as_str()) {
            cached.clone()
        } else {
            let id = StreamId(Rc::from(self.buf.as_str()));
            self.cache.insert(id.clone());
            id
        }
    }
}

/*
RouterStrategy defines a mapping between a record and its StreamId.
*/
#[derive(Clone, Copy)]
pub enum RouterStrategy {
    TopicVersion,
    TopicVersionStatusCode,
}
impl RouterStrategy {
    const UNKNOWN_SCHEMA_NAME: &str = "unknown_schema_name";
    const UNKNOWN_SCHEMA_VERSION: &str = "unknown_schema_version";
    const UNKNOWN_STATUS_CODE: &str = "unknown_status_code";

    pub fn write_id<M: Message>(&self, record: &M, buf: &mut String) {
        buf.clear();
        match self {
            Self::TopicVersion => Self::group_by_topic_version(record, buf),
            Self::TopicVersionStatusCode => Self::group_by_topic_version_status_code(record, buf),
        }
    }

    fn get_header<'a, M: Message>(record: &'a M, key: &str) -> Option<&'a str> {
        record.headers().and_then(|headers| {
            headers.iter().find_map(|header| {
                if header.key == key {
                    header.value.and_then(|val| str::from_utf8(val).ok())
                } else {
                    None
                }
            })
        })
    }

    fn group_by_topic_version<M: Message>(record: &M, buf: &mut String) {
        let schema_name =
            Self::get_header(record, "schema_name").unwrap_or(Self::UNKNOWN_SCHEMA_NAME);
        let schema_version =
            Self::get_header(record, "schema_version").unwrap_or(Self::UNKNOWN_SCHEMA_VERSION);

        buf.push_str(schema_name);
        buf.push('.');
        buf.push_str(schema_version);
    }

    fn group_by_topic_version_status_code<M: Message>(record: &M, buf: &mut String) {
        let schema_name =
            Self::get_header(record, "schema_name").unwrap_or(Self::UNKNOWN_SCHEMA_NAME);
        let schema_version =
            Self::get_header(record, "schema_version").unwrap_or(Self::UNKNOWN_SCHEMA_VERSION);
        let status_code =
            Self::get_header(record, "status_code").unwrap_or(Self::UNKNOWN_STATUS_CODE);

        buf.push_str(schema_name);
        buf.push('.');
        buf.push_str(schema_version);
        buf.push('.');
        buf.push_str(status_code);
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
