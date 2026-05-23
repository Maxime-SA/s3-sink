use rdkafka::{Message, message::Headers};
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    fmt,
    rc::Rc,
};

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
    id_cache: HashSet<StreamId>,
    router_cache: HashMap<StreamId, RouterStrategy>,
}
impl StreamIdCache {
    pub fn new() -> Self {
        StreamIdCache {
            buf: String::new(),
            id_cache: HashSet::new(),
            router_cache: HashMap::new(),
        }
    }

    pub fn get_id<M: Message>(&mut self, record: &M, strategy: &RouterStrategy) -> StreamId {
        strategy.write_id(record, &mut self.buf);

        if let Some(cached) = self.id_cache.get(self.buf.as_str()) {
            cached.clone()
        } else {
            let id = StreamId(Rc::from(self.buf.as_str()));
            self.router_cache.insert(id.clone(), *strategy);
            self.id_cache.insert(id.clone());
            id
        }
    }

    pub fn get_router(&mut self, id: &StreamId) -> Option<&RouterStrategy> {
        self.router_cache.get(id)
    }
}

/*
RouterStrategy defines a mapping between a record and its StreamId.
*/
#[derive(Clone, Copy, Debug)]
pub enum RouterStrategy {
    TopicVersion,
    Dlq,
}
impl RouterStrategy {
    const UNKNOWN_SCHEMA_NAME: &str = "unknown_schema_name";
    const UNKNOWN_SCHEMA_VERSION: &str = "unknown_schema_version";
    const UNKNOWN_STATUS_CODE: &str = "unknown_status_code";
    const DELIMITER: char = '\x1F';

    pub fn partition_spec(&self, id: &StreamId) -> String {
        let parts: Vec<&str> = id.0.split(Self::DELIMITER).collect();

        let now = chrono::Utc::now();
        let date = now.format("%Y-%m-%d");
        let timestamp = now.format("%Y%m%dT%H%M%SZ");
        let uuid = &uuid::Uuid::new_v4().to_string()[..8];

        match self {
            Self::TopicVersion => {
                format!(
                    "{schema}/{version}/ingest_year_month_day={date}/{timestamp}-{uuid}.zst",
                    schema = parts[0],
                    version = parts[1],
                    date = date,
                    timestamp = timestamp,
                    uuid = uuid
                )
            }
            Self::Dlq => {
                format!(
                    "{dlq_topic}/{schema}/{version}/error={status_code}/ingest_year_month_day={date}/{timestamp}-{uuid}.zst",
                    dlq_topic = parts[0],
                    schema = parts[1],
                    version = parts[2],
                    status_code = parts[3],
                    date = date,
                    timestamp = timestamp,
                    uuid = uuid
                )
            }
        }
    }

    fn write_id<M: Message>(&self, record: &M, buf: &mut String) {
        buf.clear();
        match self {
            Self::TopicVersion => Self::group_by_topic_version(record, buf),
            Self::Dlq => Self::group_by_dlq(record, buf),
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
        buf.push(Self::DELIMITER);
        buf.push_str(schema_version);
    }

    fn group_by_dlq<M: Message>(record: &M, buf: &mut String) {
        let schema_name =
            Self::get_header(record, "schema_name").unwrap_or(Self::UNKNOWN_SCHEMA_NAME);
        let schema_version =
            Self::get_header(record, "schema_version").unwrap_or(Self::UNKNOWN_SCHEMA_VERSION);
        let status_code =
            Self::get_header(record, "status_code").unwrap_or(Self::UNKNOWN_STATUS_CODE);

        buf.push_str(record.topic());
        buf.push(Self::DELIMITER);
        buf.push_str(schema_name);
        buf.push(Self::DELIMITER);
        buf.push_str(schema_version);
        buf.push(Self::DELIMITER);
        buf.push_str(status_code);
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
