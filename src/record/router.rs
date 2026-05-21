use std::{borrow::Cow, fmt};
use rdkafka::{Message, message::Headers};

/*
Todo:
- Review unit tests
- Avoid StreamId allocation
     
*/

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct StreamId(pub String);
impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/*
RecordRouter sends a record to its appropriate stream by generating its StreamId.
*/
pub enum RecordRouter {
    TopicVersion,
    TopicVersionStatusCode,
}
impl RecordRouter {
    pub fn id<M: Message>(&self, record: &M) -> StreamId {
        match self {
            Self::TopicVersion => Self::group_by_topic_version(record),
            Self::TopicVersionStatusCode => Self::group_by_topic_version_status_code(record),
        }
    }

    fn get_header<'a, M: Message>(record: &'a M, key: &str) -> Cow<'a, str> {
        record
            .headers()
            .and_then(|headers| {
                headers.iter().find_map(|header| {
                    if header.key == key {
                        header.value.and_then(|val| str::from_utf8(val).ok())
                    } else {
                        None
                    }
                })
            })
            .map(Cow::Borrowed)
            .unwrap_or_else(|| Cow::Owned(format!("unknown_{key}")))
    }

    fn group_by_topic_version<M: Message>(record: &M) -> StreamId {
        let schema_name = Self::get_header(record, "schema_name");
        let schema_version = Self::get_header(record, "schema_version");
        StreamId(format!("{schema_name}.{schema_version}"))
    }

    fn group_by_topic_version_status_code<M: Message>(record: &M) -> StreamId {
        let schema_name = Self::get_header(record, "schema_name");
        let schema_version = Self::get_header(record, "schema_version");
        let status_code = Self::get_header(record, "status_code");
        StreamId(format!("{schema_name}.{schema_version}.{status_code}"))
    }
}
