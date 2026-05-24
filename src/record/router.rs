use rdkafka::{Message, message::Headers};

use crate::cache::StreamId;

/*
Todo:
- Review unit tests
*/

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
        let timestamp = now.format("%Y-%m-%dT%H:%M:%SZ");
        let uuid = &uuid::Uuid::new_v4().to_string()[..8];

        match self {
            Self::TopicVersion => {
                format!(
                    "{schema}/{version}/ingest_year_month_day={date}/{timestamp}-{uuid}.json.zst",
                    schema = parts[0],
                    version = parts[1],
                    date = date,
                    timestamp = timestamp,
                    uuid = uuid
                )
            }
            Self::Dlq => {
                format!(
                    "{dlq_topic}/{schema}/{version}/error={status_code}/ingest_year_month_day={date}/{timestamp}-{uuid}.json.zst",
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

    pub fn write_id<M: Message>(&self, record: &M, buf: &mut String) {
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
