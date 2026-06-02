use rdkafka::{Message, message::Headers};

/*
RouterStrategy defines a mapping between a record and its StreamId.
*/
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RouterStrategy {
    TopicVersion,
    Dlq,
}
impl RouterStrategy {
    pub const DELIMITER: char = '\x1F';

    const UNKNOWN_SCHEMA_NAME: &str = "unknown_schema_name";
    const UNKNOWN_SCHEMA_VERSION: &str = "unknown_schema_version";
    const UNKNOWN_STATUS_CODE: &str = "unknown_status_code";

    const SCHEMA_NAME_HEADER: &str = "schema_name";
    const SCHEMA_VERSION_HEADER: &str = "schema_version";
    const STATUS_CODE_HEADER: &str = "status_code";

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
            Self::get_header(record, Self::SCHEMA_NAME_HEADER).unwrap_or(Self::UNKNOWN_SCHEMA_NAME);

        let schema_version = Self::get_header(record, Self::SCHEMA_VERSION_HEADER)
            .unwrap_or(Self::UNKNOWN_SCHEMA_VERSION);

        buf.push_str(schema_name);
        buf.push(Self::DELIMITER);
        buf.push_str(schema_version);
    }

    fn group_by_dlq<M: Message>(record: &M, buf: &mut String) {
        let schema_name =
            Self::get_header(record, Self::SCHEMA_NAME_HEADER).unwrap_or(Self::UNKNOWN_SCHEMA_NAME);

        let schema_version = Self::get_header(record, Self::SCHEMA_VERSION_HEADER)
            .unwrap_or(Self::UNKNOWN_SCHEMA_VERSION);

        let status_code =
            Self::get_header(record, Self::STATUS_CODE_HEADER).unwrap_or(Self::UNKNOWN_STATUS_CODE);

        buf.push_str(record.topic());
        buf.push(Self::DELIMITER);
        buf.push_str(schema_name);
        buf.push(Self::DELIMITER);
        buf.push_str(schema_version);
        buf.push(Self::DELIMITER);
        buf.push_str("status_code=");
        buf.push_str(status_code);
    }
}

#[cfg(test)]
mod test {
    use crate::test_utils::{make_default_owned_headers, make_owned_headers, make_owned_message};

    use super::*;

    #[test]
    fn test_get_header_when_present() {
        let record = make_owned_message(None, None, Some(make_default_owned_headers()), None, None);

        let first_actual_result = RouterStrategy::get_header(&record, "header-A").unwrap();
        let second_actual_result = RouterStrategy::get_header(&record, "header-B").unwrap();

        assert_eq!(first_actual_result, "value-A");
        assert_eq!(second_actual_result, "value-B");
    }

    #[test]
    fn test_get_header_when_absent() {
        let record = make_owned_message(None, None, None, None, None);

        let first_actual_result = RouterStrategy::get_header(&record, "header-B");

        assert_eq!(first_actual_result, None);
    }

    #[test]
    fn test_write_id_topic_version() {
        let mut buf = String::new();

        let headers = vec![
            ("schema_name".into(), "test-schema".into()),
            ("schema_version".into(), "1.0.0".into()),
        ];

        let message = make_owned_message(None, None, Some(make_owned_headers(headers)), None, None);

        RouterStrategy::TopicVersion.write_id(&message, &mut buf);

        assert_eq!(buf, String::from("test-schema\x1F1.0.0"));
    }

    #[test]
    fn test_write_id_dlq() {
        let mut buf = String::new();

        let headers = vec![
            ("schema_name".into(), "test-schema".into()),
            ("schema_version".into(), "1.0.0".into()),
            ("status_code".into(), "400".into()),
        ];

        let message = make_owned_message(
            Some("dlq"),
            None,
            Some(make_owned_headers(headers)),
            None,
            None,
        );

        RouterStrategy::Dlq.write_id(&message, &mut buf);

        assert_eq!(
            buf,
            String::from("dlq\x1Ftest-schema\x1F1.0.0\x1Fstatus_code=400")
        );
    }

    #[test]
    fn test_write_id_topic_version_unknown() {
        let mut buf = String::new();

        let message = make_owned_message(None, None, None, None, None);

        RouterStrategy::TopicVersion.write_id(&message, &mut buf);

        assert_eq!(
            buf,
            String::from("unknown_schema_name\x1Funknown_schema_version")
        );
    }

    #[test]
    fn test_write_id_dlq_unknown() {
        let mut buf = String::new();

        let message = make_owned_message(Some("dlq"), None, None, None, None);

        RouterStrategy::Dlq.write_id(&message, &mut buf);

        assert_eq!(
            buf,
            String::from(
                "dlq\x1Funknown_schema_name\x1Funknown_schema_version\x1Fstatus_code=unknown_status_code"
            )
        );
    }

    #[test]
    fn test_write_id_clears_buffer_between_calls() {
        let mut buf = String::from("stale data");

        let headers = vec![
            ("schema_name".into(), "my-schema".into()),
            ("schema_version".into(), "2.0.0".into()),
        ];

        let message = make_owned_message(None, None, Some(make_owned_headers(headers)), None, None);

        RouterStrategy::TopicVersion.write_id(&message, &mut buf);

        assert_eq!(buf, "my-schema\x1F2.0.0");
    }

    #[test]
    fn test_get_header_non_utf8_value() {
        let mut headers = rdkafka::message::OwnedHeaders::new();

        headers = headers.insert(rdkafka::message::Header {
            key: "schema_name",
            value: Some(&[0xFF, 0xFE]), // invalid UTF-8
        });

        let message = make_owned_message(None, None, Some(headers), None, None);

        assert_eq!(RouterStrategy::get_header(&message, "schema_name"), None);
    }

    #[test]
    fn test_write_id_topic_version_partial_schema_name_only() {
        let mut buf = String::new();

        let headers = vec![("schema_name".into(), "my-schema".into())];

        let message = make_owned_message(None, None, Some(make_owned_headers(headers)), None, None);

        RouterStrategy::TopicVersion.write_id(&message, &mut buf);

        assert_eq!(buf, "my-schema\x1Funknown_schema_version");
    }

    #[test]
    fn test_get_header_duplicate_keys_returns_first() {
        let headers = vec![
            ("schema_name".into(), "first".into()),
            ("schema_name".into(), "second".into()),
        ];

        let message = make_owned_message(None, None, Some(make_owned_headers(headers)), None, None);

        assert_eq!(
            RouterStrategy::get_header(&message, "schema_name"),
            Some("first")
        );
    }
}
