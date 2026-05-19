use rdkafka::{Message, message::Headers};

pub enum RecordType {
    JsonSchema,
    String,
}
impl RecordType {
    fn extract_value<'a>(&self, payload: &'a [u8]) -> &'a [u8] {
        match self {
            RecordType::JsonSchema => &payload[5..],
            RecordType::String => payload,
        }
    }
}

pub struct JsonSerializer {
    buf: Vec<u8>,
}
impl JsonSerializer {
    pub fn new() -> Self {
        JsonSerializer {
            buf: Vec::with_capacity(65536),
        }
    }

    pub fn serialize<M: Message>(&mut self, record: &M, record_type: &RecordType) -> Option<&[u8]> {
        self.buf.clear();

        if let Some(raw_payload) = record.payload() {
            let data_payload = RecordType::extract_value(record_type, raw_payload);

            self.buf.extend_from_slice(b"{\"data\":");
            self.buf.extend_from_slice(data_payload);

            if let Some(headers) = record.headers() {
                for header in headers.iter() {
                    // delimiter
                    self.buf.push(b',');
                    // write key name
                    self.buf.extend_from_slice(b"\"x-");
                    self.buf.extend_from_slice(header.key.as_bytes());
                    self.buf.extend_from_slice(b"\":\"");
                    // write value
                    self.buf.extend_from_slice(header.value.unwrap_or(b""));
                    self.buf.push(b'"');
                }
            }

            self.buf.extend_from_slice(b"}\n");
        }

        if self.buf.len() == 0 {
            None
        } else {
            Some(&self.buf)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rdkafka::message::{OwnedHeaders, OwnedMessage};

    fn make_message(payload: &[u8], headers: OwnedHeaders) -> OwnedMessage {
        OwnedMessage::new(
            Some(payload.to_vec()),
            None,
            String::from("topic"),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            Some(headers),
        )
    }

    fn make_headers(headers: Option<Vec<(String, String)>>) -> OwnedHeaders {
        let default_headers = vec![
            ("header-A".into(), "value-A".into()),
            ("header-B".into(), "value-B".into()),
        ];

        let mut result = OwnedHeaders::new();

        for (key, value) in &headers.unwrap_or(default_headers) {
            result = result.insert(rdkafka::message::Header {
                key: key,
                value: Some(value),
            });
        }
        result
    }

    #[test]
    fn test_json_schema_type() {
        let mut serializer = JsonSerializer::new();

        let mut payload = vec![];
        // Add magic bytes
        payload.extend_from_slice(b"00000");
        // Add actual JSON
        payload.extend_from_slice(b"{\"event\":{},\"product\":{\"id\":1}}");

        let headers = make_headers(None);
        let message = make_message(&payload, headers);

        let actual_result = String::from_utf8(
            serializer
                .serialize(&message, &RecordType::JsonSchema)
                .unwrap()
                .to_vec(),
        )
        .unwrap();

        let expected_result = String::from(
            "{\"data\":{\"event\":{},\"product\":{\"id\":1}},\"x-header-A\":\"value-A\",\"x-header-B\":\"value-B\"}\n",
        );

        assert_eq!(expected_result, actual_result);
    }

    #[test]
    fn test_string_type() {
        let mut serializer = JsonSerializer::new();

        let mut payload = vec![];
        payload.extend_from_slice(b"\"random-string\"");

        let headers = make_headers(None);
        let message = make_message(&payload, headers);

        let actual_result = String::from_utf8(
            serializer
                .serialize(&message, &RecordType::String)
                .unwrap()
                .to_vec(),
        )
        .unwrap();

        let expected_result = String::from(
            "{\"data\":\"random-string\",\"x-header-A\":\"value-A\",\"x-header-B\":\"value-B\"}\n",
        );

        assert_eq!(expected_result, actual_result);
    }
}
