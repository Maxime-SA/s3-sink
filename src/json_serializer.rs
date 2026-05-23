use crate::{RecordDecoder, Result, error::SinkError};
use rdkafka::{Message, message::Headers};

/*
Assumptions:
- JsonSchemaDecoder is valid Json Schema.
- JsonStringDecoder is valid Json string with characters escaped.
- Headers that contain problematic characters will be silently dropped.

Thoughts:
- These are not validated, if they are not respected, data will be corrupted.
- Not the most robust SerDe but for our use case, I think the tradeoffs are worth it.
- Buffer does not trim down once capacity is increased.

Todo:
- Review unit tests
*/

pub struct JsonSerializer {
    buf: Vec<u8>,
}
impl JsonSerializer {
    const BUFFER_CAPACITY: usize = 1024 * 512;

    pub fn new() -> Self {
        JsonSerializer {
            buf: Vec::with_capacity(Self::BUFFER_CAPACITY),
        }
    }

    pub fn serialize<M: Message>(
        &mut self,
        record: &M,
        decoder: &RecordDecoder,
    ) -> Result<Option<&[u8]>> {
        let Some(raw_payload) = record.payload() else {
            return Ok(None);
        };

        self.buf.clear();

        let data_payload = decoder.data_payload(raw_payload).ok_or_else(|| {
            SinkError::Serialization(format!(
                "could not decode record '{}', partition '{}', offset '{}' with {}",
                record.topic(),
                record.partition(),
                record.offset(),
                decoder
            ))
        })?;

        self.buf.extend_from_slice(b"{\"data\":");
        self.buf.extend_from_slice(data_payload);

        if let Some(headers) = record.headers() {
            for header in headers.iter() {
                let key_bytes = header.key.as_bytes();

                if let Some(header_value) = header.value
                    && Self::is_valid_json(header_value)
                    && Self::is_valid_json(key_bytes)
                {
                    // delimiter
                    self.buf.push(b',');
                    // write key name
                    self.buf.extend_from_slice(b"\"x-");
                    self.buf.extend_from_slice(key_bytes);
                    self.buf.extend_from_slice(b"\":\"");
                    // write value
                    self.buf.extend_from_slice(header_value);
                    self.buf.push(b'"');
                }
            }
        }

        self.buf.extend_from_slice(b"}\n");

        Ok(Some(&self.buf))
    }

    fn is_valid_json(bytes: &[u8]) -> bool {
        !bytes.iter().any(|&b| b == b'"' || b == b'\\' || b < 0x20)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rdkafka::message::{OwnedHeaders, OwnedMessage};

    fn make_message(payload: Option<Vec<u8>>, headers: Option<OwnedHeaders>) -> OwnedMessage {
        OwnedMessage::new(
            payload,
            None,
            String::from("topic"),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            headers,
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
        let message = make_message(Some(payload.to_vec()), Some(headers));

        let actual_result = String::from_utf8(
            serializer
                .serialize(&message, &RecordDecoder::JsonSchemaDecoder)
                .unwrap()
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
        let message = make_message(Some(payload.to_vec()), Some(headers));

        let actual_result = String::from_utf8(
            serializer
                .serialize(&message, &RecordDecoder::JsonStringDecoder)
                .unwrap()
                .unwrap()
                .to_vec(),
        )
        .unwrap();

        let expected_result = String::from(
            "{\"data\":\"random-string\",\"x-header-A\":\"value-A\",\"x-header-B\":\"value-B\"}\n",
        );

        assert_eq!(expected_result, actual_result);
    }

    #[test]
    fn test_empty_header() {
        let mut serializer = JsonSerializer::new();

        let mut payload = vec![];
        payload.extend_from_slice(b"\"random-string\"");

        let message = make_message(Some(payload.to_vec()), None);

        let actual_result = String::from_utf8(
            serializer
                .serialize(&message, &RecordDecoder::JsonStringDecoder)
                .unwrap()
                .unwrap()
                .to_vec(),
        )
        .unwrap();

        let expected_result = String::from("{\"data\":\"random-string\"}\n");

        assert_eq!(expected_result, actual_result);
    }

    #[test]
    fn test_empty_payload_with_headers() {
        let mut serializer = JsonSerializer::new();

        let headers = make_headers(None);
        let message = make_message(None, Some(headers));

        let actual_result = serializer
            .serialize(&message, &RecordDecoder::JsonStringDecoder)
            .unwrap();

        let expected_result = None;

        assert_eq!(expected_result, actual_result);
    }

    #[test]
    fn test_decoder_error() {
        let mut serializer = JsonSerializer::new();

        // payload without magic bytes
        let mut payload = vec![];
        payload.extend_from_slice(b"{\"data\": 4}");

        let headers = make_headers(None);
        let message = make_message(Some(payload.to_vec()), Some(headers));

        let actual_result = serializer
            .serialize(&message, &RecordDecoder::JsonSchemaDecoder)
            .err()
            .unwrap();

        let expected_result = SinkError::Serialization(format!(
            "could not decode record '{}', partition '{}', offset '{}' with {}",
            message.topic(),
            message.partition(),
            message.offset(),
            RecordDecoder::JsonSchemaDecoder
        ));

        assert_eq!(expected_result, actual_result);
    }
}
