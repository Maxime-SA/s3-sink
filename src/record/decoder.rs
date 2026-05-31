use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecordDecoder {
    JsonSchemaDecoder,
    JsonStringDecoder,
}
impl RecordDecoder {
    pub fn data_payload<'a>(&self, payload: &'a [u8]) -> Option<&'a [u8]> {
        match self {
            RecordDecoder::JsonSchemaDecoder => {
                // magic byte test
                let magic_byte = payload.first();

                /*
                we can check against CSR to make sure this is an actual schema
                we can build this list before starting the sink and keep it in memory
                */
                let _ = payload.get(1..5);

                magic_byte
                    .filter(|&&byte| byte == 0x00)
                    .and_then(|_| payload.get(5..))
            }
            RecordDecoder::JsonStringDecoder => Some(payload),
        }
    }
}

impl fmt::Display for RecordDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            RecordDecoder::JsonSchemaDecoder => "JsonSchemaDecoder",
            RecordDecoder::JsonStringDecoder => "JsonStringDecoder",
        };
        write!(f, "{}", str)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_valid_json_schema_decoder() {
        let mut payload = vec![0x00, 0x00, 0x00, 0x00, 0x00];

        let expected_result = b"{\"id\": 5, \"data\": 10}".to_vec();

        payload.append(&mut expected_result.clone());

        let actual_result = RecordDecoder::JsonSchemaDecoder
            .data_payload(&payload)
            .unwrap();

        assert_eq!(actual_result, expected_result);
    }

    #[test]
    fn test_valid_json_string_decoder() {
        let payload = "\"This is my JSON String\"";

        let actual_result = RecordDecoder::JsonStringDecoder
            .data_payload(payload.as_bytes())
            .unwrap();

        assert_eq!(actual_result, payload.as_bytes());
    }

    #[test]
    fn test_invalid_json_schema_decoder() {
        let payload = b"{\"id\": 5, \"data\": 10}".to_vec();

        assert_eq!(
            RecordDecoder::JsonSchemaDecoder.data_payload(&payload),
            None
        );
    }
}
