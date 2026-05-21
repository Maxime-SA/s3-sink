use std::fmt::{self, write};

pub enum RecordDecoder {
    JsonSchemaDecoder,
    JsonStringDecoder,
}
impl RecordDecoder {
    pub fn data_payload<'a>(&self, payload: &'a [u8]) -> Option<&'a [u8]> {
        match self {
            RecordDecoder::JsonSchemaDecoder => {
                // Magic Byte Test
                if !payload.first()?.is_ascii_digit() {
                    return None;
                }

                payload.get(5..)
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
}
