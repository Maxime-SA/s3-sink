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

impl ToString for RecordDecoder {
    fn to_string(&self) -> String {
        match self {
            RecordDecoder::JsonSchemaDecoder => String::from("JsonSchemaDecoder"),
            RecordDecoder::JsonStringDecoder => String::from("JsonStringDecoder"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
