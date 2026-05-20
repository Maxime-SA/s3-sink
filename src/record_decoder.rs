pub enum RecordDecoder {
    JsonSchemaDecoder,
    StringDecoder,
}
impl RecordDecoder {
    pub fn data_payload<'a>(&self, payload: &'a [u8]) -> Option<&'a [u8]> {
        match self {
            RecordDecoder::JsonSchemaDecoder => payload.get(5..),
            RecordDecoder::StringDecoder => Some(payload),
        }
    }
}
