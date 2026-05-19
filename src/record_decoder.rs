pub enum RecordDecoder {
    JsonSchemaDecoder,
    StringDecoder,
}
impl RecordDecoder {
    pub fn data_payload<'a>(&self, payload: &'a [u8]) -> &'a [u8] {
        match self {
            RecordDecoder::JsonSchemaDecoder => &payload[5..],
            RecordDecoder::StringDecoder => payload,
        }
    }
}
