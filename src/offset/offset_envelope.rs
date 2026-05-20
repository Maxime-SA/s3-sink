use std::collections::HashMap;
use std::marker::PhantomData;

pub type OffsetsVec = HashMap<(String, i32), Vec<i64>>;

pub struct ConsumedOffset;
pub struct UploadedOffset;

pub struct OffsetEnvelope<T> {
    offsets: OffsetsVec,
    _state: PhantomData<T>,
}
impl<T> OffsetEnvelope<T> {
    pub fn new(offsets: Option<OffsetsVec>) -> Self {
        OffsetEnvelope {
            offsets: offsets.unwrap_or_default(),
            _state: PhantomData,
        }
    }

    pub fn into_parts(self) -> OffsetsVec {
        self.offsets
    }

    pub fn get_mut_offsets(&mut self) -> &mut OffsetsVec {
        &mut self.offsets
    }
}

impl From<OffsetEnvelope<ConsumedOffset>> for OffsetEnvelope<UploadedOffset> {
    fn from(value: OffsetEnvelope<ConsumedOffset>) -> Self {
        OffsetEnvelope {
            offsets: value.offsets,
            _state: PhantomData,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
