use crate::Result;
use crate::offset::offset_envelope::{OffsetEnvelope, UploadedOffset};
use rdkafka::TopicPartitionList;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;

pub type OffsetsHeap = HashMap<(String, i32), BinaryHeap<Reverse<i64>>>;

/*
- Backpressure:
    - Offsets awaiting commit
-
*/
pub struct OffsetRegistry(OffsetsHeap);
impl OffsetRegistry {
    pub fn new() -> Self {
        OffsetRegistry(HashMap::new())
    }

    pub fn combine(&mut self, offsets_envelope: OffsetEnvelope<UploadedOffset>) {
        for (key, mut offsets) in offsets_envelope.into_parts() {
            let current = self.0.entry(key).or_insert(BinaryHeap::new());

            while let Some(offset) = offsets.pop() {
                current.push(Reverse(offset));
            }
        }
    }

    pub fn topic_partition_list(&mut self) -> Result<TopicPartitionList> {
        let mut result = TopicPartitionList::new();

        let mut keys_to_remove = vec![];

        for ((topic, partition), offsets) in &mut self.0 {
            // continue if offsets is empty
            let Some(Reverse(first)) = offsets.peek().copied() else {
                keys_to_remove.push((topic.clone(), *partition));
                continue;
            };

            /*
            Find the first contiguous offset which is not present in the offsets that we have uploaded.
            This is the offset at which a consumer should restart on crash.
             */
            let mut next_expected = first;
            while let Some(Reverse(offset)) = offsets.peek()
                && *offset == next_expected
            {
                offsets.pop();
                next_expected += 1;
            }

            result.add_partition_offset(
                topic,
                *partition,
                rdkafka::Offset::Offset(next_expected),
            )?;

            if offsets.is_empty() {
                keys_to_remove.push((topic.clone(), *partition));
            }
        }

        for key in keys_to_remove {
            self.0.remove(&key);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use crate::offset::offset_envelope::OffsetsVec;

    use super::*;

    #[test]
    fn test_offset_registry_combine() {
        let mut registry = OffsetRegistry::new();

        let mut first = OffsetsVec::new();
        first.insert(("A".into(), 0), vec![0, 5]);
        first.insert(("B".into(), 1), vec![10]);

        let mut second = OffsetsVec::new();
        second.insert(("A".into(), 0), vec![3, 7]);
        second.insert(("B".into(), 1), vec![12]);
        second.insert(("C".into(), 0), vec![100]);

        registry.combine(OffsetEnvelope::new(Some(first)));
        registry.combine(OffsetEnvelope::new(Some(second)));

        let a0 = registry.0[&("A".into(), 0)].clone().into_sorted_vec();
        assert_eq!(a0, vec![Reverse(7), Reverse(5), Reverse(3), Reverse(0)]);

        let b1 = registry.0[&("B".into(), 1)].clone().into_sorted_vec();
        assert_eq!(b1, vec![Reverse(12), Reverse(10)]);

        let c0 = registry.0[&("C".into(), 0)].clone().into_sorted_vec();
        assert_eq!(c0, vec![Reverse(100)]);
    }
}
