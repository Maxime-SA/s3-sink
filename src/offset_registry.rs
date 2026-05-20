use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;

use rdkafka::TopicPartitionList;

pub type TopicOffsets = HashMap<(String, i32), BinaryHeap<Reverse<i64>>>;

pub struct OffsetRegistry(TopicOffsets);
impl OffsetRegistry {
    pub fn new() -> Self {
        OffsetRegistry(HashMap::new())
    }

    pub fn into_parts(self) -> TopicOffsets {
        self.0
    }

    pub fn add(&mut self, topic_name: &str, partition: i32, offset: i64) {
        let heap = self
            .0
            .entry((topic_name.into(), partition))
            .or_insert(BinaryHeap::new());

        heap.push(Reverse(offset))
    }

    pub fn combine(&mut self, offsets: TopicOffsets) {
        for (key, mut offsets) in offsets {
            let current = self.0.entry(key).or_insert(BinaryHeap::new());

            while let Some(offset) = offsets.pop() {
                current.push(offset);
            }
        }
    }
}

pub struct SealedOffsets(TopicOffsets); // (topic name, partition) -> offsets
impl SealedOffsets {
    pub fn into_parts(self) -> TopicOffsets {
        self.0
    }
}

impl From<OffsetRegistry> for SealedOffsets {
    fn from(value: OffsetRegistry) -> Self {
        SealedOffsets(value.0)
    }
}

impl From<&mut OffsetRegistry> for TopicPartitionList {
    fn from(value: &mut OffsetRegistry) -> Self {
        let mut result = TopicPartitionList::new();

        // need to remove topic and partition key if no longer any item

        for ((topic, partition), offsets) in &mut value.0 {
            result.add_partition(topic, *partition);

            let offset_to_commit = -1;
            while let Some(offset) = offsets.peek()
                && offset.0 == offset_to_commit
            {
                
            }
        }

        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_offset_registry_add() {
        let mut registry = OffsetRegistry::new();

        registry.add("A", 0, 0);
        registry.add("A", 0, 5);
        registry.add("A", 1, 10);

        registry.add("B", 0, 0);
        registry.add("B", 0, 5);
        registry.add("B", 1, 10);

        let actual_result = registry.0[&("A".into(), 0)].clone().into_sorted_vec();
        assert_eq!(actual_result, vec![Reverse(5), Reverse(0)]);

        let actual_result = registry.0[&("A".into(), 1)].clone().into_sorted_vec();
        assert_eq!(actual_result, vec![Reverse(10)]);

        let actual_result = registry.0[&("B".into(), 0)].clone().into_sorted_vec();
        assert_eq!(actual_result, vec![Reverse(5), Reverse(0)]);

        let actual_result = registry.0[&("B".into(), 1)].clone().into_sorted_vec();
        assert_eq!(actual_result, vec![Reverse(10)]);
    }

    #[test]
    fn test_offset_registry_combine() {
        let mut registry = OffsetRegistry::new();
        registry.add("A", 0, 0);
        registry.add("A", 0, 5);
        registry.add("B", 1, 10);

        let mut other = OffsetRegistry::new();
        other.add("A", 0, 3);
        other.add("A", 0, 7);
        other.add("B", 1, 12);
        other.add("C", 0, 100);

        registry.combine(other.into_parts());

        let a0 = registry.0[&("A".into(), 0)].clone().into_sorted_vec();
        assert_eq!(a0, vec![Reverse(7), Reverse(5), Reverse(3), Reverse(0)]);

        let b1 = registry.0[&("B".into(), 1)].clone().into_sorted_vec();
        assert_eq!(b1, vec![Reverse(12), Reverse(10)]);

        let c0 = registry.0[&("C".into(), 0)].clone().into_sorted_vec();
        assert_eq!(c0, vec![Reverse(100)]);
    }
}
