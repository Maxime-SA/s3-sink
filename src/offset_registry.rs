use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;

pub type TopicOffsets = HashMap<(String, i32), BinaryHeap<Reverse<i64>>>;

pub struct OffsetRegistry {
    offsets: TopicOffsets, // (topic name, partition) -> offsets
}
impl OffsetRegistry {
    pub fn new() -> Self {
        OffsetRegistry {
            offsets: HashMap::new(),
        }
    }

    pub fn into_parts(self) -> TopicOffsets {
        self.offsets
    }

    pub fn offsets(&mut self) -> &mut TopicOffsets {
        &mut self.offsets
    }

    pub fn add(&mut self, topic_name: &str, partition: i32, offset: i64) {
        let heap = self
            .offsets
            .entry((topic_name.into(), partition))
            .or_insert(BinaryHeap::new());

        heap.push(Reverse(offset))
    }

    pub fn combine(&mut self, offsets: TopicOffsets) {
        for (key, mut offsets) in offsets {
            let current = self.offsets.entry(key).or_insert(BinaryHeap::new());

            while let Some(offset) = offsets.pop() {
                current.push(offset);
            }
        }
    }
}

pub struct SealedOffsets {
    offsets: TopicOffsets, // (topic name, partition) -> offsets
}
impl SealedOffsets {
    pub fn new(registry: OffsetRegistry) -> Self {
        SealedOffsets {
            offsets: registry.into_parts(),
        }
    }

    pub fn offsets(self) -> TopicOffsets {
        self.offsets
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

        let actual_result = registry.offsets[&("A".into(), 0)].clone().into_sorted_vec();
        assert_eq!(actual_result, vec![Reverse(5), Reverse(0)]);

        let actual_result = registry.offsets[&("A".into(), 1)].clone().into_sorted_vec();
        assert_eq!(actual_result, vec![Reverse(10)]);

        let actual_result = registry.offsets[&("B".into(), 0)].clone().into_sorted_vec();
        assert_eq!(actual_result, vec![Reverse(5), Reverse(0)]);

        let actual_result = registry.offsets[&("B".into(), 1)].clone().into_sorted_vec();
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

        let a0 = registry.offsets[&("A".into(), 0)].clone().into_sorted_vec();
        assert_eq!(a0, vec![Reverse(7), Reverse(5), Reverse(3), Reverse(0)]);

        let b1 = registry.offsets[&("B".into(), 1)].clone().into_sorted_vec();
        assert_eq!(b1, vec![Reverse(12), Reverse(10)]);

        let c0 = registry.offsets[&("C".into(), 0)].clone().into_sorted_vec();
        assert_eq!(c0, vec![Reverse(100)]);
    }
}
