use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;

pub type TopicOffsets = HashMap<(String, i32), BinaryHeap<Reverse<i64>>>;

pub struct SealedOffsets {
    offsets: TopicOffsets, // (topic name, partition) -> offsets
}
impl SealedOffsets {
    pub fn new(registry: OffsetRegistry) -> Self {
        SealedOffsets {
            offsets: registry.offsets(),
        }
    }

    pub fn offsets(self) -> TopicOffsets {
        self.offsets
    }
}

pub struct OffsetRegistry {
    offsets: TopicOffsets, // (topic name, partition) -> offsets
}
impl OffsetRegistry {
    pub fn new() -> Self {
        OffsetRegistry {
            offsets: HashMap::new(),
        }
    }

    pub fn offsets(self) -> TopicOffsets {
        self.offsets
    }

    pub fn add(&mut self, topic_name: &str, partition: i32, offset: i64) {
        let mut heap = self
            .offsets
            .entry((topic_name.into(), partition))
            .insert_entry(BinaryHeap::new());

        heap.get_mut().push(Reverse(offset))
    }

    pub fn combine(&mut self, offsets: TopicOffsets) {
        
    }
}
