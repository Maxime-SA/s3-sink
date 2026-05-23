use crate::Result;
use crate::cache::StreamId;
use crate::cache::TopicName;
use crate::envelopes::SealedOffsets;
use crate::error::SinkError;
use rdkafka::TopicPartitionList;
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

pub type OffsetsVec = HashMap<(TopicName, i32), Vec<i64>>;
pub type OffsetsTree = HashMap<(TopicName, i32), BTreeSet<i64>>;

/*
Todo:
- Review unit tests
- Backpressure:
    - Offsets awaiting commit
*/

pub struct OffsetRegistry {
    consumed: HashMap<StreamId, OffsetsVec>,
    uploaded: OffsetsTree,
}

impl OffsetRegistry {
    pub fn new() -> Self {
        OffsetRegistry {
            consumed: HashMap::new(),
            uploaded: OffsetsTree::new(),
        }
    }

    pub fn add_consumed(
        &mut self,
        id: &StreamId,
        topic_name: TopicName,
        partition: i32,
        offset: i64,
    ) {
        let consumed_offsets = self.get_mut_stream_offsets_or_create(id);
        consumed_offsets
            .entry((topic_name, partition))
            .or_default()
            .push(offset);
    }

    pub fn add_uploaded(&mut self, uploaded: OffsetsVec) {
        for ((topic_name, partition), offsets) in uploaded {
            self.uploaded
                .entry((topic_name, partition))
                .or_default()
                .extend(offsets);
        }
    }

    fn get_mut_stream_offsets_or_create(&mut self, id: &StreamId) -> &mut OffsetsVec {
        match self.consumed.entry(id.clone()) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => vacant.insert(OffsetsVec::new()),
        }
    }

    pub fn seal(&mut self, id: &StreamId) -> Result<SealedOffsets> {
        self.consumed
            .remove(id)
            .map(SealedOffsets::new)
            .ok_or_else(|| {
                SinkError::OffsetRegistry(format!(
                    "could not seal consumed offsets for '{id}', stream not found"
                ))
            })
    }

    pub fn committable_offsets(&mut self) -> Result<TopicPartitionList> {
        let mut result = TopicPartitionList::new();
        let mut keys_for_gc = vec![];

        for ((topic, partition), offsets) in &mut self.uploaded {
            let Some(&first) = offsets.iter().next() else {
                keys_for_gc.push((topic.clone(), *partition));
                continue;
            };

            /*
            Find the first contiguous offset which is not present in the offsets that we have uploaded.
            This is the offset at which a consumer should restart on crash.
             */
            let mut offset_to_commit = first;
            for &offset in offsets.iter() {
                if offset == offset_to_commit {
                    offset_to_commit += 1;
                } else {
                    break;
                }
            }

            result.add_partition_offset(
                topic.borrow(),
                *partition,
                rdkafka::Offset::Offset(offset_to_commit),
            )?;

            // garbage collect any redundant offsets
            let new = offsets.split_off(&offset_to_commit);
            *offsets = new;

            // track redundant topic partition keys
            if offsets.is_empty() {
                keys_for_gc.push((topic.clone(), *partition));
            }
        }

        // garbage collect any redundant topic partition keys
        for key in keys_for_gc {
            self.uploaded.remove(&key);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
