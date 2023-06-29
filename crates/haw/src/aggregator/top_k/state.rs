use super::{entry::TopKEntry, map::TopKMap};
use crate::aggregator::{Aggregator, PartialAggregateType};
use core::fmt::Debug;

#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Copy, Clone)]
pub struct TopKState<const K: usize, const KEY_BYTES: usize, A: Aggregator>
where
    A::PartialAggregate: Ord + Copy,
{
    pub(crate) top_k: [Option<TopKEntry<KEY_BYTES, A::PartialAggregate>>; K],
}
impl<const K: usize, const KEY_BYTES: usize, A: Aggregator> Default for TopKState<K, KEY_BYTES, A>
where
    A::PartialAggregate: Ord + Copy,
{
    fn default() -> Self {
        let top_k = [None; K];
        Self { top_k }
    }
}
impl<const K: usize, const KEY_BYTES: usize, A: Aggregator> TopKState<K, KEY_BYTES, A>
where
    A::PartialAggregate: Ord + Copy,
{
    pub fn from(heap: Vec<Option<TopKEntry<KEY_BYTES, A::PartialAggregate>>>) -> Self {
        let top_k: [Option<TopKEntry<KEY_BYTES, A::PartialAggregate>>; K] =
            heap.try_into().unwrap();
        Self { top_k }
    }
    pub fn iter(&self) -> &[Option<TopKEntry<KEY_BYTES, A::PartialAggregate>>; K] {
        &self.top_k
    }
    pub fn merge(&mut self, other: Self) {
        let mut map = TopKMap::<KEY_BYTES, A>::default();
        for entry in self.top_k.iter().flatten() {
            map.insert(entry.key, entry.data);
        }

        for entry in other.top_k.iter().flatten() {
            map.insert(entry.key, entry.data);
        }
        *self = map.to_state();
    }
}
impl<const K: usize, const KEY_BYTES: usize, A: Aggregator + Copy> PartialAggregateType
    for TopKState<K, KEY_BYTES, A>
where
    <A as Aggregator>::PartialAggregate: Ord + Copy,
{
}
