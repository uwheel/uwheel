use super::{entry::TopKEntry, map::TopKMap};
use crate::{aggregator::AggState, types::PartialAggregateType};
use core::fmt::Debug;

#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Copy, Clone)]
pub struct TopKState<const K: usize, const KEY_BYTES: usize> {
    pub(crate) top_k: [Option<TopKEntry<KEY_BYTES, AggState>>; K],
}
impl<const K: usize, const KEY_BYTES: usize> Default for TopKState<K, KEY_BYTES> {
    fn default() -> Self {
        let top_k = [None; K];
        Self { top_k }
    }
}
impl<const K: usize, const KEY_BYTES: usize> TopKState<K, KEY_BYTES> {
    pub fn from(heap: Vec<Option<TopKEntry<KEY_BYTES, AggState>>>) -> Self {
        let top_k: [Option<TopKEntry<KEY_BYTES, AggState>>; K] = heap.try_into().unwrap();
        Self { top_k }
    }
    pub fn iter(&self) -> &[Option<TopKEntry<KEY_BYTES, AggState>>; K] {
        &self.top_k
    }
    pub fn merge(&mut self, other: Self) {
        let mut map = TopKMap::<KEY_BYTES>::with_capacity(K);
        for entry in self.top_k.iter().flatten() {
            map.insert(entry.key, entry.data);
        }

        for entry in other.top_k.iter().flatten() {
            map.insert(entry.key, entry.data);
        }
        *self = map.to_state();
    }
}
impl<const K: usize, const KEY_BYTES: usize> PartialAggregateType for TopKState<K, KEY_BYTES> {
    type Bytes = [u8; 32];
    fn to_be_bytes(&self) -> Self::Bytes {
        unimplemented!();
    }
    fn to_le_bytes(&self) -> Self::Bytes {
        unimplemented!();
    }
    #[inline]
    fn from_le_bytes(_: Self::Bytes) -> Self {
        unimplemented!();
    }
    fn from_be_bytes(_: Self::Bytes) -> Self {
        unimplemented!();
    }
}
