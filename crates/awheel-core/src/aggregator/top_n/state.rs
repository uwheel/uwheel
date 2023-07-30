use super::{entry::TopNEntry, map::TopNMap, KeyBounds};
use crate::aggregator::{Aggregator, PartialAggregateType};
use core::{cmp::Ordering, fmt::Debug};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "serde")]
use serde_big_array::BigArray;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Debug, Copy, Clone)]
pub struct TopNState<Key, const N: usize, A>
where
    Key: KeyBounds,
    A: Aggregator,
    A::PartialAggregate: Ord + Copy,
{
    #[cfg_attr(feature = "serde", serde(with = "BigArray"))]
    pub(crate) top_n: [Option<TopNEntry<Key, A::PartialAggregate>>; N],
}
impl<Key, const N: usize, A> Default for TopNState<Key, N, A>
where
    Key: KeyBounds,
    A: Aggregator,
    A::PartialAggregate: Ord + Copy,
{
    fn default() -> Self {
        let top_n = [None; N];
        Self { top_n }
    }
}
impl<Key, const N: usize, A> TopNState<Key, N, A>
where
    Key: KeyBounds,
    A: Aggregator,
    A::PartialAggregate: Ord + Copy,
{
    pub fn from(heap: Vec<Option<TopNEntry<Key, A::PartialAggregate>>>) -> Self {
        let top_n: [Option<TopNEntry<Key, A::PartialAggregate>>; N] = heap.try_into().unwrap();
        Self { top_n }
    }
    pub fn iter(&self) -> &[Option<TopNEntry<Key, A::PartialAggregate>>; N] {
        &self.top_n
    }
    pub fn merge(&mut self, other: Self, order: Ordering) {
        let mut map = TopNMap::<Key, A>::default();
        for entry in self.top_n.iter().flatten() {
            map.insert(entry.key, entry.data);
        }

        for entry in other.top_n.iter().flatten() {
            map.insert(entry.key, entry.data);
        }
        *self = map.to_state(order);
    }
}
impl<Key, const N: usize, A> PartialAggregateType for TopNState<Key, N, A>
where
    Key: KeyBounds,
    A: Aggregator + Copy,
    <A as Aggregator>::PartialAggregate: Ord + Copy,
{
}
