use super::{entry::TopNEntry, map::TopNMap, KeyBounds};
use crate::aggregator::{Aggregator, PartialAggregateType};
use core::{cmp::Ordering, fmt::Debug, ops::Deref};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "serde")]
use serde_big_array::BigArray;

#[cfg(feature = "serde")]
use zerovec::ule::AsULE;

/// An immutable partial aggregate for the TopNAggregator
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Debug, Clone, Copy)]
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
    /// Returns the identity aggregate of TopNState
    pub const fn identity() -> Self {
        let top_n = [None; N];
        Self { top_n }
    }
    pub(super) fn from(heap: Vec<Option<TopNEntry<Key, A::PartialAggregate>>>) -> Self {
        let top_n: [Option<TopNEntry<Key, A::PartialAggregate>>; N] = heap.try_into().unwrap();
        Self { top_n }
    }
    pub(super) fn merge(&mut self, other: Self, order: Ordering) {
        let mut map = TopNMap::<Key, A>::default();
        for entry in self.top_n.iter().flatten() {
            map.insert(entry.key, entry.data);
        }

        for entry in other.top_n.iter().flatten() {
            map.insert(entry.key, entry.data);
        }
        *self = map.build(order);
    }
}
impl<Key, const N: usize, A> PartialAggregateType for TopNState<Key, N, A>
where
    Key: KeyBounds,
    A: Aggregator + Copy,
    <A as Aggregator>::PartialAggregate: Ord + Copy,
{
}

// #[allow(unsafe_code)]
// unsafe impl<Key, const N: usize, A> ULE for TopNState<Key, N, A>
// where
//     Key: KeyBounds,
//     A: Aggregator + Copy,
//     <A as Aggregator>::PartialAggregate: Ord + Copy,
// {
//     fn validate_byte_slice(bytes: &[u8]) -> Result<(), zerovec::ZeroVecError> {
//         Ok(())
//     }
// }

#[cfg(feature = "serde")]
impl<Key, const N: usize, A> AsULE for TopNState<Key, N, A>
where
    Key: KeyBounds,
    A: Aggregator + Copy,
    <A as Aggregator>::PartialAggregate: Ord + Copy,
{
    // type ULE = [Option<TopNEntry<Key, A::PartialAggregate>>; N];
    type ULE = u8; // FIX
    fn to_unaligned(self) -> Self::ULE {
        unimplemented!();
    }
    fn from_unaligned(_unaligned: Self::ULE) -> Self {
        unimplemented!();
    }
}

impl<Key, const N: usize, A> Deref for TopNState<Key, N, A>
where
    Key: KeyBounds,
    A: Aggregator + Copy,
    <A as Aggregator>::PartialAggregate: Ord + Copy,
{
    type Target = [Option<TopNEntry<Key, A::PartialAggregate>>; N];

    fn deref(&self) -> &[Option<TopNEntry<Key, A::PartialAggregate>>; N] {
        &self.top_n
    }
}
