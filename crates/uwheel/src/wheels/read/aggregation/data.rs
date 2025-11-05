use super::{
    conf::DataLayout,
    deque::{CompressedDeque, MutablePartialDeque, PrefixDeque},
};
use crate::Aggregator;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::RangeBounds;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone, Debug)]
pub enum Data<A: Aggregator> {
    Deque(MutablePartialDeque<A>),
    PrefixDeque(PrefixDeque<A>),
    CompressedDeque(CompressedDeque<A>),
}

impl<A: Aggregator> Data<A> {
    pub(crate) fn layout(&self) -> DataLayout {
        match self {
            Data::Deque(_) => DataLayout::Normal,
            Data::PrefixDeque(_) => DataLayout::Prefix,
            Data::CompressedDeque(c) => DataLayout::Compressed(c.chunk_size),
        }
    }
    pub fn deque_to_prefix(deque: &MutablePartialDeque<A>) -> Self {
        Self::PrefixDeque(PrefixDeque::_from_deque(deque))
    }
    pub fn prefix_to_deque(deque: &PrefixDeque<A>) -> Self {
        Self::Deque(MutablePartialDeque::from_slice(deque.slots_slice()))
    }
    pub fn create_prefix_deque() -> Self {
        Self::PrefixDeque(PrefixDeque::default())
    }
    pub fn create_compressed_deque(chunk_size: usize) -> Self {
        Self::CompressedDeque(CompressedDeque::new(chunk_size))
    }
    pub fn create_deque_with_capacity(capacity: usize) -> Self {
        Self::Deque(MutablePartialDeque::with_capacity(capacity))
    }
    pub fn size_bytes(&self) -> usize {
        match self {
            Data::Deque(arr) => arr.size_bytes(),
            Data::PrefixDeque(arr) => arr.size_bytes(),
            Data::CompressedDeque(arr) => arr.size_bytes(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn len(&self) -> usize {
        match self {
            Data::Deque(arr) => arr.len(),
            Data::PrefixDeque(parr) => parr.len(),
            Data::CompressedDeque(arr) => arr.len(),
        }
    }
    #[inline]
    pub fn push_front(&mut self, agg: A::PartialAggregate) {
        match self {
            Data::Deque(arr) => arr.push_front(agg),
            Data::PrefixDeque(parr) => parr.push_front(agg),
            Data::CompressedDeque(arr) => arr.push_front(agg),
        }
    }

    #[inline]
    pub fn maybe_make_contigious(&mut self) {
        if let Data::Deque(deq) = self {
            deq.make_contiguous();
        }
    }

    pub fn pop_back(&mut self) {
        match self {
            Data::Deque(arr) => arr.pop_back(),
            Data::PrefixDeque(parr) => parr.pop_back(),
            Data::CompressedDeque(arr) => arr.pop_back(),
        }
    }

    pub fn merge(&mut self, other: &Self) {
        match (self, other) {
            (Data::Deque(arr), Data::Deque(arr_other)) => arr.merge(arr_other),
            _ => unimplemented!("Only Deque Merging supported as of now"),
        }
    }

    pub fn get(&self, index: usize) -> Option<A::PartialAggregate> {
        match self {
            Data::Deque(arr) => arr.get(index).cloned(),
            Data::PrefixDeque(parr) => parr.get(index).cloned(),
            Data::CompressedDeque(arr) => arr.get(index),
        }
    }

    pub fn head(&self) -> Option<A::PartialAggregate> {
        match self {
            Data::Deque(arr) => arr.get(0).cloned(),
            Data::PrefixDeque(parr) => parr.get(0).cloned(),
            Data::CompressedDeque(arr) => arr.get(0),
        }
    }

    #[inline]
    pub fn range<R>(&self, range: R) -> Vec<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        match self {
            Data::Deque(arr) => arr.range(range),
            Data::PrefixDeque(parr) => parr.range(range),
            Data::CompressedDeque(carr) => carr.range(range),
        }
    }

    #[inline]
    pub fn combine_range<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        match self {
            Data::Deque(arr) => arr.combine_range(range),
            Data::PrefixDeque(parr) => parr.combine_range(range),
            Data::CompressedDeque(arr) => arr.combine_range(range),
        }
    }
}
