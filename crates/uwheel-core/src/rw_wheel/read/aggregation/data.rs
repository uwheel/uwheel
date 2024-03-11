use super::{
    array::{CompressedArray, MutablePartialArray, PrefixArray},
    conf::DataLayout,
};
use crate::Aggregator;
use core::ops::RangeBounds;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone, Debug)]
pub enum Data<A: Aggregator> {
    Array(MutablePartialArray<A>),
    PrefixArray(PrefixArray<A>),
    CompressedArray(CompressedArray<A>),
}

impl<A: Aggregator> Data<A> {
    pub(crate) fn layout(&self) -> DataLayout {
        match self {
            Data::Array(_) => DataLayout::Normal,
            Data::PrefixArray(_) => DataLayout::Prefix,
            Data::CompressedArray(c) => DataLayout::Compressed(c.chunk_size),
        }
    }
    pub fn array_to_prefix(array: &MutablePartialArray<A>) -> Self {
        Self::PrefixArray(PrefixArray::_from_array(array))
    }
    pub fn prefix_to_array(array: &PrefixArray<A>) -> Self {
        Self::Array(MutablePartialArray::from_slice(array.slots_slice()))
    }
    pub fn create_prefix_array() -> Self {
        Self::PrefixArray(PrefixArray::default())
    }
    pub fn create_compressed_array(chunk_size: usize) -> Self {
        Self::CompressedArray(CompressedArray::new(chunk_size))
    }
    pub fn create_array_with_capacity(capacity: usize) -> Self {
        Self::Array(MutablePartialArray::with_capacity(capacity))
    }
    pub fn size_bytes(&self) -> usize {
        match self {
            Data::Array(arr) => arr.size_bytes(),
            Data::PrefixArray(arr) => arr.size_bytes(),
            Data::CompressedArray(arr) => arr.size_bytes(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn len(&self) -> usize {
        match self {
            Data::Array(arr) => arr.len(),
            Data::PrefixArray(parr) => parr.len(),
            Data::CompressedArray(arr) => arr.len(),
        }
    }
    #[inline]
    pub fn push_front(&mut self, agg: A::PartialAggregate) {
        match self {
            Data::Array(arr) => arr.push_front(agg),
            Data::PrefixArray(parr) => parr.push_front(agg),
            Data::CompressedArray(arr) => arr.push_front(agg),
        }
    }
    pub fn pop_back(&mut self) {
        match self {
            Data::Array(arr) => arr.pop_back(),
            Data::PrefixArray(parr) => parr.pop_back(),
            Data::CompressedArray(arr) => arr.pop_back(),
        }
    }
    pub fn get(&self, index: usize) -> Option<&A::PartialAggregate> {
        match self {
            Data::Array(arr) => arr.get(index),
            Data::PrefixArray(parr) => parr.get(index),
            Data::CompressedArray(arr) => arr.get(index),
        }
    }
    #[inline]
    pub fn aggregate<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        match self {
            Data::Array(arr) => arr.range_query(range),
            Data::PrefixArray(parr) => parr.range_query(range),
            Data::CompressedArray(arr) => arr.range_query(range),
        }
    }
}
