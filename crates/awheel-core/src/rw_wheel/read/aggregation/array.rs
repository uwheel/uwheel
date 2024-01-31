use super::{combine_or_insert, conf::CompressionPolicy, into_range};
use crate::Aggregator;
use core::ops::{Bound, Range, RangeBounds};
use zerocopy::{AsBytes, Ref};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// An array of partial aggregates referenced from an underlying byte slice
pub struct PartialArray<'a, A: Aggregator> {
    arr: &'a [A::PartialAggregate],
}

impl<'a, A: Aggregator> AsRef<[A::PartialAggregate]> for PartialArray<'a, A> {
    fn as_ref(&self) -> &[A::PartialAggregate] {
        self.arr
    }
}

impl<'a, A: Aggregator> PartialArray<'a, A> {
    /// Creates a PartialArray from a byte-slice
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        Self {
            arr: Ref::<_, [A::PartialAggregate]>::new_slice(bytes)
                .unwrap()
                .into_slice(),
        }
    }
    /// Returns the true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns the length of the array
    pub fn len(&self) -> usize {
        self.arr.len()
    }
    /// Combines partial aggregates within the given range into a new partial aggregate
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of array
    #[inline]
    pub fn combine_range<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        A::combine_slice(&self.arr[into_range(&range, self.arr.len())])
    }

    /// Combines partial aggregates within the given range into and lowers it to a final aggregate
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the array
    #[inline]
    pub fn combine_range_and_lower<R>(&self, range: R) -> Option<A::Aggregate>
    where
        R: RangeBounds<usize>,
    {
        self.combine_range(range).map(A::lower)
    }

    /// Returns the combined partial aggregate within the given range that match the filter predicate
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the array
    #[inline]
    pub fn combine_range_with_filter<R>(
        &self,
        range: R,
        filter: impl Fn(&A::PartialAggregate) -> bool,
    ) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        let Range { start, end } = into_range(&range, self.arr.len());
        let slots = end - start;

        // Locate which slots we are to combine together
        let relevant_range = self.arr.iter().skip(start).take(slots);

        let mut accumulator: Option<A::PartialAggregate> = None;

        for partial in relevant_range {
            if filter(partial) {
                combine_or_insert::<A>(&mut accumulator, *partial);
            }
        }

        accumulator
    }
}

/// An event-time indexed array containing partial aggregates
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Default, Clone, Debug)]
pub struct MutablePartialArray<A: Aggregator> {
    inner: Vec<A::PartialAggregate>,
}

impl<A: Aggregator> MutablePartialArray<A> {
    /// Attempts to compress the array into bytes
    pub fn try_compress(&self) -> (Vec<u8>, bool) {
        match A::compress(&self.inner) {
            Some(compressed) => (compressed, true),
            None => (self.as_bytes().to_vec(), false),
        }
    }
    /// Attemps to docompress the given bytes into a MutablePartialArray
    pub fn try_decompress(bytes: &[u8]) -> Self {
        let partials = A::decompress(bytes).unwrap();
        Self { inner: partials }
    }

    /// Serializes the array with the given policy and returns whether it was actually compressed
    pub fn serialize_with_policy(&self, policy: CompressionPolicy) -> (Vec<u8>, bool) {
        match policy {
            CompressionPolicy::Always => self.try_compress(),
            CompressionPolicy::After(limit) if self.len() >= limit => self.try_compress(),
            _ => (self.as_bytes().to_vec(), false),
        }
    }

    /// Encodes the array to bytes
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Decode wheel from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let inner = Ref::<_, [A::PartialAggregate]>::new_slice(bytes)
            .unwrap()
            .into_slice()
            .to_vec();

        Self::from_vec(inner)
    }

    /// Creates an array with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
        }
    }
    /// Creates an array from a vector of partial aggregates
    pub fn from_vec(partials: Vec<A::PartialAggregate>) -> Self {
        Self { inner: partials }
    }
    /// Creates an array from a slice of partial aggregates
    pub fn from_slice<I: AsRef<[A::PartialAggregate]>>(slice: I) -> Self {
        Self::from_vec(slice.as_ref().to_vec())
    }

    /// Extends the array from a slice of partial aggregates
    pub fn extend_from_slice<I: AsRef<[A::PartialAggregate]>>(&mut self, slice: I) {
        self.inner.extend_from_slice(slice.as_ref())
    }

    #[doc(hidden)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[doc(hidden)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    #[doc(hidden)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[doc(hidden)]
    #[inline]
    pub fn push_front(&mut self, agg: A::PartialAggregate) {
        self.inner.insert(0, agg);
    }

    #[doc(hidden)]
    pub fn push_front_all(&mut self, iter: impl IntoIterator<Item = A::PartialAggregate>) {
        for agg in iter {
            self.push_front(agg);
        }
    }
    #[doc(hidden)]
    #[inline]
    pub fn pop_back(&mut self) {
        let _ = self.inner.pop();
    }

    /// Merges another mutable array into this array
    pub fn merge(&mut self, other: &Self) {
        A::merge_slices(&mut self.inner, &other.inner);
    }
    /// Merges a partial array into this mutable array
    pub fn merge_with_ref(&mut self, other: impl AsRef<[A::PartialAggregate]>) {
        A::merge_slices(&mut self.inner, other.as_ref());
    }

    /// Returns a front-to-back iterator of roll-up slots
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &A::PartialAggregate> {
        self.inner.iter()
    }
    /// Returns combined partial aggregate based on a given range
    #[inline]
    pub fn range_to_vec<R>(&self, range: R) -> Vec<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        self.inner[into_range(&range, self.inner.len())]
            .iter()
            .copied()
            .rev()
            .collect()
    }

    /// Returns combined partial aggregate based on a given range
    #[inline]
    pub fn range_query<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        A::combine_slice(&self.inner[into_range(&range, self.inner.len())])
    }

    /// Returns the combined partial aggregate within the given range that match the filter predicate
    #[inline]
    pub fn range_query_with_filter<R>(
        &self,
        range: R,
        filter: impl Fn(&A::PartialAggregate) -> bool,
    ) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        let Range { start, end } = into_range(&range, self.inner.len());
        let slots = end - start;

        // Locate which slots we are to combine together
        let relevant_range = self.inner.iter().skip(start).take(slots);

        let mut accumulator: Option<A::PartialAggregate> = None;

        for partial in relevant_range {
            if filter(partial) {
                combine_or_insert::<A>(&mut accumulator, *partial);
            }
        }

        accumulator
    }

    /// - If given a position, returns the drill down slots based on that position,
    ///   or `None` if out of bounds
    /// - If `0` is specified, it will drill down the current head.
    #[inline]
    pub fn get(&self, slot: usize) -> Option<&A::PartialAggregate> {
        self.inner.get(slot)
    }
    /// Returns the mutable partial aggregate at the given position
    #[inline]
    pub fn get_mut(&mut self, slot: usize) -> Option<&mut A::PartialAggregate> {
        self.inner.get_mut(slot)
    }
}
impl<A: Aggregator> AsRef<[A::PartialAggregate]> for MutablePartialArray<A> {
    fn as_ref(&self) -> &[A::PartialAggregate] {
        &self.inner
    }
}

/// An event-time indexed array using prefix-sum optimization to answer queries at O(1)
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Default, Clone, Debug)]
pub struct PrefixArray<A: Aggregator> {
    slots: MutablePartialArray<A>,
    prefix: MutablePartialArray<A>,
}

impl<A: Aggregator> PrefixArray<A> {
    fn rebuild_prefix(&mut self) {
        self.prefix = MutablePartialArray::from_vec(A::build_prefix(self.slots.as_ref()));
    }
    pub(crate) fn _from_array(array: MutablePartialArray<A>) -> Self {
        let prefix = MutablePartialArray::from_vec(A::build_prefix(array.as_ref()));
        Self {
            slots: array,
            prefix,
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }
    pub(crate) fn push_front(&mut self, agg: A::PartialAggregate) {
        self.slots.push_front(agg);
        self.rebuild_prefix();
    }
    pub(crate) fn pop_back(&mut self) {
        self.slots.pop_back();
        self.rebuild_prefix();
    }
    #[inline]
    pub(crate) fn get(&self, slot: usize) -> Option<&A::PartialAggregate> {
        self.slots.get(slot)
    }
    #[inline]
    pub(crate) fn _iter(&self) -> impl Iterator<Item = &A::PartialAggregate> {
        self.slots.iter()
    }

    #[inline]
    pub(crate) fn range_query<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        let len = self.prefix.len();

        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n - 1,
            Bound::Unbounded => len,
        };
        A::prefix_query(self.prefix.as_ref(), start, end)
    }
}
