use super::{combine_or_insert, into_range};
use crate::Aggregator;
use core::ops::{Bound, Deref, DerefMut, Range, RangeBounds};

#[cfg(not(feature = "std"))]
use alloc::collections::VecDeque;

#[cfg(feature = "std")]
use std::collections::VecDeque;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// An event-time indexed deque containing partial aggregates
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Default, Clone, Debug)]
pub struct MutablePartialDeque<A: Aggregator> {
    inner: VecDeque<A::PartialAggregate>,
}

impl<A: Aggregator> MutablePartialDeque<A> {
    /// Creates an deque  with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity),
        }
    }
    /// Creates an deque from a vector of partial aggregates
    pub fn from_vec(partials: Vec<A::PartialAggregate>) -> Self {
        Self {
            inner: VecDeque::from(partials),
        }
    }
    /// Creates an deque from a slice of partial aggregates
    pub fn from_slice<I: AsRef<[A::PartialAggregate]>>(slice: I) -> Self {
        Self::from_vec(slice.as_ref().to_vec())
    }
    #[doc(hidden)]
    pub fn size_bytes(&self) -> usize {
        core::mem::size_of::<A::PartialAggregate>() * self.inner.len()
    }

    #[doc(hidden)]
    pub fn as_slice(&self) -> &[A::PartialAggregate] {
        // SAFETY: Assumes VecDeque::make_contigious has been called prior to this
        self.inner.as_slices().0
    }
    #[doc(hidden)]
    pub fn as_mut_slice(&mut self) -> &mut [A::PartialAggregate] {
        // SAFETY: Assumes VecDeque::make_contigious has been called prior to this
        self.inner.as_mut_slices().0
    }

    #[doc(hidden)]
    #[inline]
    pub fn push_front(&mut self, agg: A::PartialAggregate) {
        self.inner.push_front(agg);
    }

    #[doc(hidden)]
    #[inline]
    pub fn make_contiguous(&mut self) {
        self.inner.make_contiguous();
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
        let _ = self.inner.pop_back();
    }

    /// Merges another mutable deque into this one
    pub fn merge(&mut self, other: &Self) {
        self.inner.make_contiguous();
        A::merge(self.as_mut_slice(), other.as_slice());
    }

    /// Returns partial aggregate based on a given range
    #[inline]
    pub fn range<R>(&self, range: R) -> Vec<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        let Range { start, end } = into_range(&range, self.inner.len());
        let slots = end - start;
        self.inner
            .iter()
            .skip(start)
            .take(slots)
            .copied()
            .rev()
            .collect()
    }

    /// Combines partial aggregates within the given range into a new partial aggregate
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of deque
    #[inline]
    pub fn combine_range<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        if A::simd_support() {
            // SAFETY: assumes the inner deque has been made contigious
            A::combine_slice(&self.as_slice()[into_range(&range, self.inner.len())])
        } else {
            let Range { start, end } = into_range(&range, self.inner.len());
            let slots = end - start;
            Some(
                self.inner
                    .iter()
                    .skip(start)
                    .take(slots)
                    .copied()
                    .fold(A::IDENTITY, A::combine),
            )
        }
    }

    /// Returns the combined partial aggregate within the given range that match the filter predicate
    #[inline]
    pub fn combine_range_with_filter<R>(
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

impl<A: Aggregator> Deref for MutablePartialDeque<A> {
    type Target = VecDeque<A::PartialAggregate>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<A: Aggregator> DerefMut for MutablePartialDeque<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// An event-time indexed deque using prefix-sum optimization to answer queries at O(1)
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Default, Clone, Debug)]
pub struct PrefixDeque<A: Aggregator> {
    slots: MutablePartialDeque<A>,
    prefix: MutablePartialDeque<A>,
}

impl<A: Aggregator> PrefixDeque<A> {
    fn rebuild_prefix(&mut self) {
        // SAFETY: make sure all data points are included in our slice
        self.slots.make_contiguous();

        self.prefix = MutablePartialDeque::from_vec(A::build_prefix(self.slots.as_slice()));
    }
    pub(crate) fn _from_deque(deque: &MutablePartialDeque<A>) -> Self {
        let prefix = MutablePartialDeque::from_vec(A::build_prefix(deque.as_slice()));
        Self {
            slots: deque.clone(),
            prefix,
        }
    }
    #[doc(hidden)]
    pub fn size_bytes(&self) -> usize {
        let bytes = core::mem::size_of::<A::PartialAggregate>() * self.slots.len();
        bytes * 2 // since slots + prefix same size
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
    pub(crate) fn slots_slice(&self) -> &[A::PartialAggregate] {
        self.slots.as_slice()
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
    pub(crate) fn range<R>(&self, range: R) -> Vec<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        self.slots.range(range)
    }

    #[inline]
    pub(crate) fn combine_range<R>(&self, range: R) -> Option<A::PartialAggregate>
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
            Bound::Unbounded => len - 1,
        };
        A::prefix_query(self.prefix.as_slice(), start, end)
    }
}

/// A Compressed deque which enables user-defined compression
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone, Debug)]
pub struct CompressedDeque<A: Aggregator> {
    head: A::PartialAggregate,
    buffer: MutablePartialDeque<A>,
    chunks: VecDeque<Vec<u8>>,
    pub(crate) chunk_size: usize,
}

impl<A: Aggregator> CompressedDeque<A> {
    pub(crate) fn new(chunk_size: usize) -> Self {
        assert!(
            A::compression_support(),
            "CompressedDeque requires the Compression method to implemented in Aggregator"
        );

        Self {
            head: A::IDENTITY,
            buffer: Default::default(),
            chunks: Default::default(),
            chunk_size,
        }
    }

    pub(crate) fn get(&self, slot: usize) -> Option<A::PartialAggregate> {
        if slot == 0 {
            Some(self.head)
        } else {
            let mut full_data = Vec::with_capacity(self.len());

            full_data.extend(self.buffer.iter().copied().collect::<Vec<_>>());

            // Iterate in newest to oldest order
            for chunk in self.chunks.iter() {
                // SAFETY: we are sure that the compression fn is implemented since we assert it in the constructor
                let decompressor = A::compression().unwrap().decompressor;
                let decompressed_chunk = (decompressor)(chunk);
                full_data.extend_from_slice(&decompressed_chunk);
            }
            full_data.get(slot).copied()
        }
    }

    pub(crate) fn len(&self) -> usize {
        (self.chunk_size * self.chunks.len()) + self.buffer.len()
    }
    pub(crate) fn _is_empty(&self) -> bool {
        self.len() == 0
    }

    #[doc(hidden)]
    pub fn size_bytes(&self) -> usize {
        let buffer_size = self.buffer.size_bytes();
        let compressed_size = self.chunks.iter().fold(0, |mut acc, chunk| {
            acc += chunk.len();
            acc
        });

        buffer_size + compressed_size
    }

    pub(crate) fn push_front(&mut self, agg: A::PartialAggregate) {
        // Keep track of the current head as it is used by `Wheel`
        // to updae the total for the current rotation.
        self.head = agg;

        self.buffer.push_front(agg);

        // if we have reached the chunk size then compress
        if self.buffer.len() == self.chunk_size {
            let compressor = A::compression().unwrap().compressor;
            let to_compress: Vec<_> = self.buffer.iter().copied().collect();
            let chunk = (compressor)(&to_compress);
            self.chunks.push_front(chunk);
            self.buffer.clear();
        }
    }
    pub(crate) fn pop_back(&mut self) {
        self.chunks.pop_back();
    }

    #[inline]
    pub(crate) fn range<R>(&self, range: R) -> Vec<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        let (deque, new_range) = self.partial_range_deque(&range);
        deque.range(new_range)
    }

    #[inline]
    pub(crate) fn combine_range<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        let (deque, new_range) = self.partial_range_deque(&range);
        deque.combine_range(new_range)
    }

    // helper method to build a temporary deque using the given range
    #[inline]
    fn partial_range_deque<R>(&self, range: &R) -> (MutablePartialDeque<A>, Range<usize>)
    where
        R: RangeBounds<usize>,
    {
        let len = self.len();

        let start = match range.start_bound() {
            core::ops::Bound::Included(&n) => n,
            core::ops::Bound::Excluded(&n) => n + 1,
            core::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            core::ops::Bound::Included(&n) => n + 1,
            core::ops::Bound::Excluded(&n) => n,
            core::ops::Bound::Unbounded => len,
        };

        let mut vec = Vec::new();

        // number of slots to query
        let slots = end - start;
        let buffer_size = self.buffer.len();
        let buffer_included = start <= self.chunk_size && buffer_size > 0;

        // check whether we need to include the buffer which is not compressed
        if buffer_included {
            vec.extend_from_slice(&self.buffer.inner.iter().copied().collect::<Vec<_>>());
        }

        let start_after_buffer = start.saturating_sub(buffer_size);

        // calculate start and end chunks
        let start_chunk = start_after_buffer / self.chunk_size;
        let end_chunk = end.saturating_sub(1).saturating_sub(buffer_size) / self.chunk_size;
        let chunks_to_decompress = end_chunk - start_chunk + 1;

        // SAFETY: we are sure that the compression fn is implemented since we assert it in the constructor
        let decompressor = A::compression().unwrap().decompressor;

        // Adjust the skip value based on whether the buffer is included
        let skip_chunks = if buffer_included {
            start_chunk.saturating_sub(1)
        } else {
            start_chunk
        };

        // decompress chunks and extend slots
        for chunk in self
            .chunks
            .iter()
            .skip(skip_chunks)
            .take(chunks_to_decompress)
        {
            let decompressed_chunk = (decompressor)(chunk);
            vec.extend_from_slice(&decompressed_chunk);
        }

        // Build a partial deque from the partial aggregates
        let partial_deque = MutablePartialDeque::from_vec(vec);

        // If buffer is included and we are only querying the buffer itself then return directly
        if buffer_included && end <= buffer_size {
            (partial_deque, start..end)
        } else {
            // NOTE: buffer is not included hence we need to recalculate the start and end indices

            // Calculate the remainder of the start index after the buffer
            let start_rem = start_after_buffer % self.chunk_size;

            // If buffer is included simply use the regular start point, else use the remainder
            let new_start = if buffer_included { start } else { start_rem };

            // calculate new end by adding total slots to new_start
            let new_end = new_start + slots;

            (partial_deque, new_start..new_end)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wheels::read::aggregation::tests::PcoSumAggregator;

    #[test]
    fn test_deque_consistency() {
        use crate::Aggregator;

        const N: usize = 1000;
        let values: Vec<u32> = (1..=N as u32).collect();
        let expected_sum: u32 = values.iter().sum();

        // Fill MutablePartialDeque
        let mut regular = MutablePartialDeque::<PcoSumAggregator>::default();
        for &v in values.iter() {
            regular.push_front(PcoSumAggregator::freeze(PcoSumAggregator::lift(v)));
        }

        // Fill CompressedDeque
        let mut compressed = CompressedDeque::<PcoSumAggregator>::new(128);
        for &v in values.iter() {
            compressed.push_front(PcoSumAggregator::freeze(PcoSumAggregator::lift(v)));
        }

        let range = 0..N;

        let sum_regular = regular.combine_range(range.clone()).unwrap();
        let sum_compressed = compressed.combine_range(range.clone()).unwrap();

        assert_eq!(sum_regular, expected_sum, "Regular sum mismatch");
        assert_eq!(sum_compressed, expected_sum, "Compressed sum mismatch");
    }
}
