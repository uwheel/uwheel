use core::{hash::Hash, marker::PhantomData};

use fastbloom::BloomFilter;

use crate::aggregator::{Aggregator, InputBounds, PartialAggregateType};

/// Partial aggregate wrapping an optional [`BloomFilter`].
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BloomPartial<
    T,
    const NUM_BITS: usize = DEFAULT_NUM_BITS,
    const NUM_HASHES: u32 = DEFAULT_NUM_HASHES,
    const SEED: u128 = DEFAULT_SEED,
> {
    inner: Option<BloomFilter>,
    #[cfg_attr(feature = "serde", serde(skip))]
    marker: PhantomData<T>,
}

impl<T, const NUM_BITS: usize, const NUM_HASHES: u32, const SEED: u128> Default
    for BloomPartial<T, NUM_BITS, NUM_HASHES, SEED>
{
    fn default() -> Self {
        Self::EMPTY
    }
}

impl<T, const NUM_BITS: usize, const NUM_HASHES: u32, const SEED: u128>
    BloomPartial<T, NUM_BITS, NUM_HASHES, SEED>
{
    const EMPTY: Self = Self {
        inner: None,
        marker: PhantomData,
    };

    /// Creates a partial aggregate from a [`BloomFilter`].
    pub fn from_filter(filter: BloomFilter) -> Self {
        Self {
            inner: Some(filter),
            marker: PhantomData,
        }
    }

    /// Consumes the partial aggregate and returns its [`BloomFilter`].
    pub fn into_filter(self) -> BloomFilter {
        self.inner
            .unwrap_or_else(BloomAggregator::<T, NUM_BITS, NUM_HASHES, SEED>::empty_filter)
    }

    /// Returns a clone of the underlying [`BloomFilter`] without consuming the partial aggregate.
    pub fn as_filter(&self) -> BloomFilter {
        self.inner
            .clone()
            .unwrap_or_else(BloomAggregator::<T, NUM_BITS, NUM_HASHES, SEED>::empty_filter)
    }

    /// Returns a reference to the underlying [`BloomFilter`], if present.
    pub fn as_ref(&self) -> Option<&BloomFilter> {
        self.inner.as_ref()
    }

    /// Checks if the filter possibly contains the provided value.
    #[inline]
    pub fn contains(&self, value: &T) -> bool
    where
        T: Hash,
    {
        self.inner
            .as_ref()
            .is_some_and(|filter| filter.contains(value))
    }

    fn union(self, other: Self) -> Self {
        match (self.inner, other.inner) {
            (Some(mut left), Some(right)) => {
                left.union(&right);
                Self::from_filter(left)
            }
            (Some(left), None) => Self::from_filter(left),
            (None, Some(right)) => Self::from_filter(right),
            (None, None) => Self::EMPTY,
        }
    }
}

impl<T, const NUM_BITS: usize, const NUM_HASHES: u32, const SEED: u128>
    From<BloomPartial<T, NUM_BITS, NUM_HASHES, SEED>> for BloomFilter
{
    fn from(value: BloomPartial<T, NUM_BITS, NUM_HASHES, SEED>) -> Self {
        value.into_filter()
    }
}

impl<T, const NUM_BITS: usize, const NUM_HASHES: u32, const SEED: u128>
    From<&BloomPartial<T, NUM_BITS, NUM_HASHES, SEED>> for BloomFilter
{
    fn from(value: &BloomPartial<T, NUM_BITS, NUM_HASHES, SEED>) -> Self {
        value.as_filter()
    }
}

impl<T, const NUM_BITS: usize, const NUM_HASHES: u32, const SEED: u128> PartialAggregateType
    for BloomPartial<T, NUM_BITS, NUM_HASHES, SEED>
where
    T: InputBounds,
{
}

const DEFAULT_NUM_BITS: usize = 65_536;
const DEFAULT_NUM_HASHES: u32 = 6;
const DEFAULT_SEED: u128 = 0xcbf2_9ce4_8422_2325_4c95_de35_c9a1_2d3b;

/// Bloom filter-based aggregator powered by the `fastbloom` crate.
#[derive(Debug, Clone, Copy)]
pub struct BloomAggregator<
    T,
    const NUM_BITS: usize = DEFAULT_NUM_BITS,
    const NUM_HASHES: u32 = DEFAULT_NUM_HASHES,
    const SEED: u128 = DEFAULT_SEED,
> {
    marker: PhantomData<T>,
}

impl<T, const NUM_BITS: usize, const NUM_HASHES: u32, const SEED: u128> Default
    for BloomAggregator<T, NUM_BITS, NUM_HASHES, SEED>
{
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T, const NUM_BITS: usize, const NUM_HASHES: u32, const SEED: u128>
    BloomAggregator<T, NUM_BITS, NUM_HASHES, SEED>
{
    fn empty_filter() -> BloomFilter {
        assert!(NUM_BITS > 0, "number of bits must be greater than zero");
        assert!(NUM_HASHES > 0, "number of hashes must be greater than zero");
        BloomFilter::with_num_bits(NUM_BITS)
            .seed(&SEED)
            .hashes(NUM_HASHES)
    }
}

impl<T, const NUM_BITS: usize, const NUM_HASHES: u32, const SEED: u128> Aggregator
    for BloomAggregator<T, NUM_BITS, NUM_HASHES, SEED>
where
    T: InputBounds + Hash + 'static,
{
    const IDENTITY: Self::PartialAggregate = BloomPartial::EMPTY;

    type Input = T;
    type MutablePartialAggregate = BloomFilter;
    type PartialAggregate = BloomPartial<T, NUM_BITS, NUM_HASHES, SEED>;
    type Aggregate = BloomFilter;

    fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
        let mut filter = Self::empty_filter();
        filter.insert(&input);
        filter
    }

    fn combine_mutable(mutable: &mut Self::MutablePartialAggregate, input: Self::Input) {
        mutable.insert(&input);
    }

    fn freeze(mutable: Self::MutablePartialAggregate) -> Self::PartialAggregate {
        BloomPartial::from_filter(mutable)
    }

    fn combine(a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a.union(b)
    }

    fn lower(partial: Self::PartialAggregate) -> Self::Aggregate {
        partial.into_filter()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Entry,
        RwWheel,
        WheelRange,
        aggregator::{Aggregator, bloom::BloomAggregator},
    };

    type BucketBloomAggregator = BloomAggregator<u32, 4096, 6, 0x0123_4567_89ab_cdef>;

    #[derive(Clone, Debug)]
    struct SensorEvent {
        timestamp_ms: u64,
        value: u32,
    }

    fn bucket(timestamp_ms: u64) -> u32 {
        (timestamp_ms / 1_000) as u32
    }

    #[test]
    fn filter_reports_temporal_membership() {
        const THRESHOLD: u32 = 70;
        let events = vec![
            SensorEvent {
                timestamp_ms: 1_000,
                value: 65,
            },
            SensorEvent {
                timestamp_ms: 2_000,
                value: 72,
            },
            SensorEvent {
                timestamp_ms: 3_000,
                value: 88,
            },
            SensorEvent {
                timestamp_ms: 6_000,
                value: 63,
            },
            SensorEvent {
                timestamp_ms: 7_000,
                value: 95,
            },
            SensorEvent {
                timestamp_ms: 9_000,
                value: 77,
            },
        ];

        let mut wheel: RwWheel<BucketBloomAggregator> = RwWheel::new(0);

        for event in &events {
            if event.value >= THRESHOLD {
                wheel.insert(Entry::new(bucket(event.timestamp_ms), event.timestamp_ms));
            }
        }

        let _ = wheel.advance_to(12_000);

        let ranges = [
            (0, 4_000, vec![2, 3]),
            (4_000, 8_000, vec![7]),
            (8_000, 12_000, vec![9]),
        ];

        for (start, end, buckets) in ranges {
            let filter = wheel
                .read()
                .combine_range(WheelRange::new_unchecked(start, end))
                .expect("range should produce aggregate")
                .into_filter();
            assert_eq!(filter, expected_filter(&buckets));
        }
    }

    #[test]
    fn filter_combines_partials_using_union() {
        let left_partial = BucketBloomAggregator::freeze({
            let mut filter = BucketBloomAggregator::lift(1);
            BucketBloomAggregator::combine_mutable(&mut filter, 3);
            filter
        });

        let right_partial = BucketBloomAggregator::freeze({
            let mut filter = BucketBloomAggregator::lift(2);
            BucketBloomAggregator::combine_mutable(&mut filter, 4);
            filter
        });

        let combined = BucketBloomAggregator::combine(left_partial, right_partial);
        assert_eq!(
            BucketBloomAggregator::lower(combined),
            expected_filter(&[1, 2, 3, 4])
        );
    }

    fn expected_filter(values: &[u32]) -> fastbloom::BloomFilter {
        let mut filter = BucketBloomAggregator::empty_filter();
        for value in values {
            filter.insert(value);
        }
        filter
    }
}
