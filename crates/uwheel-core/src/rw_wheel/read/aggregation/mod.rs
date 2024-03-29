use crate::{aggregator::Aggregator, time_internal::Duration};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::{
    assert,
    fmt::Debug,
    mem,
    ops::{Range, RangeBounds},
    option::{
        Option,
        Option::{None, Some},
    },
};

#[cfg(feature = "profiler")]
use uwheel_stats::profile_scope;

#[cfg(feature = "profiler")]
pub(crate) mod stats;

/// Array implementations for Partial Aggregates
pub mod array;
/// Configuration for [Wheel]
pub mod conf;
/// Iterator implementations for [Wheel]
pub mod iter;
/// A maybe initialized [Wheel]
pub mod maybe;

mod data;

#[cfg(feature = "profiler")]
use stats::Stats;

use array::MutablePartialArray;

use self::{
    conf::{DataLayout, RetentionPolicy, WheelConf},
    data::Data,
};

/// Combine partial aggregates or insert new entry
#[inline]
pub fn combine_or_insert<A: Aggregator>(
    dest: &mut Option<A::PartialAggregate>,
    entry: A::PartialAggregate,
) {
    match dest {
        Some(curr) => {
            let new_curr = A::combine(*curr, entry);
            *curr = new_curr;
        }
        None => {
            *dest = Some(entry);
        }
    }
}

/// Defines a drill-down cut
pub struct DrillCut<R>
where
    R: RangeBounds<usize>,
{
    /// slot ``subtrahend`` from head
    pub slot: usize,
    /// Range of partial aggregates within the drill down slots
    pub range: R,
}
// NOTE: move to slice::range function once it is stable
#[inline]
fn into_range(range: &impl RangeBounds<usize>, len: usize) -> Range<usize> {
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
    assert!(start <= end, "lower bound was too large");
    assert!(end <= len, "upper bound was too large");
    start..end
}

/// Data contained in a wheel slot
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug)]
pub struct WheelSlot<A: Aggregator> {
    /// A possible partial aggregate
    pub total: A::PartialAggregate,
    /// An array of partial aggregate slots
    ///
    /// The combined aggregate of these slots equal to the `total` field
    pub drill_down_slots: Option<Vec<A::PartialAggregate>>,
}
impl<A: Aggregator> WheelSlot<A> {
    /// Creates a new wheel slot
    pub fn new(
        total: Option<A::PartialAggregate>,
        drill_down_slots: Option<Vec<A::PartialAggregate>>,
    ) -> Self {
        Self {
            total: total.unwrap_or(A::IDENTITY),
            drill_down_slots,
        }
    }
    #[cfg(test)]
    fn with_total(total: Option<A::PartialAggregate>) -> Self {
        Self::new(total, None)
    }
}

/// Fixed-size wheel where each slot contains a possible partial aggregate
///
/// The wheel maintains partial aggregates per slot, but also updates a `total` aggregate for each tick in the wheel.
/// The total aggregate is returned once a full rotation occurs. This way the same wheel structure can be used between different hierarchical levels (e.g., seconds, minutes, hours, days)
#[repr(C)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone, Debug)]
pub struct Wheel<A: Aggregator> {
    /// Number of slots (60 seconds => 60 slots)
    capacity: usize,
    /// Total number of wheel slots (must be power of two)
    num_slots: usize,
    /// Partial aggregate for a full rotation
    total: Option<A::PartialAggregate>,
    /// The current watermark for this wheel
    watermark: u64,
    /// Configured tick size in milliseconds (seconds wheel -> 1000ms)
    tick_size_ms: u64,
    /// Retention policy for the wheel
    retention: RetentionPolicy,
    /// flag indicating whether this wheel is configured with drill-down
    drill_down: bool,
    data: Data<A>,
    /// Higher-order aggregates indexed by event time
    drill_down_slots: MutablePartialArray<A>,
    /// Keeps track whether we have done a full rotation (rotation_count == num_slots)
    rotation_count: usize,
    #[cfg(test)]
    pub(crate) total_ticks: usize,
    #[cfg(feature = "profiler")]
    stats: Stats,
}

impl<A: Aggregator> Wheel<A> {
    /// Creates a new Wheel using the given [WheelConf]
    pub fn new(conf: WheelConf) -> Self {
        let capacity = conf.capacity;
        let num_slots = crate::capacity_to_slots!(capacity);

        // Configure data layout
        let data = match conf.data_layout {
            DataLayout::Normal => Data::create_array_with_capacity(num_slots),
            DataLayout::Prefix => {
                assert!(
                    A::invertible(),
                    "Cannot configure prefix-sum without invertible agg function"
                );
                Data::create_prefix_array()
            }
            DataLayout::Compressed(chunk_size) => {
                assert!(A::compression_support(), "Compressed data layout requires the aggregator to implement compressor + decompressor");
                Data::create_compressed_array(chunk_size)
            }
        };

        Self {
            capacity,
            num_slots,
            data,
            drill_down_slots: MutablePartialArray::default(),
            total: None,
            watermark: conf.watermark,
            tick_size_ms: conf.tick_size_ms,
            drill_down: conf.drill_down,
            retention: conf.retention,
            rotation_count: 0,
            #[cfg(test)]
            total_ticks: 0,
            #[cfg(feature = "profiler")]
            stats: Stats::default(),
        }
    }

    /// Returns the current watermark of the wheel
    pub fn watermark(&self) -> u64 {
        self.watermark
    }
    /// Returns the current watermark of the wheel as a Duration
    pub fn now(&self) -> Duration {
        Duration::milliseconds(self.watermark as i64)
    }

    /// Returns ``Some(size)``of the drill down if it is enabled or ```None`` if its not
    fn _drill_down_step(&self) -> Option<usize> {
        if self.drill_down && !self.drill_down_slots.is_empty() {
            Some(self.drill_down_slots.len() / self.data.len())
        } else {
            None
        }
    }

    /// Combines partial aggregates of the last `subtrahend` slots
    ///
    /// - If given a interval, returns the combined partial aggregate based on that interval,
    ///   or `None` if out of bounds
    #[inline]
    pub fn interval(&self, subtrahend: usize) -> (Option<A::PartialAggregate>, usize) {
        if subtrahend > self.data.len() {
            (None, 0)
        } else {
            (self.data.aggregate(0..subtrahend), subtrahend)
        }
    }

    /// This function takes the current rotation into context and if the interval is equal to the rotation count,
    /// then it will return the total for the rotation otherwise call the regular interval function.
    #[inline]
    pub fn interval_or_total(&self, subtrahend: usize) -> (Option<A::PartialAggregate>, usize) {
        if subtrahend == self.rotation_count {
            (self.total(), 0)
        } else {
            self.interval(subtrahend)
        }
    }

    /// Combines partial aggregates of the last `subtrahend` slots and lowers it to a final aggregate
    ///
    /// - If given a interval, returns the final aggregate based on that interval,
    ///   or `None` if out of bounds
    #[inline]
    pub fn lower_interval(&self, subtrahend: usize) -> Option<A::Aggregate> {
        self.interval(subtrahend)
            .0
            .map(|partial_agg| A::lower(partial_agg))
    }

    /// Returns drill down slots from ´slot´ index
    #[inline]
    pub fn drill_down(&self, _slot: usize) -> Option<&[A::PartialAggregate]> {
        None
        // self.drill_down_slots.get(
    }

    /// Returns partial aggregate from `subtrahend` slots backwards from the head
    ///
    /// - If given a position, returns the partial aggregate based on that position,
    ///   or `None` if out of bounds
    /// - If `0` is specified, it will return the current head.
    #[inline]
    pub fn at(&self, subtrahend: usize) -> Option<&A::PartialAggregate> {
        if subtrahend > self.len() {
            None
        } else {
            unimplemented!();
            // self.data.get(subtrahend)
        }
    }

    /// Lowers partial aggregate from `subtrahend` slots backwards from the head
    ///
    /// - If given a position, returns the final aggregate based on that position,
    ///   or `None` if out of bounds
    /// - If `0` is specified, it will lower the current head.
    #[inline]
    pub fn lower_at(&self, subtrahend: usize) -> Option<A::Aggregate> {
        self.at(subtrahend).map(|res| A::lower(*res))
    }

    // helper method to combine drill-down N slots into 1 drill-down slot.
    #[inline]
    fn _combine_drill_down_slots<'a>(
        &self,
        iter: impl Iterator<Item = Option<&'a [A::PartialAggregate]>>,
    ) -> Vec<A::PartialAggregate> {
        iter.flatten().fold(Vec::new(), |mut res, slot| {
            if res.is_empty() {
                res.extend_from_slice(slot);
            } else {
                for (curr, other) in res.iter_mut().zip(slot) {
                    *curr = A::combine(*curr, *other);
                }
            }
            res
        })
    }

    /// Returns ``true`` if the underlying data is a PrefixArray
    #[inline]
    pub fn is_prefix(&self) -> bool {
        matches!(self.data, Data::PrefixArray(_))
    }

    #[doc(hidden)]
    pub fn to_prefix_array(&mut self) {
        assert!(A::invertible());
        if let Data::Array(array) = &self.data {
            let mut prefix_data = Data::array_to_prefix(array);
            core::mem::swap(&mut self.data, &mut prefix_data);
        }
    }

    #[doc(hidden)]
    pub fn to_array(&mut self) {
        assert!(A::invertible());
        if let Data::PrefixArray(prefix_array) = &self.data {
            let mut array_data = Data::prefix_to_array(prefix_array);
            core::mem::swap(&mut self.data, &mut array_data);
        }
    }

    /// Returns number of slots used
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns ``true`` if the wheel is empty otherwise ``false``
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of queryable slots
    #[inline]
    pub fn total_slots(&self) -> usize {
        self.data.len()
    }

    /// Executes a wheel aggregation given the [start, end) range and combines it to a final partial aggregate
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    #[inline]
    pub fn aggregate<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        self.data.aggregate(range)
    }

    /// Combines partial aggregates from the specified range and lowers it to a final aggregate value
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    pub fn aggregate_and_lower<R>(&self, range: R) -> Option<A::Aggregate>
    where
        R: RangeBounds<usize>,
    {
        self.aggregate(range).map(A::lower)
    }

    /// Shift the tail and clear any old entry
    #[inline]
    fn clear_tail(&mut self) {
        if !self.data.is_empty() && self.retention.should_drop() {
            self.data.pop_back();
        } else if let RetentionPolicy::KeepWithLimit(limit) = self.retention {
            if self.data.len() > self.capacity + limit {
                self.data.pop_back();
            }
        };
    }
    /// Returns the current rotation position in the wheel
    pub fn rotation_count(&self) -> usize {
        self.rotation_count
    }

    /// Ticks left until the wheel fully rotates
    #[inline]
    pub fn ticks_remaining(&self) -> usize {
        self.capacity - self.rotation_count
    }

    fn size_bytesz(&self) -> Option<usize> {
        let data_size = self.data.size_bytes(); // as it is on the heap
        Some(mem::size_of::<Self>() + data_size)
    }

    /// Clears the wheel
    pub fn clear(&mut self) {
        let mut new = Self::new(WheelConf {
            capacity: self.capacity,
            watermark: self.watermark,
            data_layout: self.data.layout(),
            tick_size_ms: self.tick_size_ms,
            retention: self.retention,
            drill_down: self.drill_down,
        });
        core::mem::swap(self, &mut new);
    }

    /// Returns the partial aggregate for the current rotation
    #[inline]
    pub fn total(&self) -> Option<A::PartialAggregate> {
        #[cfg(feature = "profiler")]
        self.stats.bump_total();

        self.total
    }

    /// Insert PartialAggregate into the head of the wheel
    #[inline]
    pub fn insert_head(&mut self, entry: A::PartialAggregate) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.insert);
        self.data.push_front(entry);
    }

    #[inline]
    #[doc(hidden)]
    pub fn insert_slot(&mut self, slot: WheelSlot<A>) {
        // update roll-up aggregates
        self.insert_head(slot.total);
    }

    /// Merges a vector of wheels in parallel into an Wheel
    pub fn merge_many(wheels: Vec<Self>) -> Option<Self> {
        wheels.into_iter().reduce(|mut a, b| {
            a.merge(&b);
            a
        })
    }

    /// Returns a reference to roll-up slots
    pub fn slots(&self) -> &MutablePartialArray<A> {
        unimplemented!();
    }

    /// Merge two Wheels of similar granularity
    ///
    /// NOTE: must ensure wheels have been advanced to the same time
    #[allow(clippy::useless_conversion)]
    #[inline]
    pub fn merge(&mut self, other: &Self) {
        // merge current total
        if let Some(other_total) = other.total {
            combine_or_insert::<A>(&mut self.total, other_total)
        }

        self.data.merge(&other.data);
    }

    /// Tick the wheel by 1 slot
    #[inline]
    pub fn tick(&mut self) -> Option<WheelSlot<A>> {
        // bump internal low watermark
        self.watermark += self.tick_size_ms;

        // Possibly update the partial aggregate for the current rotation
        if let Some(curr) = self.data.get(0) {
            combine_or_insert::<A>(&mut self.total, *curr);
        }

        // If the wheel is full, we clear the oldest entry
        if self.is_full() {
            self.clear_tail();
        }

        self.rotation_count += 1;

        #[cfg(test)]
        {
            self.total_ticks += 1;
        }

        // Return rotation data if wheel has doen a full rotation
        if self.rotation_count == self.capacity {
            // our total partial aggregate to be rolled up
            let total = self.total.take();

            // reset count
            self.rotation_count = 0;
            Some(WheelSlot::new(total, None))
        } else {
            None
        }
    }

    /// Check whether this wheel is utilising all its slots
    #[inline]
    pub fn is_full(&self) -> bool {
        self.data.len() >= self.capacity
    }

    #[cfg(feature = "profiler")]
    /// Returns a reference to the stats of the [Wheel]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        aggregator::{
            min::U64MinAggregator,
            sum::{U32SumAggregator, U64SumAggregator},
            Compression,
        },
        rw_wheel::read::hierarchical::HOUR_TICK_MS,
    };

    #[derive(Clone, Debug, Default)]
    pub struct PcoSumAggregator;

    impl Aggregator for PcoSumAggregator {
        const IDENTITY: Self::PartialAggregate = 0;
        type CombineSimd = fn(&[Self::PartialAggregate]) -> Self::PartialAggregate;

        type CombineInverse =
            fn(Self::PartialAggregate, Self::PartialAggregate) -> Self::PartialAggregate;

        type Input = u32;
        type PartialAggregate = u32;
        type MutablePartialAggregate = u32;
        type Aggregate = u32;

        fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
            input
        }
        fn combine_mutable(mutable: &mut Self::MutablePartialAggregate, input: Self::Input) {
            *mutable += input
        }
        fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
            a
        }

        fn combine(a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
            a + b
        }
        fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
            a
        }

        fn compression() -> Option<Compression<Self::PartialAggregate>> {
            let compressor = Box::new(|slice: &[u32]| {
                pco::standalone::auto_compress(slice, pco::DEFAULT_COMPRESSION_LEVEL)
            });
            let decompressor = Box::new(|slice: &[u8]| {
                pco::standalone::auto_decompress(slice).expect("failed to decompress")
            });
            Some(Compression::new(compressor, decompressor))
        }
    }

    #[test]
    #[should_panic]
    fn invalid_prefix_conf() {
        let conf = WheelConf::new(HOUR_TICK_MS, 24)
            .with_retention_policy(RetentionPolicy::Keep)
            .with_data_layout(DataLayout::Prefix);
        // should panic as U64MinAggregator does not support prefix-sum
        let _wheel = Wheel::<U64MinAggregator>::new(conf);
    }

    #[test]
    fn agg_wheel_prefix_test() {
        let prefix_conf = WheelConf::new(HOUR_TICK_MS, 24)
            .with_retention_policy(RetentionPolicy::Keep)
            .with_data_layout(DataLayout::Prefix);
        let mut prefix_wheel = Wheel::<U64SumAggregator>::new(prefix_conf);

        let conf = WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
        let mut wheel = Wheel::<U64SumAggregator>::new(conf);

        for i in 0..30 {
            wheel.insert_slot(WheelSlot::with_total(Some(i)));
            prefix_wheel.insert_slot(WheelSlot::with_total(Some(i)));

            wheel.tick();
            prefix_wheel.tick();
        }

        // Verify that a prefix-sum range query returns the same result as a regular scan.
        let prefix_result = prefix_wheel.aggregate(0..4);
        let result = wheel.aggregate(0..4);
        assert_eq!(prefix_result, result);
    }

    #[test]
    fn compressed_array_test() {
        let compressed_conf = WheelConf::new(HOUR_TICK_MS, 24)
            .with_retention_policy(RetentionPolicy::Keep)
            .with_data_layout(DataLayout::Compressed(60));
        let mut compressed_wheel = Wheel::<PcoSumAggregator>::new(compressed_conf);

        let conf = WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
        let mut wheel = Wheel::<U32SumAggregator>::new(conf);

        for i in 0..120 {
            wheel.insert_slot(WheelSlot::with_total(Some(i)));
            compressed_wheel.insert_slot(WheelSlot::with_total(Some(i)));

            wheel.tick();
            compressed_wheel.tick();
        }

        let compressed_result = compressed_wheel.aggregate(0..4);
        let result = wheel.aggregate(0..4);
        assert_eq!(compressed_result, result);

        assert_eq!(compressed_wheel.aggregate(0..4), wheel.aggregate(0..4));
        assert_eq!(compressed_wheel.aggregate(55..75), wheel.aggregate(55..75));
        assert_eq!(
            compressed_wheel.aggregate(60..120),
            wheel.aggregate(60..120)
        );

        // add half of the chunk_size to check whether it still works as intended
        for i in 0..30 {
            wheel.insert_slot(WheelSlot::with_total(Some(i)));
            compressed_wheel.insert_slot(WheelSlot::with_total(Some(i)));

            wheel.tick();
            compressed_wheel.tick();
        }

        assert_eq!(compressed_wheel.aggregate(55..75), wheel.aggregate(55..75));
        assert_eq!(compressed_wheel.aggregate(60..85), wheel.aggregate(60..85));
    }

    #[test]
    fn mutable_partial_array_test() {
        let mut array = MutablePartialArray::<U64SumAggregator>::default();

        array.push_front(10);
        array.push_front(20);
        array.push_front(30);

        assert_eq!(array.len(), 3);
        assert_eq!(array.range_query(..), Some(60));
        assert_eq!(array.range_query(0..2), Some(50));
        assert_eq!(array.range_query_with_filter(.., |v| *v > 10), Some(50));

        assert_eq!(array.get(0), Some(&30));
        assert_eq!(array.get(1), Some(&20));
        assert_eq!(array.get(2), Some(&10));

        array.pop_back();

        assert_eq!(array.len(), 2);
        assert_eq!(array.range_query(..), Some(50));
        assert_eq!(array.range_query(0..2), Some(50));
    }

    #[test]
    fn retention_keep_test() {
        let conf = WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
        let mut wheel = Wheel::<U64SumAggregator>::new(conf);

        for i in 0..60 {
            wheel.insert_slot(WheelSlot::with_total(Some(i)));
            wheel.tick();
        }
        assert_eq!(wheel.total_slots(), 60);
    }
    #[test]
    fn retention_keep_with_limit_test() {
        let conf = WheelConf::new(HOUR_TICK_MS, 24)
            .with_retention_policy(RetentionPolicy::KeepWithLimit(10));
        let mut wheel = Wheel::<U64SumAggregator>::new(conf);

        for i in 0..60 {
            wheel.insert_slot(WheelSlot::with_total(Some(i)));
            wheel.tick();
        }
        assert_eq!(wheel.total_slots(), 24 + 10);
    }
}
