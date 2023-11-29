use crate::{aggregator::Aggregator, time_internal::Duration};
#[cfg(feature = "std")]
use std::collections::VecDeque;

#[cfg(feature = "rayon")]
use rayon::prelude::*;

#[cfg(not(feature = "std"))]
use alloc::{collections::VecDeque, vec::Vec};
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

#[cfg(not(feature = "std"))]
use alloc::collections::vec_deque;
#[cfg(feature = "std")]
use std::collections::vec_deque;

#[cfg(feature = "profiler")]
pub(crate) mod stats;

/// Array implementations for Partial Aggregates
pub mod array;
/// Configuration for [AggregationWheel]
pub mod conf;
/// Iterator implementations for [AggregationWheel]
pub mod iter;
/// A maybe initialized [AggregationWheel]
pub mod maybe;

#[cfg(feature = "profiler")]
use stats::Stats;

use crate::rw_wheel::WheelExt;
use array::MutablePartialArray;

use self::{
    array::PartialArray,
    conf::{CompressionPolicy, RetentionPolicy, WheelConf},
};

/// File format identifier ("WHEEL")
const MAGIC_NUMBER: [u8; 5] = [0x57, 0x48, 0x45, 0x45, 0x4C];
/// Wheel version
const VERSION: u8 = 1;

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

/// A helper alias for an iterator over the Table
type TableIter<'a, A> = vec_deque::Iter<'a, A>;

/// A Table maintaining wheel slots which are event-time indexed
#[allow(missing_docs)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone, Default, Debug)]
pub struct Table<A: Aggregator> {
    _watermark: u64,
    // chunks: Vec<Chunk>,
    inner: VecDeque<WheelSlot<A>>,
}

impl<A: Aggregator> Table<A> {
    /// Creates a new table with the given watermark
    pub fn new(_watermark: u64) -> Self {
        Self {
            _watermark,
            inner: Default::default(),
        }
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
    pub fn push_front(&mut self, slot: WheelSlot<A>) {
        self.inner.push_front(slot);
    }
    #[doc(hidden)]
    pub fn pop_back(&mut self) {
        self.inner.pop_back();
    }
    /// Returns a front-to-back iterator of the Table
    pub fn iter(&self) -> TableIter<'_, WheelSlot<A>> {
        self.inner.iter()
    }

    /// Merges another Wheel Table into this one
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            self.inner.len(),
            other.inner.len(),
            "Both tables need to be the same length for merging"
        );

        for (self_slot, other_slot) in self.inner.iter_mut().zip(other.inner.iter()) {
            // combine the rolled up aggregate
            self_slot.total = A::combine(self_slot.total, other_slot.total);
            // combine_or_insert::<A>(&mut self_slot.total, other_slot.total);
        }

        // match (
        //     &mut self_slot.drill_down_slots,
        //     &other_slot.drill_down_slots,
        // ) {
        //     (Some(slots), Some(other_slots)) => {
        //         for (mut self_slot, other_slot) in
        //             slots.iter_mut().zip(other_slots.clone().iter())
        //         {
        // if let Some(other_agg) = other_slot {
        // combine_or_insert::<A>(self_slot, other_slot);
        // }
        // }
        // match (&mut self_slot, other_slot) {
        //     match (&mut self_slot, other_slot) {
        //         // if both wheels contains drill down slots, then decode, combine and encode into self
        //         (Some(self_encodes), Some(other_encodes)) => {
        //             let mut new_aggs = Vec::new();

        //             for (x, y) in self_encodes.iter_mut().zip(other_encodes) {
        //                 new_aggs.push(A::combine(*x, *y));
        //             }
        //             *self_slot = Some(new_aggs);
        //         }
        //         // if other wheel but not self, just move to self
        //         (None, Some(other_encodes)) => {
        //             *self_slot = Some(other_encodes.to_vec());
        //         }
        //         _ => {
        //             // do nothing
        //         }
        //     }
        // }
        //     }
        //     (Some(_), None) => panic!("only lhs wheel was configured with drill-down"),
        //     (None, Some(_)) => panic!("only rhs wheel was configured with drill-down"),
        //     _ => (),
        // }
    }

    // TODO: improve
    #[doc(hidden)]
    pub fn downsample<R>(&self, range: R, inner: R) -> Vec<Option<A::PartialAggregate>>
    where
        R: RangeBounds<usize>,
    {
        let Range { start, end } = into_range(&range, self.inner.len());
        let slots = end - start;

        // Locate which slots we are to combine together
        let relevant_range = self.inner.iter().skip(start).take(slots);

        let mut result: Vec<Option<A::PartialAggregate>> = Vec::new();

        for slot in relevant_range {
            if let Some(slots) = &slot.drill_down_slots {
                let Range { start, end } = into_range(&inner, slots.len());
                let partial = A::combine_slice(&slots[start..end]);
                result.push(partial);
            }
        }

        result
    }

    /// Returns combined partial aggregate based on a given range
    #[inline]
    pub fn range_query<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        // runs with a predicate that always returns true
        self.range_query_with_filter(range, |_| true)
    }

    #[doc(hidden)]
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

        for slot in relevant_range {
            if filter(&slot.total) {
                combine_or_insert::<A>(&mut accumulator, slot.total);
            }
        }

        accumulator
    }

    /// Returns drill down slots from `slot` slots backwards from the head
    ///
    ///
    /// - If given a position, returns the drill down slots based on that position,
    ///   or `None` if out of bounds
    /// - If `0` is specified, it will drill down the current head.
    #[inline]
    pub fn drill_down(&self, slot: usize) -> Option<&[A::PartialAggregate]> {
        self.inner
            .get(slot)
            .and_then(|m| m.drill_down_slots.as_deref())
    }

    /// Returns drill down slots from `slot` slots backwards from the head and lowers the aggregates
    ///
    /// If `0` is specified, it will drill down the current head.
    #[inline]
    pub fn lower_drill_down(&self, slot: usize) -> Option<Vec<A::Aggregate>> {
        self.drill_down(slot)
            .map(|partial_aggs| partial_aggs.iter().map(|agg| A::lower(*agg)).collect())
    }

    /// Drill down and cut across 2 slots
    ///
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    #[inline]
    pub fn drill_down_cut<AR, BR>(
        &self,
        a: DrillCut<AR>,
        b: DrillCut<BR>,
    ) -> Option<Vec<A::PartialAggregate>>
    where
        AR: RangeBounds<usize>,
        BR: RangeBounds<usize>,
    {
        let a_drill = self.drill_down(a.slot);
        let b_drill = self.drill_down(b.slot);
        let mut res = Vec::new();
        if let Some(a_slots) = a_drill {
            res.extend_from_slice(&a_slots[into_range(&a.range, a_slots.len())]);
        }
        if let Some(b_slots) = b_drill {
            res.extend_from_slice(&b_slots[into_range(&b.range, b_slots.len())]);
        }

        Some(res)
    }
    /// Drill down across 2 slots and lowers results
    ///
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    #[inline]
    pub fn lower_drill_down_cut<AR, BR>(
        &self,
        a: DrillCut<AR>,
        b: DrillCut<BR>,
    ) -> Option<Vec<A::Aggregate>>
    where
        AR: RangeBounds<usize>,
        BR: RangeBounds<usize>,
    {
        self.drill_down_cut(a, b)
            .map(|partials| partials.into_iter().map(|pa| A::lower(pa)).collect())
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
pub struct AggregationWheel<A: Aggregator> {
    /// Number of slots (60 seconds => 60 slots)
    capacity: usize,
    /// Total number of wheel slots (must be power of two)
    num_slots: usize,
    /// Partial aggregate for a full rotation
    total: Option<A::PartialAggregate>,
    /// The current watermark for this wheel
    watermark: u64,
    /// Compression policy for the wheel
    compression: CompressionPolicy,
    /// Configured tick size in milliseconds (seconds wheel -> 1000ms)
    tick_size_ms: u64,
    /// Retention policy for the wheel
    retention: RetentionPolicy,
    /// flag indicating whether this wheel is configured with drill-down
    drill_down: bool,
    /// Higher-order aggregates indexed by event time
    slots: MutablePartialArray<A>,
    /// Higher-order aggregates indexed by event time
    drill_down_slots: MutablePartialArray<A>,
    /// Table that maintains wheel slots
    table: Table<A>,
    /// Keeps track whether we have done a full rotation (rotation_count == num_slots)
    rotation_count: usize,
    #[cfg(test)]
    pub(crate) total_ticks: usize,
    #[cfg(feature = "profiler")]
    stats: Stats,
}

impl<A: Aggregator> AggregationWheel<A> {
    /// Creates a new AggregationWheel using the given [WheelConf]
    pub fn new(conf: WheelConf) -> Self {
        let capacity = conf.capacity;
        let num_slots = crate::capacity_to_slots!(capacity);

        Self {
            capacity,
            num_slots,
            slots: MutablePartialArray::with_capacity(num_slots),
            drill_down_slots: MutablePartialArray::default(),
            total: None,
            table: Table::new(0), // insert watermark
            watermark: conf.watermark,
            tick_size_ms: conf.tick_size_ms,
            drill_down: conf.drill_down,
            compression: conf.compression,
            retention: conf.retention,
            rotation_count: 0,
            #[cfg(test)]
            total_ticks: 0,
            #[cfg(feature = "profiler")]
            stats: Stats::default(),
        }
    }
    /// Returns a reference to the wheel table
    pub fn table(&self) -> &Table<A> {
        &self.table
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
            Some(self.drill_down_slots.len() / self.slots.len())
        } else {
            None
        }
    }

    /// Combines partial aggregates of the last `subtrahend` slots
    ///
    /// - If given a interval, returns the combined partial aggregate based on that interval,
    ///   or `None` if out of bounds
    #[inline]
    pub fn interval(&self, subtrahend: usize) -> Option<A::PartialAggregate> {
        if subtrahend > self.slots.len() {
            None
        } else {
            self.slots.range_query(0..subtrahend)
        }
    }

    /// This function takes the current rotation into context and if the interval is equal to the rotation count,
    /// then it will return the total for the rotation otherwise call the regular interval function.
    #[inline]
    pub fn interval_or_total(&self, subtrahend: usize) -> Option<A::PartialAggregate> {
        if subtrahend == self.rotation_count {
            self.total()
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
            .map(|partial_agg| A::lower(partial_agg))
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
            self.slots.get(subtrahend)
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

    /// Returns the number of queryable slots
    #[inline]
    pub fn total_slots(&self) -> usize {
        self.slots.len()
    }

    /// Combines partial aggregates within the given range into a new partial aggregate
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    #[inline]
    pub fn combine_range<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        self.slots.range_query(range)
    }
    /// Combines partial aggregates from the specified range and lowers it to a final aggregate value
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    pub fn combine_and_lower_range<R>(&self, range: R) -> Option<A::Aggregate>
    where
        R: RangeBounds<usize>,
    {
        self.combine_range(range).map(|res| A::lower(res))
    }

    /// Shift the tail and clear any old entry
    #[inline]
    fn clear_tail(&mut self) {
        if !self.slots.is_empty() && self.retention.should_drop() {
            self.slots.pop_back();
            self.table.pop_back();
        }
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
        // roll-up slots are stored on the heap, calculate how much bytes we are using for them..
        let inner_slots = mem::size_of::<Option<A::PartialAggregate>>() * self.slots.len();
        Some(mem::size_of::<Self>() + inner_slots)
    }

    /// Clears the wheel
    pub fn clear(&mut self) {
        let mut new = Self::new(WheelConf {
            capacity: self.capacity,
            watermark: self.watermark,
            compression: self.compression,
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
        self.slots.push_front(entry);
    }

    // /// Combines the partial at the current head
    // pub fn combine_head(&mut self, entry: A::PartialAggregate) {
    //     if let Some(ref mut head) = self.slotz.get(0) {
    //         // head = A::combine(*head.clone(), entry);
    //     }
    //     // combine_or_insert::<A>(head, entry)
    //     // self.slotz.push_front(entry);
    // }

    #[inline]
    #[doc(hidden)]
    pub fn insert_slot(&mut self, mut slot: WheelSlot<A>) {
        // update roll-up aggregates
        self.insert_head(slot.total);

        // if this wheel is configured to store data then update accordingly.
        if self.retention.should_keep() {
            // If this wheel should not maintain drill-down slots then make sure it is cleared.
            if !self.drill_down {
                slot.drill_down_slots = None;
            }

            // push slot into the table
            self.table.push_front(slot);

            // if there is a configured limit then check it
            if let RetentionPolicy::KeepWithLimit(limit) = self.retention {
                if self.slots.len() > limit {
                    self.table.pop_back();
                    self.slots.pop_back();
                }
            }
        }
    }

    /// Merges a vector of wheels in parallel into an AggregationWheel
    #[cfg(feature = "rayon")]
    pub fn merge_parallel(wheels: Vec<Self>) -> Option<Self> {
        wheels.into_par_iter().reduce_with(|mut a, b| {
            a.merge(&b);
            a
        })
    }

    /// Merges a vector of wheels in parallel into an AggregationWheel
    pub fn merge_many(wheels: Vec<Self>) -> Option<Self> {
        wheels.into_iter().reduce(|mut a, b| {
            a.merge(&b);
            a
        })
    }

    /// Merges the slots from a zero-copy partial array into this one
    pub fn merge_from_array<'a>(&mut self, array: &PartialArray<'a, A>) {
        if let Some(total) = array.combine_range(..) {
            combine_or_insert::<A>(&mut self.total, total);
        }
        self.slots.merge_with_ref(array);
    }

    /// Merges the slots from a reference
    pub fn merge_from_ref<'a>(&mut self, array: impl AsRef<[A::PartialAggregate]>) {
        if let Some(total) = A::combine_slice(array.as_ref()) {
            combine_or_insert::<A>(&mut self.total, total);
        }

        self.slots.merge_with_ref(array);
    }

    /// Merges the slots from an array partial arrays into this wheel
    pub fn merge_from_arrays<'a>(&mut self, arrays: &[PartialArray<'a, A>]) {
        for array in arrays {
            self.merge_from_array(array)
        }
    }

    /// Returns a reference to roll-up slots
    pub fn slots(&self) -> &MutablePartialArray<A> {
        &self.slots
    }

    /// Merge two AggregationWheels of similar granularity
    ///
    /// NOTE: must ensure wheels have been advanced to the same time
    #[allow(clippy::useless_conversion)]
    #[inline]
    pub fn merge(&mut self, other: &Self) {
        // merge current total
        if let Some(other_total) = other.total {
            combine_or_insert::<A>(&mut self.total, other_total)
        }

        self.slots.merge(&other.slots);
        // self.drill_down_slots.merge(&other.drill_down_slots);
        // TODO: merge drill-down also
    }

    /// Fast skip `capacity - 1` and prepare wheel for a full rotation
    ///
    /// Note that This function clears all existing wheel slots
    pub fn fast_skip_tick(&mut self) {
        // todo!("Fix");
    }

    /// Tick the wheel by 1 slot
    #[inline]
    pub fn tick(&mut self) -> Option<WheelSlot<A>> {
        // bump internal low watermark
        self.watermark += self.tick_size_ms;

        // Possibly update the partial aggregate for the current rotation
        if let Some(curr) = self.slots.get(0) {
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

            if self.drill_down {
                let drill_down_slots = self.slots.range_to_vec(0..self.capacity);

                Some(WheelSlot::new(total, Some(drill_down_slots)))
            } else {
                Some(WheelSlot::new(total, None))
            }
        } else {
            None
        }
    }

    /// Check whether this wheel is utilising all its slots
    pub fn is_full(&self) -> bool {
        let len = self.slots.len();
        // + 1 as we want to maintain num_slots of history at all times
        len == self.capacity + 1
    }

    /// Returns a back-to-front iterator of regular wheel slots
    pub fn iter(&self) -> impl Iterator<Item = &A::PartialAggregate> {
        self.slots.iter()
    }

    #[cfg(feature = "profiler")]
    /// Returns a reference to the stats of the [AggregationWheel]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Turn wheel into bytes
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        // insert header
        // Header (5b MAGIC NUMBER) + (1b VERSION)
        bytes.extend_from_slice(&MAGIC_NUMBER);
        bytes.push(VERSION);

        // wheel metadata
        // fn get_type_name<T>() -> &'static str {
        //     let type_str = core::any::type_name::<T>();
        //     let parts: Vec<&str> = type_str.split("::").collect();
        //     parts.last().copied().unwrap_or(type_str)
        // }
        // let aggregator_id = get_type_name::<A>();
        // dbg!(aggregator_id);

        let watermark = self.watermark.to_le_bytes();
        let rotation_count = (self.rotation_count as u32).to_le_bytes();
        let capacity = (self.capacity as u32).to_le_bytes();
        let partial_size = (core::mem::size_of::<A::PartialAggregate>() as u32).to_le_bytes();
        let drill_down = self.drill_down as u8;
        let tick_size_ms = (self.tick_size_ms as u32).to_le_bytes();

        // metadata
        bytes.extend_from_slice(&watermark);
        bytes.extend_from_slice(&rotation_count);
        bytes.extend_from_slice(&capacity);
        bytes.extend_from_slice(&partial_size);
        bytes.extend_from_slice(&tick_size_ms);
        bytes.push(drill_down);
        // bytes.extend_from_slice(&self.retention.to_bytes());
        // bytes.extend_from_slice(&self.compression.to_bytes());

        // Chunk for roll-up slots

        let (data, is_compressed) = self.slots.serialize_with_policy(self.compression);
        let data_size = (data.len() as u32).to_le_bytes();

        bytes.push(is_compressed as u8); // is_compressed (1b)
        bytes.extend_from_slice(&(self.slots.len() as u32).to_le_bytes()); // slots (4b)
        bytes.extend_from_slice(&data_size); // data_size (4b)
        bytes.extend_from_slice(&data); // data (Nb)

        // Chunk for optional drill-down slots
        // NOTE: todo

        bytes
    }

    /// Convert bytes to a wheel
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        // Check if there are enough bytes to read the header
        if bytes.len() < 6 {
            return None;
        }

        // Read the header
        let magic_number = &bytes[0..MAGIC_NUMBER.len()];
        let version = u8::from_le_bytes(bytes[5..6].try_into().unwrap());
        // dbg!((magic_number, version));

        // Check if the magic number and version match your expectations
        if magic_number != MAGIC_NUMBER || version != VERSION {
            return None;
        }
        let mut offset = 6;
        let watermark = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;
        // let watermark = u64::from_le_bytes(bytes[offset..offset].try_into().unwrap());
        // dbg!(watermark);

        let rotation_count = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        offset += 4;

        let capacity = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        offset += 4;

        let _partial_size = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        offset += 4;

        let tick_size_ms = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        offset += 4;

        let drill_down = u8::from_le_bytes([bytes[offset]]);

        offset += 1;

        // Chunk
        let is_compressed = u8::from_le_bytes([bytes[offset]]);

        offset += 1;

        let _slots = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        offset += 4;

        let data_size = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        offset += 4;

        // dbg!((
        //     watermark,
        //     capacity,
        //     partial_size,
        //     rotation_count,
        //     tick_size_ms,
        //     drill_down,
        //     is_compressed,
        //     slots,
        //     data_size
        // ));

        let slots = {
            if is_compressed == 1 {
                // decompress
                let slots: MutablePartialArray<A> = MutablePartialArray::try_decompress(
                    &bytes[offset..offset + data_size as usize],
                );
                slots
            } else {
                let slots: MutablePartialArray<A> =
                    MutablePartialArray::from_bytes(&bytes[offset..offset + data_size as usize]);
                slots
            }
        };

        Some(Self {
            capacity: capacity as usize,
            num_slots: capacity as usize,
            rotation_count: rotation_count as usize,
            watermark,
            slots,
            tick_size_ms: tick_size_ms as u64,
            total: None,
            compression: Default::default(),
            retention: Default::default(),
            drill_down: drill_down != 0,
            drill_down_slots: Default::default(),
            table: Default::default(),
            #[cfg(test)]
            total_ticks: 0,
        })

        // Self {
        //     capacity,
        //     num_slots: 0,
        //     slots,
        //     watermark,
        //     tick_size_ms,
        //     drill_down,
        //     drill_down_slots: Default::default(),

        // }
    }
}

impl<A: Aggregator> WheelExt for AggregationWheel<A> {
    fn num_slots(&self) -> usize {
        self.num_slots
    }
    fn capacity(&self) -> usize {
        self.capacity
    }
    fn head(&self) -> usize {
        panic!("not supported")
    }
    fn tail(&self) -> usize {
        panic!("not supported")
    }

    fn size_bytes(&self) -> Option<usize> {
        // TODO: calculate drill down slots

        // roll-up slots are stored on the heap, calculate how much bytes we are using for them..
        let inner_slots = mem::size_of::<Option<A::PartialAggregate>>() * self.num_slots;

        Some(mem::size_of::<Self>() + inner_slots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        aggregator::sum::U64SumAggregator,
        rw_wheel::read::{aggregation::array::PartialArray, hierarchical::HOUR_TICK_MS},
    };

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
    fn mutable_partial_array_to_ref() {
        let mut array = MutablePartialArray::<U64SumAggregator>::default();

        for i in 0..100 {
            array.push_front(i);
        }
        let bytes = array.as_bytes();

        let partial_arr: PartialArray<'_, U64SumAggregator> = PartialArray::from_bytes(&bytes);
        assert_eq!(partial_arr.combine_range(..), Some(4950));
    }

    #[test]
    fn agg_wheel_merge_test() {
        let conf = WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
        let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);
        let mut wheel_two = AggregationWheel::<U64SumAggregator>::new(conf);

        for i in 0..1000 {
            wheel.insert_slot(WheelSlot::with_total(Some(i)));
            wheel.tick();
            wheel_two.insert_slot(WheelSlot::with_total(Some(i)));
            wheel_two.tick();
        }

        assert_eq!(wheel.combine_range(..), Some(499500));
        assert_eq!(wheel.combine_range(..2), Some(1997));

        // merge the second wheel into wheel one and confirm results
        wheel.merge(&wheel_two);

        assert_eq!(wheel.combine_range(..), Some(499500 * 2));
        assert_eq!(wheel.combine_range(..2), Some(1997 * 2));
    }

    #[test]
    fn agg_wheel_encode_decode_test() {
        let conf = WheelConf::new(HOUR_TICK_MS, 24)
            .with_retention_policy(RetentionPolicy::Keep)
            .with_compression_policy(conf::CompressionPolicy::Always);
        let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);

        for i in 0..1000 {
            wheel.insert_slot(WheelSlot::with_total(Some(i)));
            wheel.tick();
        }
        assert_eq!(wheel.combine_range(..), Some(499500));
        let bytes = wheel.as_bytes();
        let decoded: Option<AggregationWheel<U64SumAggregator>> =
            AggregationWheel::from_bytes(&bytes);
        assert_eq!(decoded.is_some(), true);
        assert_eq!(decoded.unwrap().combine_range(..), Some(499500));

        let slots = wheel.slots.as_bytes().to_vec();
        let partial: PartialArray<'_, U64SumAggregator> = PartialArray::from_bytes(&slots);
        wheel.merge_from_ref(partial);
        assert_eq!(wheel.combine_range(..), Some(499500 * 2));
    }

    //     #[test]
    //     fn overflow_keep_test() {
    //         let conf = WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
    //         let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);

    //         for i in 0..60 {
    //             wheel.insert_slot(WheelSlot::with_total(Some(i)));
    //             wheel.tick();
    //         }
    //         wheel.slotz.len()
    //         assert_eq!(wheel.len(), 24);
    //         assert_eq!(wheel.table().len(), 60);
    //     }
    //     #[test]
    //     fn overflow_keep_with_limit_test() {
    //         let conf = WheelConf::new(HOUR_TICK_MS, 24)
    //             .with_retention_policy(RetentionPolicy::KeepWithLimit(10));
    //         let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);

    //         for i in 0..60 {
    //             wheel.insert_slot(WheelSlot::with_total(Some(i)));
    //             wheel.tick();
    //         }
    //         assert_eq!(wheel.len(), 24);
    //         assert_eq!(wheel.table().len(), 10);
    //     }

    //     #[test]
    //     fn downsample_test() {
    //         let seconds_conf = WheelConf::new(SECOND_TICK_MS, 60)
    //             .with_retention_policy(RetentionPolicy::Keep)
    //             .with_drill_down(true);
    //         let minutes_conf = WheelConf::new(MINUTE_TICK_MS, 60)
    //             .with_retention_policy(RetentionPolicy::Keep)
    //             .with_drill_down(true);

    //         let haw_conf = HawConf::default()
    //             .with_seconds(seconds_conf)
    //             .with_minutes(minutes_conf);

    //         let options = Options::default().with_haw_conf(haw_conf);
    //         let mut wheel = RwWheel::<U64SumAggregator>::with_options(0, options);

    //         for _i in 0..10800 {
    //             // 3 hours
    //             wheel.insert(Entry::new(1, wheel.watermark()));
    //             use crate::time::NumericalDuration;
    //             wheel.advance(1.seconds());
    //         }

    //         let result = wheel
    //             .read()
    //             .as_ref()
    //             .minutes_unchecked()
    //             .table()
    //             .downsample(0..2, 0..8);
    //         assert_eq!(result, vec![Some(8), Some(8)]);
    //     }
}
