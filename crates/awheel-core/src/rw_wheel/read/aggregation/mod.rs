use crate::aggregator::Aggregator;
#[cfg(feature = "std")]
use std::collections::VecDeque;

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, collections::VecDeque, vec::Vec};
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
pub(crate) mod stats;

/// Configuration for [AggregationWheel]
pub mod conf;
/// Iterator implementations for [AggregationWheel]
pub mod iter;
/// A maybe initialized [AggregationWheel]
pub mod maybe;

use iter::Iter;
#[cfg(feature = "profiler")]
use stats::Stats;

use crate::rw_wheel::WheelExt;

use self::conf::{RetentionPolicy, WheelConf};

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
    pub total: Option<A::PartialAggregate>,
    /// An array of partial aggregate slots
    ///
    /// The combined aggregate of these slots equal to the `total` field
    pub drill_down_slots: Option<Vec<A::PartialAggregate>>,
}
impl<A: Aggregator> WheelSlot<A> {
    fn new(
        total: Option<A::PartialAggregate>,
        drill_down_slots: Option<Vec<A::PartialAggregate>>,
    ) -> Self {
        Self {
            total,
            drill_down_slots,
        }
    }
    #[cfg(test)]
    fn with_total(total: Option<A::PartialAggregate>) -> Self {
        Self {
            total,
            drill_down_slots: None,
        }
    }
}

#[cfg(not(feature = "std"))]
use alloc::collections::vec_deque;
#[cfg(feature = "std")]
use std::collections::vec_deque;

/// A helper alias for an iterator over the Table
type TableIter<'a, A> = vec_deque::Iter<'a, A>;

/// A Table maintaining wheel slots which are event-time indexed
#[allow(missing_docs)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone, Default, Debug)]
pub struct Table<A: Aggregator> {
    inner: VecDeque<WheelSlot<A>>,
}

impl<A: Aggregator> Table<A> {
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
                let mut accumulator: Option<A::PartialAggregate> = None;
                let Range { start, end } = into_range(&inner, slots.len());

                for partial in slots.iter().skip(start).take(end) {
                    combine_or_insert::<A>(&mut accumulator, *partial);
                }
                result.push(accumulator);
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
            if let Some(partial) = slot.total {
                if filter(&partial) {
                    combine_or_insert::<A>(&mut accumulator, partial);
                }
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
    /// Slots for Partial Aggregates
    pub(crate) slots: Box<[Option<A::PartialAggregate>]>,
    /// Partial aggregate for a full rotation
    total: Option<A::PartialAggregate>,
    /// Configuration for this wheel
    conf: WheelConf,
    /// Table that maintains wheel slots
    table: Table<A>,
    /// Keeps track whether we have done a full rotation (rotation_count == num_slots)
    rotation_count: usize,
    /// Tracks the head (write slot)
    ///
    /// Time goes from tail to head
    head: usize,
    /// Tracks the tail of the circular buffer
    ///
    /// Represents the oldest in time slot
    tail: usize,
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
            slots: Self::init_slots(num_slots),
            total: None,
            table: Default::default(),
            conf,
            rotation_count: 0,
            head: 0,
            tail: 0,
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

    /// Combines partial aggregates of the last `subtrahend` slots
    ///
    /// - If given a interval, returns the combined partial aggregate based on that interval,
    ///   or `None` if out of bounds
    #[inline]
    pub fn interval(&self, subtrahend: usize) -> Option<A::PartialAggregate> {
        if subtrahend > self.len() {
            None
        } else {
            let tail = self.slot_idx_backward_from_head(subtrahend);
            Iter::<A>::new(&self.slots, tail, self.head).combine()
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
    pub fn at(&self, subtrahend: usize) -> Option<A::PartialAggregate> {
        if subtrahend > self.len() {
            None
        } else {
            let index = self.slot_idx_backward_from_head(subtrahend);
            Some(self.slots[index].unwrap_or_default())
        }
    }

    /// Lowers partial aggregate from `subtrahend` slots backwards from the head
    ///
    /// - If given a position, returns the final aggregate based on that position,
    ///   or `None` if out of bounds
    /// - If `0` is specified, it will lower the current head.
    #[inline]
    pub fn lower_at(&self, subtrahend: usize) -> Option<A::Aggregate> {
        self.at(subtrahend).map(|res| A::lower(res))
    }

    /// Returns an interval of drill down slots from `subtrahend` slots backwards from the head
    ///
    ///
    /// - If given a position, returns the drill down slots based on that position,
    ///   or `None` if out of bounds
    /// - If `0` is specified, it will drill down the current head.
    ///
    /// # Panics
    ///
    /// Panics if the wheel has not been configured with drill-down.
    pub fn drill_down_interval(&self, subtrahend: usize) -> Option<Vec<A::PartialAggregate>> {
        if subtrahend > self.len() {
            None
        } else {
            unimplemented!();
            // let iter = self.iter().take(subtrahend);
            // let tail = self.slot_idx_backward_from_head(subtrahend);
            // let iter: DrillIter<A> =
            //     DrillIter::new(self.drill_down_slots.as_ref().unwrap(), tail, self.head);
            // Some(self.combine_drill_down_slots(iter))
        }
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

    /// Drill downs a range of wheel slots and combines their aggregates
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    pub fn combine_drill_down_range<R>(&self, _range: R) -> Vec<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        // self.combine_drill_down_slots(self.drill_down_range(range))
        unimplemented!();
    }

    /// Combines partial aggregates within the given range into a new partial aggregate
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    pub fn combine_range<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        self.range(range).combine()
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
    /// Returns an iterator going from tail to head in the given range
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    pub fn range<R>(&self, range: R) -> Iter<'_, A>
    where
        R: RangeBounds<usize>,
    {
        let (tail, head) = self.range_tail_head(range);
        Iter::new(&self.slots, tail, head)
    }

    /// Returns an iterator going from tail to head in the given range
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    ///
    /// Panics if the wheel has not been configured with drill-down or
    /// if drill down slots has yet been allocated.
    pub fn drill_down_range<R>(&self, _range: R) -> Vec<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        // let (tail, head) = self.range_tail_head(range);
        // DrillIter::new(self.drill_down_slots.as_ref().unwrap(), tail, head)
        unimplemented!();
    }

    /// Shift the tail and clear any old entry
    #[inline]
    fn clear_tail(&mut self) {
        if !self.is_empty() {
            // update new tail
            let tail = self.tail;
            self.tail = self.wrap_add(self.tail, 1);

            // take the partial out
            let _partial = self.slots[tail].take();
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

    /// Clears the wheel
    pub fn clear(&mut self) {
        let mut new = Self::new(self.conf);
        core::mem::swap(self, &mut new);
    }

    /// Returns the partial aggregate for the current rotation
    #[inline]
    pub fn total(&self) -> Option<A::PartialAggregate> {
        #[cfg(feature = "profiler")]
        self.stats.bump_total();

        self.total
    }
    /// Returns a reference to the underyling roll-up slots
    pub fn slots(&self) -> &[Option<A::PartialAggregate>] {
        &self.slots
    }

    /// Insert PartialAggregate into the head of the wheel
    #[inline]
    pub fn insert_head(&mut self, entry: A::PartialAggregate) {
        combine_or_insert::<A>(self.slot(self.head), entry);
    }

    #[inline]
    pub(crate) fn insert_slot(&mut self, mut slot: WheelSlot<A>) {
        // update roll-up aggregates
        if let Some(partial_agg) = slot.total {
            self.insert_head(partial_agg);
        }
        // if this wheel is configured to store data then update accordingly.
        if self.conf.retention.should_keep() {
            // If this wheel should not maintain drill-down slots then make sure it is cleared.
            if !self.conf.drill_down {
                slot.drill_down_slots = None;
            }

            // push slot into the table
            self.table.push_front(slot);

            // if there is a configured limit then check it
            if let RetentionPolicy::KeepWithLimit(limit) = self.conf.retention {
                if self.table.len() > limit {
                    self.table.pop_back();
                }
            }
        }
    }

    /// Merge two AggregationWheels of similar granularity
    ///
    /// NOTE: must ensure wheels have been advanced to the same time
    #[allow(clippy::useless_conversion)]
    pub(crate) fn merge(&mut self, other: &Self) {
        // merge current total
        if let Some(other_total) = other.total {
            combine_or_insert::<A>(&mut self.total, other_total)
        }

        // Merge regular wheel slots
        // NOTE: this assumes both wheels where started with same watermark
        for (self_slot, other_slot) in self.slots.iter_mut().zip(other.slots.iter()) {
            if let Some(other_agg) = other_slot {
                combine_or_insert::<A>(self_slot, *other_agg);
            }
        }
        // match (&mut self.drill_down_slots, &other.drill_down_slots) {
        //     (Some(slots), Some(other_slots)) => {
        //         for (mut self_slot, other_slot) in slots.iter_mut().zip(other_slots.clone().iter())
        //         {
        //             match (&mut self_slot, other_slot) {
        //                 // if both wheels contains drill down slots, then decode, combine and encode into self
        //                 (Some(self_encodes), Some(other_encodes)) => {
        //                     let mut new_aggs = Vec::new();

        //                     for (x, y) in self_encodes.iter_mut().zip(other_encodes) {
        //                         new_aggs.push(A::combine(*x, *y));
        //                     }
        //                     *self_slot = Some(new_aggs);
        //                 }
        //                 // if other wheel but not self, just move to self
        //                 (None, Some(other_encodes)) => {
        //                     *self_slot = Some(other_encodes.to_vec());
        //                 }
        //                 _ => {
        //                     // do nothing
        //                 }
        //             }
        //         }
        //     }
        //     (Some(_), None) => panic!("only lhs wheel was configured with drill-down"),
        //     (None, Some(_)) => panic!("only rhs wheel was configured with drill-down"),
        //     _ => (),
        // }
    }

    #[inline]
    fn slot(&mut self, idx: usize) -> &mut Option<A::PartialAggregate> {
        &mut self.slots[idx]
    }

    /// Fast skip `capacity - 1` and prepare wheel for a full rotation
    ///
    /// Note that This function clears all existing wheel slots
    pub fn fast_skip_tick(&mut self) {
        let skips = self.capacity - 1;

        // reset internal state
        for slot in self.slots.iter_mut() {
            *slot = None;
        }
        self.total = None;
        self.head = 0;
        self.tail = 0;

        // prepare fast tick
        self.head = self.wrap_add(self.head, skips);
        self.rotation_count = skips;
    }

    /// Tick the wheel by 1 slot
    #[inline]
    pub fn tick(&mut self) -> Option<WheelSlot<A>> {
        // Possibly update the partial aggregate for the current rotation
        if let Some(curr) = &self.slots[self.head] {
            combine_or_insert::<A>(&mut self.total, *curr);
        }

        // If the wheel is full, we clear the oldest entry
        if self.is_full() {
            self.clear_tail();
        }

        // shift head of slots
        self.head = self.wrap_add(self.head, 1);

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

            if self.conf.drill_down {
                let drill_down_slots = self
                    .range(..)
                    .copied()
                    .map(|m| m.unwrap_or_default())
                    .collect();

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
        let len = self.len();
        // + 1 as we want to maintain num_slots of history at all times
        (self.capacity + 1) - len == 1
    }

    /// Returns a back-to-front iterator of regular wheel slots
    pub fn iter(&self) -> Iter<'_, A> {
        Iter::new(&self.slots, self.tail, self.head)
    }

    // Taken from Rust std
    fn range_tail_head<R>(&self, range: R) -> (usize, usize)
    where
        R: RangeBounds<usize>,
    {
        let Range { start, end } = into_range(&range, self.len());
        let tail = self.wrap_add(self.tail, start);
        let head = self.wrap_add(self.tail, end);
        (tail, head)
    }

    // helper functions
    fn init_slots(slots: usize) -> Box<[Option<A::PartialAggregate>]> {
        (0..slots)
            .map(|_| None)
            .collect::<Vec<_>>()
            .into_boxed_slice()
    }
    #[cfg(feature = "profiler")]
    /// Returns a reference to the stats of the [AggregationWheel]
    pub fn stats(&self) -> &Stats {
        &self.stats
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
        self.head
    }
    fn tail(&self) -> usize {
        self.tail
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
    use crate::{
        aggregator::sum::U64SumAggregator,
        rw_wheel::read::hierarchical::HawConf,
        Entry,
        Options,
        RwWheel,
    };

    use super::*;

    #[test]
    fn range_query_hours_test() {
        let conf = WheelConf::new(24).with_retention_policy(RetentionPolicy::Keep);
        let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);

        for i in 0..30 {
            wheel.insert_slot(WheelSlot::with_total(Some(i)));
            wheel.tick();
        }
        // with a 24 hour wheel, 6 slots should have been moved over to the overflow list
        assert_eq!(wheel.len(), 24);
        assert_eq!(wheel.table().len(), 30);

        let table = wheel.table();

        assert_eq!(table.range_query(0..5), Some(135));
        assert_eq!(table.range_query(..), Some(435));

        assert_eq!(table.range_query(2..=4), Some(78)); // 27 + 26 + 25
        assert_eq!(table.range_query(3..5), Some(51));

        // slots: 1,2,3,4,5
        // filter out hours where sum is below 3:
        // filters out 1,2,3 and returns 4+5
        assert_eq!(
            table.range_query_with_filter(2..=4, |agg| *agg > 25),
            Some(27 + 26)
        );
    }

    #[test]
    fn overflow_keep_test() {
        let conf = WheelConf::new(24).with_retention_policy(RetentionPolicy::Keep);
        let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);

        for i in 0..60 {
            wheel.insert_slot(WheelSlot::with_total(Some(i)));
            wheel.tick();
        }
        assert_eq!(wheel.len(), 24);
        assert_eq!(wheel.table().len(), 60);
    }
    #[test]
    fn overflow_keep_with_limit_test() {
        let conf = WheelConf::new(24).with_retention_policy(RetentionPolicy::KeepWithLimit(10));
        let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);

        for i in 0..60 {
            wheel.insert_slot(WheelSlot::with_total(Some(i)));
            wheel.tick();
        }
        assert_eq!(wheel.len(), 24);
        assert_eq!(wheel.table().len(), 10);
    }

    #[test]
    fn downsample_test() {
        let seconds_conf = WheelConf::new(60)
            .with_retention_policy(RetentionPolicy::Keep)
            .with_drill_down(true);
        let minutes_conf = WheelConf::new(60)
            .with_retention_policy(RetentionPolicy::Keep)
            .with_drill_down(true);

        let haw_conf = HawConf::default()
            .with_seconds(seconds_conf)
            .with_minutes(minutes_conf);

        let options = Options::default().with_haw_conf(haw_conf);
        let mut wheel = RwWheel::<U64SumAggregator>::with_options(0, options);

        for _i in 0..10800 {
            // 3 hours
            wheel.insert(Entry::new(1, wheel.watermark()));
            use crate::time::NumericalDuration;
            wheel.advance(1.seconds());
        }

        let result = wheel
            .read()
            .as_ref()
            .minutes_unchecked()
            .table()
            .downsample(0..2, 0..8);
        assert_eq!(result, vec![Some(8), Some(8)]);
    }
}
