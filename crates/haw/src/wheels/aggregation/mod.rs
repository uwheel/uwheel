use crate::aggregator::Aggregator;
use core::{
    assert,
    fmt::Debug,
    ops::{Range, RangeBounds},
    option::{
        Option,
        Option::{None, Some},
    },
};

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "rkyv")]
use rkyv::{ser::Serializer, with::Skip, AlignedVec, Archive, Deserialize, Serialize};

#[cfg(feature = "rkyv")]
use crate::wheels::wheel::{DefaultSerializer, PartialAggregate};

pub(crate) mod iter;
mod maybe;

pub use maybe::MaybeWheel;

use iter::Iter;

use crate::wheels::aggregation::iter::DrillIter;

use super::{len, wrap_add, wrap_sub};

/// Combine partial aggregates or insert new entry
#[inline]
pub fn combine_or_insert<A: Aggregator>(
    dest: &mut Option<A::PartialAggregate>,
    entry: A::PartialAggregate,
    aggregator: &A,
) {
    match dest {
        Some(curr) => {
            let new_curr = aggregator.combine(*curr, entry);
            *curr = new_curr;
        }
        None => {
            *dest = Some(entry);
        }
    }
}

/// Type alias for drill down slots
type DrillDownSlots<A, const CAP: usize> = Option<Box<[Option<Vec<A>>; CAP]>>;

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

/// Struct holding data for a complete wheel rotation
pub struct RotationData<A: Aggregator> {
    /// A possible partial aggregate that is being rolled up into another wheel
    pub total: Option<A::PartialAggregate>,
    /// An array of partial aggregate slots
    ///
    /// The combined aggregate of these slots equal to the `total` field
    pub drill_down_slots: Option<Vec<A::PartialAggregate>>,
}
impl<A: Aggregator> RotationData<A> {
    pub fn new(
        total: Option<A::PartialAggregate>,
        drill_down_slots: Option<Vec<A::PartialAggregate>>,
    ) -> Self {
        Self {
            total,
            drill_down_slots,
        }
    }
}

/// Fixed-size wheel where each slot contains a possible partial aggregate
///
/// The wheel maintains partial aggregates per slot, but also updates a `total` aggregate for each tick in the wheel.
/// The total aggregate is returned once a full rotation occurs. This way the same wheel structure can be used between different hierarchical levels (e.g., seconds, minutes, hours, days)
///
///
/// Const Parameters:
/// * `CAP` defines the circular buffer capacity (power of two).
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Clone, Debug)]
pub struct AggregationWheel<const CAP: usize, A: Aggregator> {
    /// Number of slots (60 seconds => 60 slots)
    ///
    /// Note that CAP is aligned up to power of two due to circular buffer
    num_slots: usize,
    /// Slots for Partial Aggregates
    pub(crate) slots: [Option<A::PartialAggregate>; CAP],
    /// Slots used for drill-down operations
    ///
    /// The slots hold encoded entries from a different granularity.
    /// Example: Drill down slots for a day would hold 24 hour slots
    drill_down_slots: DrillDownSlots<A::PartialAggregate, CAP>,
    /// A flag indicating whether drill down is enabled
    drill_down: bool,
    /// Partial aggregate for a full rotation
    total: Option<A::PartialAggregate>,
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
    #[cfg_attr(feature = "rkyv", with(Skip))]
    aggregator: A,
    #[cfg(test)]
    pub(crate) total_ticks: usize,
}

impl<const CAP: usize, A: Aggregator> AggregationWheel<CAP, A> {
    const INIT_VALUE: Option<A::PartialAggregate> = None;
    const INIT_DRILL_DOWN_VALUE: Option<Vec<A::PartialAggregate>> = None;

    /// Creates a new AggregationWheel with drill-down enabled
    pub fn with_capacity_and_drill_down(num_slots: usize) -> Self {
        let mut agg_wheel = Self::with_capacity(num_slots);
        agg_wheel.drill_down = true;
        agg_wheel
    }

    /// Creates a new AggregationWheel using `num_slots`
    pub fn with_capacity(num_slots: usize) -> Self {
        assert_capacity!(CAP);
        let slots: [Option<A::PartialAggregate>; CAP] = [Self::INIT_VALUE; CAP];

        Self {
            num_slots,
            slots,
            drill_down_slots: None,
            drill_down: false,
            total: None,
            rotation_count: 0,
            head: 0,
            tail: 0,
            aggregator: Default::default(),
            #[cfg(test)]
            total_ticks: 0,
        }
    }

    /// Returns `true` if the wheel is empty or `false` if it contains slots
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail == self.head
    }

    /// Combines partial aggregates of the last `subtrahend` slots
    ///
    /// - If given a interval, returns the combined partial aggregate based on that interval,
    ///   or `None` if out of bounds
    #[inline]
    pub fn interval(&self, subtrahend: usize) -> Option<A::PartialAggregate> {
        let len = len(self.tail, self.head, CAP);
        if subtrahend > len {
            None
        } else {
            let tail = self.slot_idx_from_head(subtrahend);
            Iter::<A>::new(&self.slots, tail, self.head).combine()
        }
    }

    /// This function takes the current rotation into context and if the interval is equal to the rotation count,
    /// then it will return the total for the rotation otherwise call the regular interval function.
    #[inline]
    pub fn interval_or_total(&self, subtrahend: usize) -> Option<A::PartialAggregate> {
        if subtrahend == self.rotation_count {
            self.total
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
            .map(|partial_agg| self.aggregator.lower(partial_agg))
    }

    /// Returns partial aggregate from `subtrahend` slots backwards from the head
    ///
    /// - If given a position, returns the partial aggregate based on that position,
    ///   or `None` if out of bounds
    /// - If `0` is specified, it will return the current head.
    #[inline]
    pub fn at(&self, subtrahend: usize) -> Option<A::PartialAggregate> {
        let len = len(self.tail, self.head, CAP);
        if subtrahend > len {
            None
        } else {
            let index = self.slot_idx_from_head(subtrahend);
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
        self.at(subtrahend).map(|res| self.aggregator.lower(res))
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
        let len = len(self.tail, self.head, CAP);
        if subtrahend > len {
            None
        } else {
            let tail = self.slot_idx_from_head(subtrahend);
            let iter: DrillIter<CAP, A> =
                DrillIter::new(self.drill_down_slots.as_ref().unwrap(), tail, self.head);
            Some(self.combine_drill_down_slots(iter))
        }
    }

    // helper method to combine drill-down N slots into 1 drill-down slot.
    #[inline]
    fn combine_drill_down_slots<'a>(
        &self,
        iter: impl Iterator<Item = Option<&'a [A::PartialAggregate]>>,
    ) -> Vec<A::PartialAggregate> {
        iter.flatten().fold(Vec::new(), |mut res, slot| {
            if res.is_empty() {
                res.extend_from_slice(slot);
            } else {
                for (curr, other) in res.iter_mut().zip(slot) {
                    *curr = self.aggregator.combine(*curr, *other);
                }
            }
            res
        })
    }

    /// Returns drill down slots from `slot` slots backwards from the head
    ///
    ///
    /// - If given a position, returns the drill down slots based on that position,
    ///   or `None` if out of bounds
    /// - If `0` is specified, it will drill down the current head.
    #[inline]
    pub fn drill_down(&self, slot: usize) -> Option<&[A::PartialAggregate]> {
        let len = len(self.tail, self.head, CAP);
        if slot > len {
            None
        } else {
            let index = self.slot_idx_from_head(slot);

            if let Some(slots) = self.drill_down_slots.as_ref() {
                slots[index].as_deref()
            } else {
                None
            }
        }
    }

    /// Returns drill down slots from `slot` slots backwards from the head and lowers the aggregates
    ///
    /// If `0` is specified, it will drill down the current head.
    #[inline]
    pub fn lower_drill_down(&self, slot: usize) -> Option<Vec<A::Aggregate>> {
        self.drill_down(slot).map(|partial_aggs| {
            partial_aggs
                .iter()
                .map(|agg| self.aggregator.lower(*agg))
                .collect()
        })
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
        self.drill_down_cut(a, b).map(|partials| {
            partials
                .into_iter()
                .map(|pa| self.aggregator.lower(pa))
                .collect()
        })
    }

    /// Drill downs a range of wheel slots and combines their aggregates
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    pub fn combine_drill_down_range<R>(&self, range: R) -> Vec<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        self.combine_drill_down_slots(self.drill_down_range(range))
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
        self.combine_range(range)
            .map(|res| self.aggregator.lower(res))
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
    pub fn drill_down_range<R>(&self, range: R) -> DrillIter<'_, CAP, A>
    where
        R: RangeBounds<usize>,
    {
        let (tail, head) = self.range_tail_head(range);
        DrillIter::new(self.drill_down_slots.as_ref().unwrap(), tail, head)
    }

    /// Shift the tail and clear any old entry
    #[inline]
    fn clear_tail(&mut self) {
        if !self.is_empty() {
            let tail = self.tail;
            self.tail = wrap_add(self.tail, 1, CAP);
            self.slots[tail] = None;
            if let Some(ref mut drill_down_slots) = &mut self.drill_down_slots {
                drill_down_slots[tail] = None;
            }
        }
    }
    /// Returns the current rotation position in the wheel
    pub fn rotation_count(&self) -> usize {
        self.rotation_count
    }

    /// Ticks left until the wheel fully rotates
    #[inline]
    pub fn ticks_remaining(&self) -> usize {
        self.num_slots - self.rotation_count
    }
    pub fn len(&self) -> usize {
        len(self.tail, self.head, CAP)
    }

    /// Clears the wheel
    pub fn clear(&mut self) {
        let mut new = if self.drill_down {
            Self::with_capacity_and_drill_down(self.num_slots)
        } else {
            Self::with_capacity(self.num_slots)
        };
        core::mem::swap(self, &mut new);
    }

    /// Returns the partial aggregate for the current rotation
    pub fn total(&self) -> Option<A::PartialAggregate> {
        self.total
    }
    pub fn slots(&self) -> &[Option<A::PartialAggregate>; CAP] {
        &self.slots
    }

    /// Insert encoded drill down slots at the current head
    fn insert_drill_down_slots(&mut self, encoded_slots_opt: Option<Vec<A::PartialAggregate>>) {
        // if drill down slots have not been allocated
        if self.drill_down_slots.is_none() {
            self.drill_down_slots = Some(Box::new([Self::INIT_DRILL_DOWN_VALUE; CAP]));
        }
        // insert drill down slots into the current head
        if let Some(ref mut drill_down_slots) = &mut self.drill_down_slots {
            drill_down_slots[self.head] = encoded_slots_opt;
        }
    }

    /// Insert PartialAggregate into the head of the wheel
    #[inline]
    pub fn insert_head(&mut self, entry: A::PartialAggregate, aggregator: &A) {
        combine_or_insert(self.slot(self.head), entry, aggregator);
    }

    #[inline]
    pub(super) fn insert_rotation_data(&mut self, data: RotationData<A>) {
        if let Some(partial_agg) = data.total {
            self.insert_head(partial_agg, &Default::default());
        }
        if self.drill_down {
            self.insert_drill_down_slots(data.drill_down_slots);
        }
    }

    /// Merge two AggregationWheels of similar granularity
    ///
    /// NOTE: must ensure wheels have been advanced to the same time
    pub(crate) fn merge(&mut self, other: &Self) {
        // merge current total
        if let Some(other_total) = other.total {
            combine_or_insert(&mut self.total, other_total, &self.aggregator)
        }

        // Merge regular wheel slots
        for (self_slot, other_slot) in self.slots.iter_mut().zip(other.slots) {
            if let Some(other_agg) = other_slot {
                combine_or_insert(self_slot, other_agg, &self.aggregator);
            }
        }
        match (&mut self.drill_down_slots, &other.drill_down_slots) {
            (Some(slots), Some(other_slots)) => {
                for (mut self_slot, other_slot) in
                    slots.iter_mut().zip(other_slots.clone().into_iter())
                {
                    match (&mut self_slot, other_slot) {
                        // if both wheels contains drill down slots, then decode, combine and encode into self
                        (Some(self_encodes), Some(other_encodes)) => {
                            let mut new_aggs = Vec::new();

                            for (x, y) in self_encodes.iter_mut().zip(other_encodes) {
                                new_aggs.push(self.aggregator.combine(*x, y));
                            }
                            *self_slot = Some(new_aggs);
                        }
                        // if other wheel but not self, just move to self
                        (None, Some(other_encodes)) => {
                            *self_slot = Some(other_encodes);
                        }
                        _ => {
                            // do nothing
                        }
                    }
                }
            }
            (Some(_), None) => panic!("only lhs wheel was configured with drill-down"),
            (None, Some(_)) => panic!("only rhs wheel was configured with drill-down"),
            _ => (),
        }
    }

    /// Locate slot id `subtrahend` back
    pub(crate) fn slot_idx_from_head(&self, subtrahend: usize) -> usize {
        wrap_sub(self.head, subtrahend, CAP)
    }

    #[inline]
    fn slot(&mut self, idx: usize) -> &mut Option<A::PartialAggregate> {
        &mut self.slots[idx]
    }

    pub fn head(&self) -> usize {
        self.head
    }
    pub fn tail(&self) -> usize {
        self.tail
    }

    /// Fast skip `num_slots - 1` and prepare wheel for a full rotation
    ///
    /// Note that This function clears all existing wheel slots
    pub fn fast_skip_tick(&mut self) {
        let skips = self.num_slots - 1;

        // reset internal state
        let slots: [Option<A::PartialAggregate>; CAP] = [Self::INIT_VALUE; CAP];
        self.slots = slots;
        self.total = None;
        self.head = 0;
        self.tail = 0;

        if let Some(drill_down_slots) = &mut self.drill_down_slots {
            let slots: [Option<Vec<A::PartialAggregate>>; CAP] = [Self::INIT_DRILL_DOWN_VALUE; CAP];
            **drill_down_slots = slots;
        }

        // prepare fast tick
        self.head = wrap_add(self.head, skips, CAP);
        self.rotation_count = skips;
    }

    /// Tick the wheel by 1 slot
    #[inline]
    pub fn tick(&mut self) -> Option<RotationData<A>> {
        // Possibly update the partial aggregate for the current rotation
        if let Some(curr) = &self.slots[self.head] {
            combine_or_insert(&mut self.total, *curr, &self.aggregator);
        }

        // If the wheel is full, we clear the oldest entry
        if self.is_full() {
            self.clear_tail();
        }

        // shift head of slots
        self.head = wrap_add(self.head, 1, CAP);

        self.rotation_count += 1;

        #[cfg(test)]
        {
            self.total_ticks += 1;
        }

        // Return RotationData if wheel has doen a full rotation
        if self.rotation_count == self.num_slots {
            // our total partial aggregate to be rolled up
            let total = self.total.take();

            // reset count
            self.rotation_count = 0;

            if self.drill_down {
                let drill_down_slots = self
                    .range(..)
                    .copied()
                    .map(|m| m.unwrap_or_default())
                    .collect();

                Some(RotationData::new(total, Some(drill_down_slots)))
            } else {
                Some(RotationData::new(total, None))
            }
        } else {
            None
        }
    }

    #[cfg(feature = "rkyv")]
    /// Converts the wheel to bytes
    pub fn as_bytes(&self) -> AlignedVec
    where
        PartialAggregate<A>: Serialize<DefaultSerializer>,
    {
        let mut serializer = DefaultSerializer::default();
        serializer.serialize_value(self).unwrap();
        serializer.into_serializer().into_inner()
    }

    /// Check whether this wheel is utilising all its slots
    pub fn is_full(&self) -> bool {
        let len = self.len();
        // + 1 as we want to maintain num_slots of history at all times
        (self.num_slots + 1) - len == 1
    }

    /// Returns a back-to-front iterator of regular wheel slots
    pub fn iter(&self) -> Iter<'_, A> {
        Iter::new(&self.slots, self.tail, self.head)
    }

    /// Returns a back-to-front iterator of drill-down slots
    ///
    /// # Panics
    ///
    /// Panics if the wheel has not been configured with drill-down capabilities.
    pub fn drill_iter(&self) -> DrillIter<'_, CAP, A> {
        DrillIter::new(
            self.drill_down_slots.as_ref().unwrap(),
            self.tail,
            self.head,
        )
    }

    // Taken from Rust std
    fn range_tail_head<R>(&self, range: R) -> (usize, usize)
    where
        R: RangeBounds<usize>,
    {
        let Range { start, end } = into_range(&range, self.len());
        let tail = wrap_add(self.tail, start, CAP);
        let head = wrap_add(self.tail, end, CAP);
        (tail, head)
    }
}
