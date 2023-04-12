use crate::aggregator::Aggregator;
use core::{
    assert,
    debug_assert,
    fmt::Debug,
    ops::{Range, RangeBounds},
    option::{
        Option,
        Option::{None, Some},
    },
    slice,
};

#[cfg(all(feature = "alloc", not(feature = "std")))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "rkyv")]
use rkyv::{
    ser::serializers::{
        AlignedSerializer,
        AllocScratch,
        AllocSerializer,
        CompositeSerializer,
        FallbackScratch,
        HeapScratch,
        SharedSerializeMap,
    },
    ser::Serializer,
    AlignedVec,
    Archive,
    Deserialize,
    Infallible,
    Serialize,
};

mod iter;

use iter::Iter;

cfg_drill_down! {
    use crate::agg_wheel::iter::DrillIter;

    /// Type alias for drill down slots
    type DrillDownSlots<A, const CAP: usize> = Option<Box<[Option<Vec<A>>; CAP]>>;

    pub struct DrillCut<R>
    where
        R: RangeBounds<usize>,
    {
        /// slot ``subtrahend`` from head
        pub slot: usize,
        /// Range of partial aggregates within the drill down slots
        pub range: R,
    }

    // utility function for drill down cutting
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
        start..end
    }
}

/// Struct holding data for a complete wheel rotation
pub struct RotationData<A: Aggregator> {
    pub total: Option<A::PartialAggregate>,
    #[cfg(feature = "drill_down")]
    pub drill_down_slots: Option<Vec<A::PartialAggregate>>,
}
impl<A: Aggregator> RotationData<A> {
    pub fn new(
        total: Option<A::PartialAggregate>,
        #[cfg(feature = "drill_down")] drill_down_slots: Option<Vec<A::PartialAggregate>>,
    ) -> Self {
        Self {
            total,
            #[cfg(feature = "drill_down")]
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
    #[cfg(feature = "drill_down")]
    drill_down_slots: DrillDownSlots<A::PartialAggregate, CAP>,
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
    #[cfg(test)]
    pub(crate) total_ticks: usize,
}
impl<const CAP: usize, A: Aggregator> AggregationWheel<CAP, A> {
    const INIT_VALUE: Option<A::PartialAggregate> = None;

    cfg_drill_down! {
        const INIT_DRILL_DOWN_VALUE: Option<Vec<A::PartialAggregate>> = None;

        /// Creates a new AggregationWheel with drill-down enabled
        pub fn with_drill_down(num_slots: usize) -> Self {
            let drill_down_slots: [Option<Vec<A::PartialAggregate>>; CAP] =
                [Self::INIT_DRILL_DOWN_VALUE; CAP];

            let mut agg_wheel = Self::new(num_slots);
            agg_wheel.drill_down_slots = Some(Box::new(drill_down_slots));
            agg_wheel
        }
    }

    /// Creates a new AggregationWheel using `num_slots`
    pub fn new(num_slots: usize) -> Self {
        assert!(CAP != 0, "Capacity is not allowed to be zero");
        assert!(CAP.is_power_of_two(), "Capacity must be a power of two");

        let slots: [Option<A::PartialAggregate>; CAP] = [Self::INIT_VALUE; CAP];

        Self {
            num_slots,
            slots,
            #[cfg(feature = "drill_down")]
            drill_down_slots: None,
            total: None,
            rotation_count: 0,
            head: 0,
            tail: 0,
            #[cfg(test)]
            total_ticks: 0,
        }
    }

    /// Returns `true` if the wheel is empty or `false` if it contains slots
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail == self.head
    }

    /// Returns drill down slots from `slot` slots backwards from the head
    ///
    /// If `0` is specified, it will drill down the current head.
    #[cfg(feature = "drill_down")]
    #[inline]
    pub fn drill_down(&self, slot: usize) -> Option<&[A::PartialAggregate]> {
        debug_assert!(slot <= self.num_slots);
        let index = self.slot_idx_from_head(slot);
        self.drill_down_slots
            .as_ref()
            .and_then(|slots| slots[index].as_deref())
    }

    /// Drill down and cut across 2 slots
    ///
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    #[cfg(feature = "drill_down")]
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

    /// Drill downs a range of wheel slots and their combines aggregates
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    #[cfg(feature = "drill_down")]
    pub fn drill_down_range<R>(&self, range: R) -> Option<Vec<A::PartialAggregate>>
    where
        R: RangeBounds<usize>,
    {
        // TODO: range should go from head to tail (check combine_up_to_head)
        let (tail, head) = self.range_tail_head(range);
        let binding = self.drill_down_slots.as_ref().unwrap();
        let drill_iter = DrillIter::<CAP, A>::new(binding, tail, head);
        let mut res = Vec::new();
        let aggregator = A::default();
        for slot in drill_iter.flatten() {
            if res.is_empty() {
                res.extend_from_slice(slot);
            } else {
                for (curr, other) in res.iter_mut().zip(slot) {
                    *curr = aggregator.combine(*curr, *other);
                }
            }
        }
        Some(res)
    }

    /// Lowers partial aggregate from `slot` slots backwards from the head
    ///
    /// If `0` is specified, it will lower the current head.
    #[inline]
    pub fn lower(&self, slot: usize, aggregator: &A) -> Option<A::Aggregate> {
        debug_assert!(slot <= self.num_slots);
        let index = self.slot_idx_from_head(slot);
        self.slots[index].map(|res| aggregator.lower(res))
    }

    /// Combines partial aggregates from `subtrahend` slots back up to the head.
    pub fn combine_up_to_head(
        &self,
        subtrahend: usize,
        aggregator: &A,
    ) -> Option<A::PartialAggregate> {
        let tail = self.slot_idx_from_head(subtrahend);
        let iter: Iter<CAP, A> = Iter::new(&self.slots, tail, self.head);
        let mut res: Option<A::PartialAggregate> = None;
        for slot in iter.flatten() {
            Self::insert(&mut res, *slot, aggregator);
        }
        res
    }

    /// Combines partial aggregates within the given range into a new partial aggregate
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    pub fn combine_range<R>(&self, range: R, aggregator: &A) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        let mut res: Option<A::PartialAggregate> = None;
        for slot in self.range(range).flatten() {
            Self::insert(&mut res, *slot, aggregator);
        }
        res
    }
    /// Combines partial aggregates from the specified range and lowers it to a final aggregate value
    ///
    /// # Panics
    ///
    /// Panics if the starting point is greater than the end point or if
    /// the end point is greater than the length of the wheel.
    pub fn combine_and_lower_range<R>(&self, range: R, aggregator: &A) -> Option<A::Aggregate>
    where
        R: RangeBounds<usize>,
    {
        self.combine_range(range, aggregator)
            .map(|res| aggregator.lower(res))
    }
    /// Returns an iterator going from tail to head in the given range
    fn range<R>(&self, range: R) -> Iter<'_, CAP, A>
    where
        R: RangeBounds<usize>,
    {
        let (tail, head) = self.range_tail_head(range);
        Iter::new(&self.slots, tail, head)
    }

    /// Shift the tail and clear any old entry
    #[inline]
    fn clear_tail(&mut self) {
        if !self.is_empty() {
            let tail = self.tail;
            self.tail = self.wrap_add(self.tail, 1);
            self.slots[tail] = None;
            #[cfg(feature = "drill_down")]
            {
                if let Some(ref mut drill_down_slots) = &mut self.drill_down_slots {
                    drill_down_slots[tail] = None;
                }
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

    /// Clears the wheel
    pub fn clear(&mut self) {
        let mut new = Self::new(self.num_slots);
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
    #[cfg(feature = "drill_down")]
    fn insert_drill_down_slots(&mut self, encoded_slots_opt: Option<Vec<A::PartialAggregate>>) {
        if let Some(ref mut drill_down_slots) = &mut self.drill_down_slots {
            drill_down_slots[self.head] = encoded_slots_opt;
        }
    }

    /// Combine into the curent head of the circular buffer
    #[inline]
    fn insert_at(&mut self, slot_idx: usize, entry: A::PartialAggregate, aggregator: &A) {
        Self::insert(self.slot(slot_idx), entry, aggregator);
    }

    /// Combine into the curent head of the circular buffer
    #[inline]
    fn insert_head(&mut self, entry: A::PartialAggregate, aggregator: &A) {
        Self::insert(self.slot(self.head), entry, aggregator);
    }

    /// Combine partial aggregates or insert new entry
    #[inline]
    fn insert(slot: &mut Option<A::PartialAggregate>, entry: A::PartialAggregate, aggregator: &A) {
        match slot {
            Some(curr) => {
                let new_curr = aggregator.combine(*curr, entry);
                *curr = new_curr;
            }
            None => {
                *slot = Some(entry);
            }
        }
    }

    #[inline]
    pub(super) fn insert_rotation_data(&mut self, data: RotationData<A>, aggregator: &A) {
        if let Some(partial_agg) = data.total {
            self.insert_head(partial_agg, aggregator);
        }
        #[cfg(feature = "drill_down")]
        self.insert_drill_down_slots(data.drill_down_slots);
    }

    /// Merge two AggregationWheels of similar granularity
    ///
    /// NOTE: must ensure wheels have been advanced to the same time
    pub(crate) fn merge(&mut self, other: &Self, aggregator: &A) {
        // merge current total
        if let Some(other_total) = other.total {
            Self::insert(&mut self.total, other_total, aggregator)
        }

        // Merge regular wheel slots
        for (self_slot, other_slot) in self.slots.iter_mut().zip(other.slots) {
            if let Some(other_agg) = other_slot {
                Self::insert(self_slot, other_agg, aggregator);
            }
        }
        #[cfg(feature = "drill_down")]
        {
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
                                    new_aggs.push(aggregator.combine(*x, y));
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
    }

    /// Locate slot id `subtrahend` back
    pub(crate) fn slot_idx_from_head(&self, subtrahend: usize) -> usize {
        self.wrap_sub(self.head, subtrahend)
    }

    /// How many write ahead slots are available
    #[inline]
    pub(crate) fn write_ahead_len(&self) -> usize {
        let diff = self.len();
        CAP - diff
    }

    /// Check whether this wheel can write ahead by ´addend` slots
    pub(crate) fn can_write_ahead(&self, addend: u64) -> bool {
        // Problem: if the wheel is full length and addend slots wraps around the tail, then
        // we will update valid historic slots with future aggregates. We should not allow this.
        addend as usize <= self.write_ahead_len()
    }

    /// Attempts to write `entry` into the Wheel
    #[inline]
    pub fn write_ahead(&mut self, addend: u64, partial_agg: A::PartialAggregate, aggregator: &A) {
        let slot_idx = self.slot_idx_forward_from_head(addend as usize);
        self.insert_at(slot_idx, partial_agg, aggregator);
    }

    /// Locate slot id `addend` forward
    fn slot_idx_forward_from_head(&self, addend: usize) -> usize {
        self.wrap_add(self.head, addend)
    }

    /// Locate slot id `addend` forward
    fn _slot_idx_from_tail(&self, addend: usize) -> usize {
        self.wrap_add(self.tail, addend)
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

        #[cfg(feature = "drill_down")]
        {
            if let Some(drill_down_slots) = &mut self.drill_down_slots {
                let slots: [Option<Vec<A::PartialAggregate>>; CAP] =
                    [Self::INIT_DRILL_DOWN_VALUE; CAP];
                **drill_down_slots = slots;
            }
        }

        // prepare fast tick
        self.head = self.wrap_add(self.head, skips);
        self.rotation_count = skips;
    }

    /// Tick the wheel by 1 slot
    #[inline]
    pub fn tick(&mut self, aggregator: &A) -> Option<RotationData<A>> {
        // Possibly update the partial aggregate for the current rotation
        if let Some(curr) = &self.slots[self.head] {
            Self::insert(&mut self.total, *curr, aggregator);
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

        if self.rotation_count == self.num_slots {
            let total = self.total.take();
            self.rotation_count = 0;

            #[cfg(feature = "drill_down")]
            // drill-down slots of this wheel to be inserted in another wheel
            let drill_down_slots = self
                .range(..)
                .copied()
                .map(|m| m.unwrap_or_default())
                .collect();

            #[cfg(feature = "drill_down")]
            return Some(RotationData::new(total, Some(drill_down_slots)));

            #[cfg(not(feature = "drill_down"))]
            Some(RotationData::new(total))
        } else {
            None
        }
    }

    #[cfg(feature = "rkyv")]
    #[cfg(any(feature = "rkyv", doc))]
    #[doc(cfg(feature = "rkyv"))]
    /// Deserialise given bytes into an AggregationWheel
    pub fn from_bytes(bytes: &[u8]) -> Self
    where
        <<A as Aggregator>::PartialAggregate as Archive>::Archived:
            Deserialize<<A as Aggregator>::PartialAggregate, Infallible>,
    {
        let archived = unsafe { rkyv::archived_root::<Self>(bytes) };
        let wheel: Self = archived.deserialize(&mut Infallible).unwrap();
        wheel
    }

    #[cfg(feature = "rkyv")]
    #[cfg(any(feature = "rkyv", doc))]
    #[doc(cfg(feature = "rkyv"))]
    /// Serialises the AggregationWheel to bytes
    pub fn as_bytes(&self) -> AlignedVec
    where
        <A as Aggregator>::PartialAggregate: Serialize<
            CompositeSerializer<
                AlignedSerializer<AlignedVec>,
                FallbackScratch<HeapScratch<4096>, AllocScratch>,
                SharedSerializeMap,
            >,
        >,
    {
        let mut serializer = AllocSerializer::<4096>::default();
        serializer.serialize_value(self).unwrap();
        serializer.into_serializer().into_inner()
    }

    /// Check whether this wheel is utilising all its slots
    pub fn is_full(&self) -> bool {
        // + 1 as we want to maintain num_slots of history at all times
        (self.num_slots + 1) - self.len() == 1
    }

    // NOTE: Methods below are based on Rust's VecDeque impl

    /// Returns the index in the underlying buffer for a given logical element
    /// index + addend.
    #[inline]
    fn wrap_add(&self, idx: usize, addend: usize) -> usize {
        wrap_index(idx.wrapping_add(addend), CAP)
    }

    /// Returns the index in the underlying buffer for a given logical element
    /// index - subtrahend.
    #[inline]
    fn wrap_sub(&self, idx: usize, subtrahend: usize) -> usize {
        wrap_index(idx.wrapping_sub(subtrahend), CAP)
    }

    /// Returns the current number of used slots (includes empty NONE slots as well)
    pub fn len(&self) -> usize {
        count(self.tail, self.head, CAP)
    }

    // Taken from Rust std
    fn range_tail_head<R>(&self, range: R) -> (usize, usize)
    where
        R: RangeBounds<usize>,
    {
        let Range { start, end } = slice::range(range, ..self.len());
        let tail = self.wrap_add(self.tail, start);
        let head = self.wrap_add(self.tail, end);
        (tail, head)
    }
}

// Functions below are adapted from Rust's VecDeque impl

/// Returns the index in the underlying buffer for a given logical element index.
#[inline]
fn wrap_index(index: usize, size: usize) -> usize {
    // size is always a power of 2
    debug_assert!(size.is_power_of_two());
    index & (size - 1)
}

/// Calculate the number of elements left to be read in the buffer
#[inline]
fn count(tail: usize, head: usize, size: usize) -> usize {
    // size is always a power of 2
    (head.wrapping_sub(tail)) & (size - 1)
}
