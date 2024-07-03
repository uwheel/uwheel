use core::{mem, time::Duration as CoreDuration};

use crate::{aggregator::Aggregator, duration::Duration, Entry};

use super::{timer::RawTimerWheel, wheel_ext::WheelExt};

/// Number of write ahead slots
pub const DEFAULT_WRITE_AHEAD_SLOTS: usize = 64;

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

/// A writer wheel optimized for single-threaded ingestion of aggregates.
///
/// Note that you do not have to interact manually with this wheel if you are using the
/// Reader-Writer Wheel.
#[repr(C)]
#[derive(Clone)]
pub struct WriterWheel<A: Aggregator> {
    /// Current low watermark
    watermark: u64,
    /// Defines the number actual slots used for the write-ahead wheel
    ///
    /// This value may be different than capacity if capacity is not a power of two.
    num_slots: usize,
    /// Defines the capacity of the write-ahead wheel
    capacity: usize,
    /// A Hierarchical Timing Wheel for managing future entries that do not fit within the write-ahead wheel
    overflow: RawTimerWheel<Entry<A::Input>>,
    /// Pre-allocated memory for mutable write-ahead aggregation
    slots: Box<[Option<A::MutablePartialAggregate>]>,
    /// The current tail of the write-ahead section
    tail: usize,
    /// The current head of the write-ahead section
    head: usize,
}
impl<A: Aggregator> Default for WriterWheel<A> {
    fn default() -> Self {
        Self::with_watermark(0)
    }
}

impl<A: Aggregator> WriterWheel<A> {
    /// Creates a Write wheel starting from the given watermark and a capacity of [DEFAULT_WRITE_AHEAD_SLOTS]
    pub fn with_watermark(watermark: u64) -> Self {
        Self::with_capacity_and_watermark(DEFAULT_WRITE_AHEAD_SLOTS, watermark)
    }
    /// Creates a WriterWheel starting from the given watermark and capacity
    pub fn with_capacity_and_watermark(capacity: usize, watermark: u64) -> Self {
        let num_slots = crate::capacity_to_slots!(capacity);
        Self {
            num_slots,
            capacity,
            watermark,
            overflow: RawTimerWheel::new(watermark),
            slots: (0..capacity)
                .map(|_| None)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            head: 0,
            tail: 0,
        }
    }
    /// Returns the current low watermark
    pub fn watermark(&self) -> u64 {
        self.watermark
    }

    /// Ticks the `WriterWheel` and returns a possible mutable partial aggregate
    ///
    /// Note that you don't need to use this function directly if you are using the `Reader-Writer Wheel`.
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Entry, aggregator::sum::U32SumAggregator, wheels::WriterWheel};
    ///
    /// // Creates a wheel with time 0 and default write-ahead capacity
    /// let mut wheel: WriterWheel<U32SumAggregator> = WriterWheel::default();
    /// // Insert two entries at time 0
    /// wheel.insert(Entry::new(10, 0));
    /// wheel.insert(Entry::new(20, 0));
    /// // verify that the ticked result returns 20 + 10 and that time has advanced
    /// assert_eq!(wheel.tick(), Some(30));
    /// assert_eq!(wheel.watermark(), 1000);
    /// ```
    #[inline]
    pub fn tick(&mut self) -> Option<A::MutablePartialAggregate> {
        // bump the watermark by 1 second as millis
        self.watermark += Duration::SECOND.whole_milliseconds() as u64;

        // bump head + tail
        self.head = self.wrap_add(self.head, 1);
        let tail = self.tail;
        self.tail = self.wrap_add(self.tail, 1);

        // advance the overflow wheel and check there are entries to aggregate
        for entry in self.overflow.advance_to(self.watermark) {
            self.insert(entry); // this is assumed to be safe if it was scheduled correctly
        }

        self.slot(tail).take()
    }

    /// Check whether this wheel can write ahead by ´addend` slots
    #[inline]
    pub(crate) fn can_write_ahead(&self, addend: u64) -> bool {
        (addend as usize) < self.write_ahead_len()
    }

    /// How many write ahead slots are available
    #[inline]
    pub fn write_ahead_len(&self) -> usize {
        self.capacity - self.len()
    }
    // used for uwheel-demo
    #[doc(hidden)]
    pub fn at(&self, subtrahend: usize) -> Option<&A::MutablePartialAggregate> {
        let idx = self.wrap_add(self.tail(), subtrahend);
        self.slots[idx].as_ref()
    }

    /// Attempts to write `entry` into the Wheel
    #[inline(always)]
    fn write_ahead(&mut self, addend: u64, data: A::Input) {
        let slot_idx = self.slot_idx_forward_from_head(addend as usize);
        self.combine_or_lift(slot_idx, data);
    }

    #[inline]
    fn slot(&mut self, idx: usize) -> &mut Option<A::MutablePartialAggregate> {
        &mut self.slots[idx]
    }
    #[inline(always)]
    fn combine_or_lift(&mut self, idx: usize, entry: A::Input) {
        let slot = self.slot(idx);
        match slot {
            Some(dst) => A::combine_mutable(dst, entry),
            None => *slot = Some(A::lift(entry)),
        }
    }
    /// Inserts an entry into the wheel
    ///
    /// Note that you don't need to use this function directly if you are using the `Reader-Writer Wheel`.
    ///
    /// # Safety
    /// - The entry will be dropped if its timestamp is below the current watermark.
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Entry, aggregator::sum::U32SumAggregator, wheels::WriterWheel};
    ///
    /// // Creates a wheel with time 0 and default write-ahead capacity
    /// let mut wheel: WriterWheel<U32SumAggregator> = WriterWheel::default();
    /// // Insert an entry at time 0
    /// wheel.insert(Entry::new(10, 0));
    /// ```
    #[inline]
    pub fn insert(&mut self, e: impl Into<Entry<A::Input>>) {
        let entry = e.into();
        let watermark = self.watermark;

        if entry.timestamp >= watermark {
            let diff = entry.timestamp - self.watermark;
            let seconds = CoreDuration::from_millis(diff).as_secs();
            if self.can_write_ahead(seconds) {
                self.write_ahead(seconds, entry.data);
            } else {
                // Overflows: schedule it to be aggregated later on
                // TODO: batch as many entries at possible into the same overflow slot
                let schedule_ts = watermark + seconds * 1000; // convert back to milliseconds
                self.overflow.schedule_at(schedule_ts, entry).unwrap();
            }
        }
    }
}

impl<A: Aggregator> WheelExt for WriterWheel<A> {
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
        let inner_slots = mem::size_of::<Option<A::MutablePartialAggregate>>() * self.num_slots;
        Some(mem::size_of::<Self>() + inner_slots)
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregator::sum::U64SumAggregator;

    use super::*;

    #[test]
    fn empty_wheel_test() {
        let mut wheel: WriterWheel<U64SumAggregator> =
            WriterWheel::with_capacity_and_watermark(8, 0);
        assert_eq!(wheel.head, 0);
        assert_eq!(wheel.tail, 0);
        assert_eq!(wheel.tick(), None);
    }

    #[test]
    fn write_ahead_test() {
        let mut wheel: WriterWheel<U64SumAggregator> =
            WriterWheel::with_capacity_and_watermark(16, 0);

        wheel.insert(Entry::new(1, 0));
        wheel.insert(Entry::new(10, 1000));
        wheel.insert(Entry::new(20, 2000));
        wheel.insert(Entry::new(10, 15000));

        assert_eq!(wheel.tick(), Some(1));
        assert_eq!(wheel.head, 1);
        assert_eq!(wheel.tail, 1);

        // late event which should be dropped
        wheel.insert(Entry::new(10, 0));
        // verify by checking
        assert_eq!(wheel.at(0), Some(&10));

        wheel.insert(Entry::new(5, 1000));
        assert_eq!(wheel.at(0), Some(&15));

        assert_eq!(wheel.tick(), Some(15));
        assert_eq!(wheel.head, 2);
        assert_eq!(wheel.tail, 2);

        assert_eq!(wheel.tick(), Some(20));
        assert_eq!(wheel.head, 3);
        assert_eq!(wheel.tail, 3);

        for _ in 0..12 {
            assert_eq!(wheel.tick(), None);
        }

        wheel.insert(Entry::new(2, 16000));
        assert_eq!(wheel.tick(), Some(10));
        assert_eq!(wheel.head, 0);
        assert_eq!(wheel.tail, 0);
    }

    #[test]
    fn wrap_around_test() {
        let mut wheel: WriterWheel<U64SumAggregator> =
            WriterWheel::with_capacity_and_watermark(4, 1000);
        wheel.insert(Entry::new(1, 1000));
        wheel.insert(Entry::new(2, 2000));
        wheel.insert(Entry::new(3, 3000));
        wheel.insert(Entry::new(4, 4000));
        assert_eq!(wheel.tick(), Some(1));
        assert_eq!(wheel.tick(), Some(2));
        assert_eq!(wheel.tick(), Some(3));
        assert_eq!(wheel.tick(), Some(4));
        wheel.insert(Entry::new(5, 5000));
        assert_eq!(wheel.head, 0);
        assert_eq!(wheel.tail, 0);
        assert_eq!(wheel.tick(), Some(5));
    }

    #[test]
    fn late_event_handling_test() {
        let mut wheel: WriterWheel<U64SumAggregator> =
            WriterWheel::with_capacity_and_watermark(8, 0);
        wheel.insert(Entry::new(10, 1000));
        wheel.insert(Entry::new(20, 2000));
        assert_eq!(wheel.tick(), None);
        wheel.insert(Entry::new(5, 500)); // Late event
        assert_eq!(wheel.at(0), Some(&10)); // Ensure late event was not added
        assert_eq!(wheel.tick(), Some(10));
    }

    #[test]
    fn overflow_test() {
        let mut watermark = 0;
        let write_ahead_capacity = 8;
        let mut wheel: WriterWheel<U64SumAggregator> =
            WriterWheel::with_capacity_and_watermark(write_ahead_capacity, watermark);

        // Insert entries and ensure they are over the write ahead capacity
        // meaning they will be scheduled to be aggregated later on
        for _i in 0..1000 {
            wheel.insert(Entry::new(1, watermark));
            watermark += 1000;
        }

        let mut time = 0;
        // Tick all the entries up to the watermark and ensure we get results as intended
        while time < watermark {
            assert_eq!(wheel.tick(), Some(1));
            time += 1000;
        }
    }
}
