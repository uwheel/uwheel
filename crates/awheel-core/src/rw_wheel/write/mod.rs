use core::time::Duration as CoreDuration;

use crate::{aggregator::Aggregator, time::Duration, Entry, Error};
//use smallvec::SmallVec;

use super::ext::WheelExt;

/// Number of write ahead slots
pub const DEFAULT_WRITE_AHEAD_SLOTS: usize = 64;

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

/// A fixed-sized Write-ahead Wheel where slots are represented as seconds
#[repr(C)]
#[derive(Debug, Clone)]
pub struct WriteAheadWheel<A: Aggregator> {
    watermark: u64,
    capacity: usize,
    slots: Box<[Option<A::MutablePartialAggregate>]>,
    tail: usize,
    head: usize,
}
impl<A: Aggregator> Default for WriteAheadWheel<A> {
    fn default() -> Self {
        Self::with_watermark(0)
    }
}

impl<A: Aggregator> WriteAheadWheel<A> {
    pub fn with_watermark(watermark: u64) -> Self {
        Self::with_capacity_and_watermark(DEFAULT_WRITE_AHEAD_SLOTS, watermark)
    }
    pub fn with_capacity_and_watermark(capacity: usize, watermark: u64) -> Self {
        crate::assert_capacity!(capacity);
        Self {
            capacity,
            watermark,
            slots: (0..capacity)
                .map(|_| None)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            head: 0,
            tail: 0,
        }
    }
    pub fn watermark(&self) -> u64 {
        self.watermark
    }

    #[inline]
    pub(super) fn tick(&mut self) -> Option<A::MutablePartialAggregate> {
        self.watermark += Duration::seconds(1i64).whole_milliseconds() as u64;
        // bump head
        self.head = self.wrap_add(self.head, 1);

        if !self.is_empty() {
            let tail = self.tail;
            self.tail = self.wrap_add(self.tail, 1);
            self.slot(tail).take()
        } else {
            None
        }
    }
    pub(super) fn watermark_mut(&mut self) -> &mut u64 {
        &mut self.watermark
    }

    /// Returns `true` if the wheel is empty or `false` if it contains slots
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail == self.head
    }

    /// Check whether this wheel can write ahead by ´addend` slots
    pub(crate) fn can_write_ahead(&self, addend: u64) -> bool {
        addend as usize <= self.write_ahead_len()
    }

    /// How many write ahead slots are available
    #[inline]
    pub fn write_ahead_len(&self) -> usize {
        self.capacity - self.len()
    }

    /// Attempts to write `entry` into the Wheel
    #[inline]
    fn write_ahead(&mut self, addend: u64, data: A::Input) {
        let slot_idx = self.slot_idx_forward_from_head(addend as usize);
        self.combine_or_lift(slot_idx, data);
    }

    #[inline]
    fn slot(&mut self, idx: usize) -> &mut Option<A::MutablePartialAggregate> {
        &mut self.slots[idx]
    }
    #[inline]
    fn combine_or_lift(&mut self, idx: usize, entry: A::Input) {
        let slot = self.slot(idx);
        match slot {
            Some(window) => A::combine_mutable(window, entry),
            None => *slot = Some(A::lift(entry)),
        }
    }
    /// Inserts entry into the wheel
    #[inline]
    pub fn insert(&mut self, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        let watermark = self.watermark;

        // If timestamp is below the watermark, then reject it.
        if entry.timestamp < watermark {
            Err(Error::Late { entry, watermark })
        } else {
            let diff = entry.timestamp - self.watermark;
            let seconds = CoreDuration::from_millis(diff).as_secs();
            if self.can_write_ahead(seconds) {
                self.write_ahead(seconds, entry.data);
                Ok(())
            } else {
                // cannot fit within the write-ahead wheel, return it to the user to handle it..
                let write_ahead_ms =
                    CoreDuration::from_secs(self.write_ahead_len() as u64).as_millis();
                let max_write_ahead_ts = self.watermark + write_ahead_ms as u64;
                Err(Error::Overflow {
                    entry,
                    max_write_ahead_ts,
                })
            }
        }
    }
}

impl<A: Aggregator> WheelExt for WriteAheadWheel<A> {
    fn capacity(&self) -> usize {
        self.capacity
    }
    fn head(&self) -> usize {
        self.head
    }
    fn tail(&self) -> usize {
        self.tail
    }
}
