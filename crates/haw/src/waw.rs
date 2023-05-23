use crate::aggregator::Aggregator;
#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

// Write-ahead Wheel with slots represented as seconds
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Clone)]
pub struct Waw<A: Aggregator> {
    capacity: usize,
    slots: Box<[Option<A::PartialAggregate>]>,
    tail: usize,
    head: usize,
}

impl<A: Aggregator> Waw<A> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "Capacity must be power of two");
        Self {
            capacity,
            slots: (0..capacity)
                .map(|_| None)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            head: 0,
            tail: 0,
        }
    }
    #[inline]
    pub fn tick(&mut self) -> Option<A::PartialAggregate> {
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
    pub(crate) fn write_ahead_len(&self) -> usize {
        let diff = self.len();
        self.capacity - diff
    }

    /// Attempts to write `entry` into the Wheel
    #[inline]
    pub fn write_ahead(&mut self, addend: u64, partial_agg: A::PartialAggregate, aggregator: &A) {
        let slot_idx = self.slot_idx_forward_from_head(addend as usize);
        //dbg!(slot_idx);
        self.insert_at(slot_idx, partial_agg, aggregator);
    }
    #[inline]
    fn insert_at(&mut self, slot_idx: usize, entry: A::PartialAggregate, aggregator: &A) {
        Self::insert(self.slot(slot_idx), entry, aggregator);
    }
    #[inline]
    fn slot(&mut self, idx: usize) -> &mut Option<A::PartialAggregate> {
        &mut self.slots[idx]
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

    /// Locate slot id `addend` forward
    fn slot_idx_forward_from_head(&self, addend: usize) -> usize {
        self.wrap_add(self.head, addend)
    }
    /// Returns the current number of used slots (includes empty NONE slots as well)
    pub fn len(&self) -> usize {
        count(self.tail, self.head, self.capacity)
    }
    /// Returns the index in the underlying buffer for a given logical element
    /// index + addend.
    #[inline]
    fn wrap_add(&self, idx: usize, addend: usize) -> usize {
        wrap_index(idx.wrapping_add(addend), self.capacity)
    }
}

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
