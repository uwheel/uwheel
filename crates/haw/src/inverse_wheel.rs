use crate::aggregator::Aggregator;
#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

/// A fixed-sized wheel used to maintain partial aggregates for slides that can later
/// be used to inverse windows.
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Clone)]
pub struct InverseWheel<A: Aggregator> {
    capacity: usize,
    aggregator: A,
    slots: Box<[Option<A::PartialAggregate>]>,
    tail: usize,
    head: usize,
}

impl<A: Aggregator> InverseWheel<A> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "Capacity must be power of two");
        Self {
            capacity,
            aggregator: Default::default(),
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
            // Tick next partial agg to be inversed
            // 1: [0-10] 2: [10-20] -> need that to be [0-20] so we combine
            let partial_agg = self.slot(tail).take();
            if let Some(agg) = partial_agg {
                Self::insert(self.slot(self.tail), agg, &Default::default());
            }
            partial_agg
        } else {
            None
        }
    }

    /// Returns `true` if the wheel is empty or `false` if it contains slots
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail == self.head
    }

    pub fn clear_current_tail(&mut self) {
        *self.slot(self.tail) = Some(Default::default());
    }

    /// Attempts to write `entry` into the Wheel
    #[inline]
    pub fn write_ahead(&mut self, addend: u64, data: A::PartialAggregate, aggregator: &A) {
        let slot_idx = self.slot_idx_forward_from_head(addend as usize);
        //dbg!(slot_idx);
        Self::insert(self.slot(slot_idx), data, aggregator);
    }

    #[inline]
    fn slot(&mut self, idx: usize) -> &mut Option<A::PartialAggregate> {
        &mut self.slots[idx]
    }
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
    #[inline]
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

#[cfg(test)]
mod tests {
    use crate::aggregator::U64SumAggregator;

    use super::*;

    #[test]
    fn inverse_wheel_test() {
        let mut iwheel: InverseWheel<U64SumAggregator> = InverseWheel::with_capacity(64);
        let aggregator = U64SumAggregator::default();

        iwheel.write_ahead(0, 2u64, &aggregator);
        iwheel.write_ahead(1, 3u64, &aggregator);
        iwheel.write_ahead(2, 10u64, &aggregator);

        assert_eq!(iwheel.tick().unwrap(), 2u64);
        assert_eq!(iwheel.tick().unwrap(), 5u64);
        assert_eq!(iwheel.tick().unwrap(), 15u64);
    }
}
