// Taken from Rust's VecDeque Iter

use crate::rw_wheel::ext::{count, wrap_index};

use super::*;
use core::iter::Iterator;

pub struct Iter<'a, A: Aggregator> {
    ring: &'a [Option<A::PartialAggregate>],
    tail: usize,
    head: usize,
}

impl<'a, A: Aggregator> Iter<'a, A> {
    pub fn new(ring: &'a [Option<A::PartialAggregate>], tail: usize, head: usize) -> Self {
        Iter { ring, tail, head }
    }
    #[inline]
    pub fn combine(self) -> Option<A::PartialAggregate> {
        let mut res: Option<A::PartialAggregate> = None;
        for partial in self.flatten() {
            combine_or_insert::<A>(&mut res, *partial);
        }
        res
    }
}
impl<'a, A: Aggregator> Iterator for Iter<'a, A> {
    type Item = &'a Option<A::PartialAggregate>;

    #[inline]
    fn next(&mut self) -> Option<&'a Option<A::PartialAggregate>> {
        if self.tail == self.head {
            return None;
        }
        let tail = self.tail;
        self.tail = wrap_index(self.tail.wrapping_add(1), self.ring.len());
        // Safety:
        // - `self.tail` in a ring buffer is always a valid index.
        // - `self.head` and `self.tail` equality is checked above.
        Some(&self.ring[tail])
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = count(self.tail, self.head, self.ring.len());
        (len, Some(len))
    }
}

pub struct DrillIter<'a, A: Aggregator> {
    ring: &'a [Option<Vec<A::PartialAggregate>>],
    tail: usize,
    head: usize,
}

impl<'a, A: Aggregator> DrillIter<'a, A> {
    pub(super) fn new(
        ring: &'a [Option<Vec<A::PartialAggregate>>],
        tail: usize,
        head: usize,
    ) -> Self {
        DrillIter { ring, tail, head }
    }
}
impl<'a, A: Aggregator> Iterator for DrillIter<'a, A> {
    type Item = Option<&'a [A::PartialAggregate]>;

    #[inline]
    fn next(&mut self) -> Option<Option<&'a [A::PartialAggregate]>> {
        if self.tail == self.head {
            return None;
        }
        let tail = self.tail;
        self.tail = wrap_index(self.tail.wrapping_add(1), self.ring.len());
        // Safety:
        // - `self.tail` in a ring buffer is always a valid index.
        // - `self.head` and `self.tail` equality is checked above.
        Some(self.ring[tail].as_deref())
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = count(self.tail, self.head, self.ring.len());
        (len, Some(len))
    }
}
