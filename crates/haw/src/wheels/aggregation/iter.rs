// Taken from Rust's VecDeque Iter

use super::*;
use core::iter::Iterator;

pub struct Iter<'a, const CAP: usize, A: Aggregator> {
    ring: &'a [Option<A::PartialAggregate>; CAP],
    tail: usize,
    head: usize,
}

impl<'a, const CAP: usize, A: Aggregator> Iter<'a, CAP, A> {
    pub(super) fn new(
        ring: &'a [Option<A::PartialAggregate>; CAP],
        tail: usize,
        head: usize,
    ) -> Self {
        Iter { ring, tail, head }
    }
}
impl<'a, const CAP: usize, A: Aggregator> Iterator for Iter<'a, CAP, A> {
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

pub struct DrillIter<'a, const CAP: usize, A: Aggregator> {
    ring: &'a [Option<Vec<A::PartialAggregate>>; CAP],
    tail: usize,
    head: usize,
}

impl<'a, const CAP: usize, A: Aggregator> DrillIter<'a, CAP, A> {
    pub(super) fn new(
        ring: &'a [Option<Vec<A::PartialAggregate>>; CAP],
        tail: usize,
        head: usize,
    ) -> Self {
        DrillIter { ring, tail, head }
    }
}
impl<'a, const CAP: usize, A: Aggregator> Iterator for DrillIter<'a, CAP, A> {
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
