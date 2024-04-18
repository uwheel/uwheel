// Taken from Rust's VecDeque Iter

use crate::wheels::wheel_ext::{count, wrap_index};

use super::*;

/// An Iterator over a slice of partial aggregates
pub struct Iter<'a, A: Aggregator> {
    ring: &'a [Option<A::PartialAggregate>],
    tail: usize,
    head: usize,
}

impl<'a, A: Aggregator> Iter<'a, A> {
    #[doc(hidden)]
    pub fn new(ring: &'a [Option<A::PartialAggregate>], tail: usize, head: usize) -> Self {
        Iter { ring, tail, head }
    }
    /// Combines each partial aggregate in the Iterator and returns the final result
    #[inline]
    pub fn combinez(self) -> Option<A::PartialAggregate> {
        let mut res: Option<A::PartialAggregate> = None;
        for partial in self.flatten() {
            combine_or_insert::<A>(&mut res, *partial);
        }
        res
    }
    /// Combines each partial aggregate in the Iterator and returns the final result + combine operations
    #[inline]
    pub fn combine(self) -> (Option<A::PartialAggregate>, usize) {
        let mut res: Option<A::PartialAggregate> = None;
        let mut ops = 0;
        for partial in self.flatten() {
            if res.is_some() {
                // count combine operation only if current res has some value
                ops += 1;
            }
            combine_or_insert::<A>(&mut res, *partial);
        }
        (res, ops)
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
