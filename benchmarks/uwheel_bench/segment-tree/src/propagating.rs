use std::{default::Default, mem};

use crate::ops::{Commutative, Identity};

/// This data structure allows range modification and single element queries.
///
/// This tree allocates `2n * sizeof(N)` bytes of memory.
///
/// This tree is implemented using a binary tree, where each node contains the changes
/// that need to be propogated to its children.
///
/// # Examples
///
/// Quickly add something to every value in some interval.
///
/// ```rust
/// use segment_tree::PointSegment;
/// use segment_tree::ops::Add;
///
/// use std::iter::repeat;
///
/// // make a giant tree of zeroes
/// let mut tree = PointSegment::build(repeat(0).take(1_000_000).collect(), Add);
///
/// // add one to every value between 200 and 500000
/// tree.modify(200, 500_000, 1);
/// assert_eq!(tree.query(100), 0);
/// assert_eq!(tree.query(200), 1);
/// assert_eq!(tree.query(500), 1);
/// assert_eq!(tree.query(499_999), 1);
/// assert_eq!(tree.query(500_000), 0);
///
/// // add five to every value between 0 and 1000
/// tree.modify(0, 1000, 5);
/// assert_eq!(tree.query(10), 5);
/// assert_eq!(tree.query(500), 6);
/// assert_eq!(tree.query(10_000), 1);
/// assert_eq!(tree.query(600_000), 0);
/// ```
pub struct PointSegment<N, O>
where
    O: Commutative<N> + Identity<N>,
{
    buf: Vec<N>,
    n: usize,
    op: O,
}

impl<N, O: Commutative<N> + Identity<N>> PointSegment<N, O> {
    /// Builds a tree using the given buffer. If the given buffer is less than half full,
    /// this function allocates.
    /// Uses `O(len)` time.
    pub fn build(mut buf: Vec<N>, op: O) -> PointSegment<N, O> {
        let n = buf.len();
        buf.reserve_exact(n);
        for i in 0..n {
            let val = mem::replace(&mut buf[i], op.identity());
            buf.push(val);
        }
        PointSegment { buf, n, op }
    }
    /// Allocate a new buffer and build the tree using the values in the slice.
    /// Uses `O(len)` time.
    pub fn build_slice(buf: &[N], op: O) -> PointSegment<N, O>
    where
        N: Clone,
    {
        PointSegment::build_iter(buf.iter().cloned(), op)
    }
    /// Allocate a new buffer and build the tree using the values in the iterator.
    /// Uses `O(len)` time.
    pub fn build_iter<I: ExactSizeIterator<Item = N>>(iter: I, op: O) -> PointSegment<N, O> {
        let n = iter.len();
        let mut buf = Vec::with_capacity(2 * n);
        for _ in 0..n {
            buf.push(op.identity());
        }
        buf.extend(iter);
        PointSegment { buf, n, op }
    }
    /// Computes the value at `p`.
    /// Uses `O(log(len))` time.
    pub fn query(&self, mut p: usize) -> N {
        p += self.n;
        let mut res = self.op.identity();
        while p > 0 {
            res = self.op.combine_left(res, &self.buf[p]);
            p >>= 1;
        }
        res
    }
    /// Combine every value in the interval with `delta`.
    /// Uses `O(log(len))` time.
    pub fn modify(&mut self, mut l: usize, mut r: usize, delta: N) {
        l += self.n;
        r += self.n;
        while l < r {
            if l & 1 == 1 {
                self.op.combine_mut(&mut self.buf[l], &delta);
                l += 1;
            }
            if r & 1 == 1 {
                r -= 1;
                self.op.combine_mut(&mut self.buf[r], &delta);
            }
            l >>= 1;
            r >>= 1;
        }
    }
    /// Propogate all changes to the leaves in the tree and return a mutable slice
    /// containing the leaves.
    ///
    /// Uses `O(len)` time.
    pub fn propogate(&mut self) -> &mut [N] {
        for i in 1..self.n {
            let prev = mem::replace(&mut self.buf[i], self.op.identity());
            self.op.combine_mut(&mut self.buf[i << 1], &prev);
            self.op.combine_mut(&mut self.buf[i << 1 | 1], &prev);
        }
        &mut self.buf[self.n..]
    }
    /// Returns the number of values in the underlying array.
    /// Uses `O(1)` time.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.n
    }
}

impl<N: Clone, O: Identity<N> + Commutative<N> + Clone> Clone for PointSegment<N, O> {
    #[inline]
    fn clone(&self) -> PointSegment<N, O> {
        PointSegment {
            buf: self.buf.clone(),
            n: self.n,
            op: self.op.clone(),
        }
    }
}
impl<N, O: Identity<N> + Commutative<N> + Default> Default for PointSegment<N, O> {
    #[inline]
    fn default() -> PointSegment<N, O> {
        PointSegment {
            buf: Vec::new(),
            n: 0,
            op: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{ops::*, propagating::*};
    use rand::{distributions::Standard, prelude::*};
    use std::num::Wrapping;

    #[test]
    fn test() {
        let mut rng = thread_rng();
        for i in 1..130 {
            let mut buf: Vec<Wrapping<i32>> = rng
                .sample_iter(&Standard)
                .map(|n| Wrapping(n))
                .take(i)
                .collect();
            let mut tree = match i % 3 {
                0 => PointSegment::build_slice(&buf[..], Add),
                1 => PointSegment::build_iter(buf.iter().cloned(), Add),
                2 => PointSegment::build(buf.clone(), Add),
                _ => unreachable!(),
            };
            let tests: Vec<(usize, usize, i32)> = rng.sample_iter(&Standard).take(10).collect();
            for (n, m, v) in tests {
                let n = n % i;
                let m = m % i;
                if n > m {
                    continue;
                }
                for index in n..m {
                    buf[index] += Wrapping(v);
                }
                tree.modify(n, m, Wrapping(v));
                for index in 0..i {
                    assert_eq!(buf[index], tree.query(index));
                }
            }
            assert_eq!(&mut buf[..], tree.propogate());
        }
    }
}
