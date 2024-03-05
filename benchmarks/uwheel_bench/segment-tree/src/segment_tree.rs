use std::{
    cmp::{Eq, PartialEq},
    default::Default,
    fmt,
    hash::{Hash, Hasher},
    mem,
};

use crate::{
    maybe_owned::MaybeOwned,
    ops::{Commutative, Identity, Operation},
};

/// This data structure allows range queries and single element modification.
///
/// This tree allocates `2n * sizeof(N)` bytes of memory.
///
/// This tree is implemented using a segment tree.  A segment tree is a binary tree where
/// each node contains the combination of the children under the operation.
pub struct SegmentPoint<N, O>
where
    O: Operation<N>,
{
    buf: Vec<N>,
    n: usize,
    op: O,
}

impl<N, O: Operation<N>> SegmentPoint<N, O> {
    /// Builds a tree using the given buffer.  If the given buffer is less than half full,
    /// this function allocates.  This function clones every value in the input array.
    /// Uses `O(len)` time.
    ///
    /// See also the function [`build_noalloc`].
    ///
    /// [`build_noalloc`]: struct.SegmentPoint.html#method.build_noalloc
    pub fn build(mut buf: Vec<N>, op: O) -> SegmentPoint<N, O>
    where
        N: Clone,
    {
        let n = buf.len();
        buf.reserve_exact(n);
        for i in 0..n {
            debug_assert!(i < buf.len());
            let clone = unsafe { buf.get_unchecked(i).clone() }; // i < n < buf.len()
            buf.push(clone);
        }
        SegmentPoint::build_noalloc(buf, op)
    }
    /// Set the value at the specified index and return the old value.
    /// Uses `O(log(len))` time.
    pub fn modify(&mut self, mut p: usize, value: N) -> N {
        p += self.n;
        let res = mem::replace(&mut self.buf[p], value);
        while {
            p >>= 1;
            p > 0
        } {
            self.buf[p] = self.op.combine(&self.buf[p << 1], &self.buf[p << 1 | 1]);
        }
        res
    }
    /// Computes `a[l] * a[l+1] * ... * a[r-1]`.
    /// Uses `O(log(len))` time.
    ///
    /// If `l >= r`, this method returns the identity.
    ///
    /// See [`query_noiden`] or [`query_noclone`] for a version that works with
    /// non-[commutative operations][1].
    ///
    /// [`query_noiden`]: struct.SegmentPoint.html#method.query_noiden
    /// [`query_noclone`]: struct.SegmentPoint.html#method.query_noclone
    /// [1]: ops/trait.Commutative.html
    pub fn query(&self, mut l: usize, mut r: usize) -> (N, usize)
    where
        O: Commutative<N> + Identity<N>,
    {
        let mut res = self.op.identity();
        let mut count = 0;
        l += self.n;
        r += self.n;
        while l < r {
            if l & 1 == 1 {
                res = self.op.combine_left(res, &self.buf[l]);
                count += 1;
                l += 1;
            }
            if r & 1 == 1 {
                r -= 1;
                res = self.op.combine_left(res, &self.buf[r]);
                count += 1;
            }
            l >>= 1;
            r >>= 1;
        }
        (res, count)
    }
    /// Combine the value at `p` with `delta`.
    /// Uses `O(log(len))` time.
    #[inline(always)]
    pub fn compose(&mut self, p: usize, delta: &N)
    where
        O: Commutative<N>,
    {
        self.compose_right(p, delta);
    }
    /// Combine the value at `p` with `delta`, such that `delta` is the left argument.
    /// Uses `O(log(len))` time.
    pub fn compose_left(&mut self, mut p: usize, delta: &N) {
        p += self.n;
        self.op.combine_mut2(delta, &mut self.buf[p]);
        while {
            p >>= 1;
            p > 0
        } {
            self.buf[p] = self.op.combine(&self.buf[p << 1], &self.buf[p << 1 | 1]);
        }
    }
    /// Combine the value at `p` with `delta`, such that `delta` is the right argument.
    /// Uses `O(log(len))` time.
    pub fn compose_right(&mut self, mut p: usize, delta: &N) {
        p += self.n;
        self.op.combine_mut(&mut self.buf[p], delta);
        while {
            p >>= 1;
            p > 0
        } {
            self.buf[p] = self.op.combine(&self.buf[p << 1], &self.buf[p << 1 | 1]);
        }
    }
    /// View the values in this segment tree using a slice.  Uses `O(1)` time.
    #[inline(always)]
    pub fn view(&self) -> &[N] {
        &self.buf[self.n..]
    }
    /// The number of elements stored in this segment tree.  Uses `O(1)` time.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.n
    }
    /// Builds a tree using the given buffer.  The buffer must be even in size.
    /// The first `n` values have no effect on the resulting tree,
    /// and the remaining `n` values contains the array to build the tree on.
    /// Uses `O(len)` time.
    ///
    /// This function panics if the size of the buffer is odd.
    ///
    /// # Example
    ///
    /// ```rust
    /// use segment_tree::SegmentPoint;
    /// use segment_tree::ops::Min;
    ///
    /// // make a segment point using the other build method
    /// let mut tree = SegmentPoint::build(
    ///     vec![1, 2, 3, 4], Min
    /// );
    /// // make a segment point using the build_noalloc method:
    /// let mut tree1 = SegmentPoint::build_noalloc(
    ///     // the first half of the values are ignored
    ///     vec![3282, 210, 0, 245, 1, 2, 3, 4], Min
    /// );
    /// assert_eq!(tree, tree1);
    ///
    /// let mut tree2 = SegmentPoint::build_noalloc(
    ///     // we can also try some other first few values
    ///     vec![0, 0, 0, 0, 1, 2, 3, 4], Min
    /// );
    /// assert_eq!(tree1, tree2);
    /// assert_eq!(tree, tree2);
    /// ```
    pub fn build_noalloc(mut buf: Vec<N>, op: O) -> SegmentPoint<N, O> {
        let len = buf.len();
        let n = len >> 1;
        if len & 1 == 1 {
            panic!("SegmentPoint::build_noalloc: odd size");
        }
        for i in (1..n).rev() {
            let res = op.combine(&buf[i << 1], &buf[i << 1 | 1]);
            buf[i] = res;
        }
        SegmentPoint { buf, op, n }
    }
}
impl<N, O: Operation<N>> SegmentPoint<N, O> {
    /// Like [`query`], except it doesn't require the operation to be commutative, nor to
    /// have any identity.
    ///
    /// Computes `a[l] * a[l+1] * ... * a[r-1]`.
    ///
    /// This method panics if `l >= r`.
    ///
    /// This method clones at most twice and runs in `O(log(len))` time.
    /// See [`query_noclone`] for a version that doesn't clone.
    ///
    /// [`query_noclone`]: struct.SegmentPoint.html#method.query_noclone
    /// [`query`]: struct.SegmentPoint.html#method.query
    pub fn query_noiden(&self, mut l: usize, mut r: usize) -> N
    where
        N: Clone,
    {
        let mut resl = None;
        let mut resr = None;
        l += self.n;
        r += self.n;
        while l < r {
            if l & 1 == 1 {
                resl = match resl {
                    None => Some(self.buf[l].clone()),
                    Some(v) => Some(self.op.combine_left(v, &self.buf[l])),
                };
                l += 1;
            }
            if r & 1 == 1 {
                r -= 1;
                resr = match resr {
                    None => Some(self.buf[r].clone()),
                    Some(v) => Some(self.op.combine_right(&self.buf[r], v)),
                }
            }
            l >>= 1;
            r >>= 1;
        }
        match resl {
            None => match resr {
                None => panic!("Empty interval."),
                Some(r) => r,
            },
            Some(l) => match resr {
                None => l,
                Some(r) => self.op.combine_both(l, r),
            },
        }
    }
    /// Like [`query_noiden`], except it doesn't clone.
    ///
    /// Computes `a[l] * a[l+1] * ... * a[r-1]`.
    ///
    /// This method panics if `l >= r`.
    ///
    /// Uses `O(log(len))` time.
    /// See also [`query`] and [`query_commut`].
    ///
    /// [`query_commut`]: struct.SegmentPoint.html#method.query_commut
    /// [`query_noiden`]: struct.SegmentPoint.html#method.query_noiden
    /// [`query`]: struct.SegmentPoint.html#method.query
    pub fn query_noclone<'a>(&'a self, mut l: usize, mut r: usize) -> MaybeOwned<'a, N> {
        let mut resl = None;
        let mut resr = None;
        l += self.n;
        r += self.n;
        while l < r {
            if l & 1 == 1 {
                resl = match resl {
                    None => Some(MaybeOwned::Borrowed(&self.buf[l])),
                    Some(MaybeOwned::Borrowed(ref v)) => {
                        Some(MaybeOwned::Owned(self.op.combine(v, &self.buf[l])))
                    }
                    Some(MaybeOwned::Owned(v)) => {
                        Some(MaybeOwned::Owned(self.op.combine_left(v, &self.buf[l])))
                    }
                };
                l += 1;
            }
            if r & 1 == 1 {
                r -= 1;
                resr = match resr {
                    None => Some(MaybeOwned::Borrowed(&self.buf[r])),
                    Some(MaybeOwned::Borrowed(ref v)) => {
                        Some(MaybeOwned::Owned(self.op.combine(&self.buf[r], v)))
                    }
                    Some(MaybeOwned::Owned(v)) => {
                        Some(MaybeOwned::Owned(self.op.combine_right(&self.buf[r], v)))
                    }
                }
            }
            l >>= 1;
            r >>= 1;
        }
        match resl {
            None => match resr {
                None => panic!("Empty interval."),
                Some(v) => v,
            },
            Some(MaybeOwned::Borrowed(ref l)) => match resr {
                None => MaybeOwned::Borrowed(l),
                Some(MaybeOwned::Borrowed(ref r)) => MaybeOwned::Owned(self.op.combine(l, r)),
                Some(MaybeOwned::Owned(r)) => MaybeOwned::Owned(self.op.combine_right(l, r)),
            },
            Some(MaybeOwned::Owned(l)) => match resr {
                None => MaybeOwned::Owned(l),
                Some(MaybeOwned::Borrowed(ref r)) => MaybeOwned::Owned(self.op.combine_left(l, r)),
                Some(MaybeOwned::Owned(r)) => MaybeOwned::Owned(self.op.combine_both(l, r)),
            },
        }
    }
}

impl<N: Clone, O: Operation<N> + Clone> Clone for SegmentPoint<N, O> {
    #[inline]
    fn clone(&self) -> SegmentPoint<N, O> {
        SegmentPoint {
            buf: self.buf.clone(),
            n: self.n,
            op: self.op.clone(),
        }
    }
}
impl<N: fmt::Debug, O: Operation<N>> fmt::Debug for SegmentPoint<N, O> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentPoint({:?})", self.view())
    }
}
impl<N: PartialEq, O: Operation<N> + PartialEq> PartialEq for SegmentPoint<N, O> {
    #[inline]
    fn eq(&self, other: &SegmentPoint<N, O>) -> bool {
        self.op.eq(&other.op) && self.view().eq(other.view())
    }
    #[inline]
    fn ne(&self, other: &SegmentPoint<N, O>) -> bool {
        self.op.ne(&other.op) && self.view().ne(other.view())
    }
}
impl<N: Eq, O: Operation<N> + Eq> Eq for SegmentPoint<N, O> {}
impl<N, O: Operation<N> + Default> Default for SegmentPoint<N, O> {
    #[inline]
    fn default() -> SegmentPoint<N, O> {
        SegmentPoint {
            buf: Vec::new(),
            n: 0,
            op: Default::default(),
        }
    }
}
impl<'a, N: 'a + Hash, O: Operation<N>> Hash for SegmentPoint<N, O> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.view().hash(state);
    }
}

// Tests disabled as query syntax has been updated..
// #[cfg(test)]
// mod tests {
//     use crate::{maybe_owned::MaybeOwned, ops::*, SegmentPoint};
//     use rand::{
//         distributions::{Distribution, Standard},
//         prelude::*,
//         seq::SliceRandom,
//     };
//     use std::num::Wrapping;

//     /// Not commutative! Not useful in practice since the root always contains the
//     /// concatenation of every string.
//     #[derive(PartialEq, Eq, Clone, Debug)]
//     struct StrType {
//         value: String,
//     }
//     impl StrType {
//         fn cat(list: &[StrType]) -> StrType {
//             let mut res = String::new();
//             for v in list {
//                 res.push_str(v.value.as_str());
//             }
//             StrType { value: res }
//         }
//         fn sub(&self, i: usize, j: usize) -> StrType {
//             StrType {
//                 value: String::from(&self.value[4 * i..4 * j]),
//             }
//         }
//     }
//     impl Distribution<StrType> for Standard {
//         fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> StrType {
//             let a = rng.gen_range('A' as u8, 'Z' as u8 + 1);
//             let b = rng.gen_range('A' as u8, 'Z' as u8 + 1);
//             let c = rng.gen_range('A' as u8, 'Z' as u8 + 1);
//             let d = rng.gen_range('A' as u8, 'Z' as u8 + 1);
//             let bytes = [a, b, c, d];
//             let utf8 = std::str::from_utf8(&bytes).unwrap();
//             StrType {
//                 value: String::from(utf8),
//             }
//         }
//     }
//     impl Operation<StrType> for Add {
//         fn combine(&self, a: &StrType, b: &StrType) -> StrType {
//             StrType {
//                 value: a.value.clone() + b.value.as_str(),
//             }
//         }
//         fn combine_mut(&self, a: &mut StrType, b: &StrType) {
//             a.value.push_str(b.value.as_str());
//         }
//     }

//     #[test]
//     fn segment_tree_build() {
//         let mut rng = thread_rng();
//         let vals: Vec<Wrapping<i32>> = rng
//             .sample_iter(&Standard)
//             .map(|i| Wrapping(i))
//             .take(130)
//             .collect();
//         for i in 0..vals.len() {
//             let buf: Vec<_> = vals[0..i].iter().cloned().collect();
//             println!("{:?}", buf);
//             let mut buf2 = vec![];
//             let n = buf.len();
//             buf2.resize(2 * n, Wrapping(0));
//             for i in 0..n {
//                 buf2[n + i] = buf[i];
//             }
//             let tree1 = SegmentPoint::build(buf, Add);
//             let tree2 = SegmentPoint::build_noalloc(buf2, Add);
//             let mut buf = tree1.buf;
//             let mut buf2 = tree2.buf;
//             if i > 0 {
//                 buf[0] = Wrapping(0);
//                 buf2[0] = Wrapping(0);
//             }
//             println!("build");
//             println!("{:?}", buf);
//             println!("build_noalloc");
//             println!("{:?}", buf2);
//             assert_eq!(buf, buf2);
//             assert_eq!(buf.len(), 2 * n);
//         }
//     }
//     #[test]
//     fn segment_tree_query() {
//         let mut rng = thread_rng();
//         let vals: Vec<StrType> = rng.sample_iter(&Standard).take(130).collect();
//         for i in 0..vals.len() {
//             let buf: Vec<_> = vals[0..i].iter().cloned().collect();
//             let tree = SegmentPoint::build(buf.clone(), Add);
//             let sum = StrType::cat(&buf);
//             let n = buf.len();
//             println!("n: {} tree.buf.len: {}", n, tree.buf.len());
//             for i in 0..n {
//                 for j in i + 1..n + 1 {
//                     println!("i: {}, j: {}", i, j);
//                     assert_eq!(tree.query_noiden(i, j), sum.sub(i, j));
//                     assert_eq!(tree.query_noclone(i, j), MaybeOwned::Owned(sum.sub(i, j)));
//                 }
//             }
//         }
//     }
//     #[test]
//     fn segment_tree_query_commut() {
//         let mut rng = thread_rng();
//         let vals: Vec<Wrapping<i32>> = rng
//             .sample_iter(&Standard)
//             .map(|i| Wrapping(i))
//             .take(130)
//             .collect();
//         for i in 0..vals.len() {
//             let mut buf: Vec<_> = vals[0..i].iter().cloned().collect();
//             let tree = SegmentPoint::build(buf.clone(), Add);
//             assert_eq!(tree.view(), &buf[..]);

//             for i in 1..buf.len() {
//                 let prev = buf[i - 1];
//                 buf[i] += prev;
//             }

//             let n = buf.len();
//             println!("n: {} tree.buf.len: {}", n, tree.buf.len());
//             for i in 0..n {
//                 for j in i + 1..n + 1 {
//                     println!("i: {}, j: {}", i, j);
//                     if i == 0 {
//                         assert_eq!(tree.query(i, j).0, buf[j - 1]);
//                     } else {
//                         assert_eq!(tree.query(i, j).0, buf[j - 1] - buf[i - 1]);
//                     }
//                 }
//             }
//         }
//     }
//     #[test]
//     fn segment_tree_modify() {
//         let mut rng = thread_rng();
//         let vals1: Vec<Wrapping<i32>> = rng
//             .sample_iter(&Standard)
//             .map(|i| Wrapping(i))
//             .take(130)
//             .collect();
//         let vals2: Vec<Wrapping<i32>> = rng
//             .sample_iter(&Standard)
//             .map(|i| Wrapping(i))
//             .take(130)
//             .collect();
//         for i in 0..vals1.len() {
//             let mut order: Vec<_> = (0..i).collect();
//             order.shuffle(&mut rng);
//             let mut buf: Vec<_> = vals1[0..i].iter().cloned().collect();
//             let mut tree = SegmentPoint::build(buf.clone(), Add);
//             for next in order {
//                 tree.modify(next, vals2[next]);
//                 buf[next] = vals2[next];
//                 let tree2 = SegmentPoint::build(buf.clone(), Add);
//                 assert_eq!(tree.buf[1..], tree2.buf[1..]);
//             }
//         }
//     }
// }
