//! Module of operations that can be performed in a segment tree.
//!
//! A segment tree needs some operation, and this module contains the main [`Operation`]
//! trait, together with the marker trait [`Commutative`]. This module also contains
//! implementations for simple operations.
//!
//! [`Operation`]: trait.Operation.html
//! [`Commutative`]: trait.Commutative.html

use std::num::Wrapping;

#[cfg(feature = "num-bigint")]
mod num;

/// A trait that specifies which associative operator to use in a segment tree.
pub trait Operation<N> {
    /// The operation that is performed to combine two intervals in the segment tree.
    ///
    /// This function must be [associative][1], that is `combine(combine(a, b), c) =
    /// combine(a, combine(b, c))`.
    ///
    /// [1]: https://en.wikipedia.org/wiki/Associative_property
    fn combine(&self, a: &N, b: &N) -> N;
    /// Replace the value in `a` with `combine(a, b)`. This function exists to allow
    /// certain optimizations and by default simply calls `combine`.
    #[inline]
    fn combine_mut(&self, a: &mut N, b: &N) {
        let res = self.combine(&*a, b);
        *a = res;
    }
    /// Replace the value in `b` with `combine(a, b)`. This function exists to allow
    /// certain optimizations and by default simply calls `combine`.
    #[inline]
    fn combine_mut2(&self, a: &N, b: &mut N) {
        let res = self.combine(a, &*b);
        *b = res;
    }
    /// Must return the same as `combine`. This function exists to allow certain
    /// optimizations and by default simply calls `combine_mut`.
    #[inline]
    fn combine_left(&self, mut a: N, b: &N) -> N {
        self.combine_mut(&mut a, b);
        a
    }
    /// Must return the same as `combine`. This function exists to allow certain
    /// optimizations and by default simply calls `combine_mut2`.
    #[inline]
    fn combine_right(&self, a: &N, mut b: N) -> N {
        self.combine_mut2(a, &mut b);
        b
    }
    /// Must return the same as `combine`. This function exists to allow certain
    /// optimizations and by default simply calls `combine_left`.
    #[inline]
    fn combine_both(&self, a: N, b: N) -> N {
        self.combine_left(a, &b)
    }
}

/// A marker trait that specifies that an [`Operation`] is [commutative][1], that is:
/// `combine(a, b) = combine(b, a)`.
///
/// [`Operation`]: trait.Operation.html
/// [1]: https://en.wikipedia.org/wiki/Commutative_property
pub trait Commutative<N>: Operation<N> {}

/// A trait that specifies that this [`Operation`] has an [identity element][1].
///
/// An identity must satisfy `combine(a, id) = a` and `combine(id, a) = a`, where `id` is
/// something returned by [`identity`].
///
/// [`Operation`]: trait.Operation.html
/// [`identity`]: #tymethod.identity
/// [1]: https://en.wikipedia.org/wiki/Identity_element
pub trait Identity<N> {
    /// Returns an element such that if [combined][1] with any element `a` the result must
    /// be `a`.
    ///
    /// [1]: trait.Operation.html#tymethod.combine
    fn identity(&self) -> N;
}

/// A trait for invertible operations.
pub trait Invertible<N> {
    /// A method such that the following code will leave `a` in the same state as it
    /// started.
    ///
    ///```rust
    ///# use segment_tree::ops::{Add, Operation, Invertible};
    ///# let op = Add;
    ///# let mut a = 1i32;
    ///# let mut original_value_of_a = 1i32;
    ///# let mut b = 2i32;
    /// // after running these two methods, a should be unchanged:
    ///op.combine_mut(&mut a, &b);
    ///op.uncombine(&mut a, &b);
    ///assert_eq!(a, original_value_of_a);
    /// // a should also be unchanged after running them in the reverse order
    ///op.uncombine(&mut a, &b);
    ///op.combine_mut(&mut a, &b);
    ///assert_eq!(a, original_value_of_a);
    ///```
    fn uncombine(&self, a: &mut N, b: &N);
}

/// Each node contains the sum of the interval it represents.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct Add;
/// Each node contains the product of the interval it represents.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct Mul;

/// Each node contains the bitwise and of the interval it represents.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct And;
/// Each node contains the bitwise or of the interval it represents.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct Or;
/// Each node contains the bitwise xor of the interval it represents.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct Xor;

/// Each node contains the minimum of the interval it represents.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct Min;
/// Each node contains the maximum of the interval it represents.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct Max;

macro_rules! impl_operation_infix {
    ($op:ty, $ty:ty, $combineop:tt, $doc:expr) => {
        impl Operation<$ty> for $op {
            #[doc = $doc]
            #[inline]
            fn combine(&self, a: &$ty, b: &$ty) -> $ty {
                *a $combineop *b
            }
        }
    }
}
macro_rules! impl_operation_prefix {
    ($op:ty, $ty:ty, $combinef:expr, $doc:expr) => {
        impl Operation<$ty> for $op {
            #[doc = $doc]
            #[inline]
            fn combine(&self, a: &$ty, b: &$ty) -> $ty {
                $combinef(*a, *b)
            }
        }
    };
}
macro_rules! impl_identity {
    ($op:ty, $ty:ty, $iden:expr, $doc:expr) => {
        impl Identity<$ty> for $op {
            #[doc = $doc]
            #[inline]
            fn identity(&self) -> $ty {
                $iden
            }
        }
    };
}
macro_rules! impl_inverse {
    ($op:ty, $ty:ty, $uncombineop:tt, $doc:expr) => {
        impl Invertible<$ty> for $op {
            #[doc = $doc]
            #[inline]
            fn uncombine(&self, a: &mut $ty, b: &$ty) {
                *a = *a $uncombineop *b;
            }
        }
    }
}
macro_rules! impl_unsigned_primitive {
    ($ty:tt) => {
        impl_operation_infix!(Add, $ty, +, "Returns the sum.");
        impl_identity!(Add, $ty, 0, "Returns zero.");
        impl Commutative<$ty> for Add {}

        impl_operation_infix!(Add, Wrapping<$ty>, +, "Returns the sum.");
        impl_identity!(Add, Wrapping<$ty>, Wrapping(0), "Returns zero.");
        impl_inverse!(Add, Wrapping<$ty>, -, "Returns the difference.");
        impl Commutative<Wrapping<$ty>> for Add {}

        impl_operation_infix!(Xor, $ty, ^, "Returns the bitwise exclusive or.");
        impl_identity!(Xor, $ty, 0, "Returns zero.");
        impl_inverse!(Xor, $ty, ^, "Returns the bitwise exclusive or.");
        impl Commutative<$ty> for Xor {}

        impl_operation_infix!(Mul, $ty, *, "Returns the product.");
        impl_identity!(Mul, $ty, 1, "Returns one.");
        impl Commutative<$ty> for Mul {}

        impl_operation_infix!(Mul, Wrapping<$ty>, *, "Returns the product.");
        impl_identity!(Mul, Wrapping<$ty>, Wrapping(1), "Returns one.");
        impl Commutative<Wrapping<$ty>> for Mul {}

        impl_operation_infix!(And, $ty, &, "Returns the bitwise and.");
        impl_identity!(And, $ty, std::$ty::MAX, "Returns the largest possible value.");
        impl Commutative<$ty> for And {}

        impl_operation_infix!(Or, $ty, &, "Returns the bitwise or.");
        impl_identity!(Or, $ty, 0, "Returns zero.");
        impl Commutative<$ty> for Or {}

        impl_operation_prefix!(Min, $ty, std::cmp::min, "Returns the minimum.");
        impl_identity!(Min, $ty, std::$ty::MAX, "Returns the largest possible value.");
        impl Commutative<$ty> for Min {}

        impl_operation_prefix!(Max, $ty, std::cmp::max, "Returns the maximum.");
        impl_identity!(Max, $ty, 0, "Returns zero.");
        impl Commutative<$ty> for Max {}
    }
}
impl_unsigned_primitive!(u8);
impl_unsigned_primitive!(u16);
impl_unsigned_primitive!(u32);
impl_unsigned_primitive!(u64);
impl_unsigned_primitive!(u128);
impl_unsigned_primitive!(usize);

macro_rules! impl_signed_primitive {
    ($ty:tt) => {
        impl_operation_infix!(Add, $ty, +, "Returns the sum.");
        impl_identity!(Add, $ty, 0, "Returns zero.");
        impl_inverse!(Add, $ty, -, "Returns the difference.");
        impl Commutative<$ty> for Add {}

        impl_operation_infix!(Add, Wrapping<$ty>, +, "Returns the sum.");
        impl_identity!(Add, Wrapping<$ty>, Wrapping(0), "Returns zero.");
        impl_inverse!(Add, Wrapping<$ty>, -, "Returns the difference.");
        impl Commutative<Wrapping<$ty>> for Add {}

        impl_operation_infix!(Xor, $ty, ^, "Returns the bitwise exclusive or.");
        impl_identity!(Xor, $ty, 0, "Returns zero.");
        impl_inverse!(Xor, $ty, ^, "Returns the bitwise exclusive or.");
        impl Commutative<$ty> for Xor {}

        impl_operation_infix!(Mul, $ty, *, "Returns the product.");
        impl_identity!(Mul, $ty, 1, "Returns one.");
        impl Commutative<$ty> for Mul {}

        impl_operation_infix!(Mul, Wrapping<$ty>, *, "Returns the product.");
        impl_identity!(Mul, Wrapping<$ty>, Wrapping(1), "Returns one.");
        impl Commutative<Wrapping<$ty>> for Mul {}

        impl_operation_infix!(And, $ty, &, "Returns the bitwise and.");
        impl_identity!(And, $ty, -1, "Returns negative one.");
        impl Commutative<$ty> for And {}

        impl_operation_infix!(Or, $ty, &, "Returns the bitwise or.");
        impl_identity!(Or, $ty, 0, "Returns zero.");
        impl Commutative<$ty> for Or {}

        impl_operation_prefix!(Min, $ty, std::cmp::min, "Returns the minimum.");
        impl_identity!(Min, $ty, std::$ty::MAX, "Returns the largest possible value.");
        impl Commutative<$ty> for Min {}

        impl_operation_prefix!(Max, $ty, std::cmp::max, "Returns the maximum.");
        impl_identity!(Max, $ty, std::$ty::MIN, "Returns the smallest possible value.");
        impl Commutative<$ty> for Max {}
    }
}
impl_signed_primitive!(i8);
impl_signed_primitive!(i16);
impl_signed_primitive!(i32);
impl_signed_primitive!(i64);
impl_signed_primitive!(i128);
impl_signed_primitive!(isize);

impl_operation_infix!(Add, f32, +, "Returns the sum.");
impl_inverse!(Add, f32, -, "Returns the difference.");
impl_identity!(Add, f32, 0.0, "Returns zero.");
impl Commutative<f32> for Add {}

impl_operation_infix!(Mul, f32, *, "Returns the product.");
impl_inverse!(Mul, f32, /, "Returns the ratio.");
impl_identity!(Mul, f32, 1.0, "Returns one.");
impl Commutative<f32> for Mul {}

impl_operation_infix!(Add, f64, +, "Returns the sum.");
impl_inverse!(Add, f64, -, "Returns the difference.");
impl_identity!(Add, f64, 0.0, "Returns zero.");
impl Commutative<f64> for Add {}

impl_operation_infix!(Mul, f64, *, "Returns the product.");
impl_inverse!(Mul, f64, /, "Returns the ratio.");
impl_identity!(Mul, f64, 1.0, "Returns one.");
impl Commutative<f64> for Mul {}

/// Variant of [`Min`] that considers NaN the largest value.
///
/// [`Min`]: struct.Min.html
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct MinIgnoreNaN;
impl_identity!(MinIgnoreNaN, f32, std::f32::NAN, "Returns NaN.");
impl_identity!(MinIgnoreNaN, f64, std::f64::NAN, "Returns NaN.");
impl Commutative<f32> for MinIgnoreNaN {}
impl Commutative<f64> for MinIgnoreNaN {}
impl Operation<f32> for MinIgnoreNaN {
    /// Returns the smallest of the two values.
    ///
    /// If either argument is NaN, the other is returned.
    fn combine(&self, a: &f32, b: &f32) -> f32 {
        if b > a || b.is_nan() {
            *a
        } else {
            *b
        }
    }
}
impl Operation<f64> for MinIgnoreNaN {
    /// Returns the smallest of the two values.
    ///
    /// If either argument is NaN, the other is returned.
    fn combine(&self, a: &f64, b: &f64) -> f64 {
        if b > a || b.is_nan() {
            *a
        } else {
            *b
        }
    }
}

/// Variant of [`Min`] that considers NaN the smallest value.
///
/// [`Min`]: struct.Min.html
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct MinTakeNaN;
impl_identity!(MinTakeNaN, f32, std::f32::INFINITY, "Returns infinity.");
impl_identity!(MinTakeNaN, f64, std::f64::INFINITY, "Returns infinity.");
impl Commutative<f32> for MinTakeNaN {}
impl Commutative<f64> for MinTakeNaN {}
impl Operation<f32> for MinTakeNaN {
    /// Returns the smallest of the two values.
    ///
    /// If either argument is NaN, this method returns NaN.
    fn combine(&self, a: &f32, b: &f32) -> f32 {
        if b > a || a.is_nan() {
            *a
        } else {
            *b
        }
    }
}
impl Operation<f64> for MinTakeNaN {
    /// Returns the smallest of the two values.
    ///
    /// If either argument is NaN, this method returns NaN.
    fn combine(&self, a: &f64, b: &f64) -> f64 {
        if b > a || a.is_nan() {
            *a
        } else {
            *b
        }
    }
}

/// Variant of [`Max`] that considers NaN the smallest value.
///
/// [`Max`]: struct.Max.html
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct MaxIgnoreNaN;
impl_identity!(MaxIgnoreNaN, f32, std::f32::NAN, "Returns NaN.");
impl_identity!(MaxIgnoreNaN, f64, std::f64::NAN, "Returns NaN.");
impl Commutative<f32> for MaxIgnoreNaN {}
impl Commutative<f64> for MaxIgnoreNaN {}
impl Operation<f32> for MaxIgnoreNaN {
    /// Returns the largest of the two values.
    ///
    /// If either argument is NaN, the other is returned.
    fn combine(&self, a: &f32, b: &f32) -> f32 {
        if b < a || b.is_nan() {
            *a
        } else {
            *b
        }
    }
}
impl Operation<f64> for MaxIgnoreNaN {
    /// Returns the largest of the two values.
    ///
    /// If either argument is NaN, the other is returned.
    fn combine(&self, a: &f64, b: &f64) -> f64 {
        if b < a || b.is_nan() {
            *a
        } else {
            *b
        }
    }
}

/// Variant of [`Max`] that considers NaN the largest value.
///
/// [`Max`]: struct.Max.html
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct MaxTakeNaN;
impl_identity!(
    MaxTakeNaN,
    f32,
    std::f32::NEG_INFINITY,
    "Returns negative infinity."
);
impl_identity!(
    MaxTakeNaN,
    f64,
    std::f64::NEG_INFINITY,
    "Returns negative infinity."
);
impl Commutative<f32> for MaxTakeNaN {}
impl Commutative<f64> for MaxTakeNaN {}
impl Operation<f32> for MaxTakeNaN {
    /// Returns the largest of the two values.
    ///
    /// If either argument is NaN, this method returns NaN.
    fn combine(&self, a: &f32, b: &f32) -> f32 {
        if b < a || a.is_nan() {
            *a
        } else {
            *b
        }
    }
}
impl Operation<f64> for MaxTakeNaN {
    /// Returns the largest of the two values.
    ///
    /// If either argument is NaN, this method returns NaN.
    fn combine(&self, a: &f64, b: &f64) -> f64 {
        if b < a || a.is_nan() {
            *a
        } else {
            *b
        }
    }
}

impl_operation_infix!(And, bool, &&, "Returns the boolean and.");
impl_identity!(And, bool, true, "Returns `true`.");

impl_operation_infix!(Or, bool, ||, "Returns the boolean or.");
impl_identity!(Or, bool, false, "Returns `false`.");

impl_operation_infix!(Xor, bool, ^, "Returns the boolean xor.");
impl_inverse!(Xor, bool, ^, "Returns the boolean xor.");
impl_identity!(Xor, bool, false, "Returns `false`.");

#[cfg(test)]
mod tests {
    use crate::ops::*;
    use std::{f32, i32, u32};
    #[test]
    fn ops_nan() {
        assert_eq!(MaxIgnoreNaN.combine_both(0.0, 1.0), 1.0);
        assert_eq!(MaxIgnoreNaN.combine_both(1.0, 0.0), 1.0);
        assert_eq!(MaxIgnoreNaN.combine_both(f32::NAN, 1.0), 1.0);
        assert_eq!(MaxIgnoreNaN.combine_both(1.0, f32::NAN), 1.0);
        assert_eq!(
            MaxIgnoreNaN.combine_both(f32::NAN, f32::NEG_INFINITY),
            f32::NEG_INFINITY
        );
        assert_eq!(
            MaxIgnoreNaN.combine_both(f32::NEG_INFINITY, f32::NAN),
            f32::NEG_INFINITY
        );
        assert!(MaxIgnoreNaN.combine_both(f32::NAN, f32::NAN).is_nan());

        assert_eq!(MinIgnoreNaN.combine_both(0.0, 1.0), 0.0);
        assert_eq!(MinIgnoreNaN.combine_both(1.0, 0.0), 0.0);
        assert_eq!(MinIgnoreNaN.combine_both(f32::NAN, 1.0), 1.0);
        assert_eq!(MinIgnoreNaN.combine_both(1.0, f32::NAN), 1.0);
        assert_eq!(
            MinIgnoreNaN.combine_both(f32::NAN, f32::INFINITY),
            f32::INFINITY
        );
        assert_eq!(
            MinIgnoreNaN.combine_both(f32::INFINITY, f32::NAN),
            f32::INFINITY
        );
        assert!(MinIgnoreNaN.combine_both(f32::NAN, f32::NAN).is_nan());

        assert_eq!(MaxTakeNaN.combine_both(0.0, 1.0), 1.0);
        assert_eq!(MaxTakeNaN.combine_both(1.0, 0.0), 1.0);
        assert!(MaxTakeNaN.combine_both(f32::NAN, f32::INFINITY).is_nan());
        assert!(MaxTakeNaN.combine_both(f32::INFINITY, f32::NAN).is_nan());
        assert!(MaxTakeNaN
            .combine_both(f32::NAN, f32::NEG_INFINITY)
            .is_nan());
        assert!(MaxTakeNaN
            .combine_both(f32::NEG_INFINITY, f32::NAN)
            .is_nan());
        assert!(MaxTakeNaN.combine_both(f32::NAN, f32::NAN).is_nan());

        assert_eq!(MinTakeNaN.combine_both(0.0, 1.0), 0.0);
        assert_eq!(MinTakeNaN.combine_both(1.0, 0.0), 0.0);
        assert!(MinTakeNaN.combine_both(f32::NAN, f32::INFINITY).is_nan());
        assert!(MinTakeNaN.combine_both(f32::INFINITY, f32::NAN).is_nan());
        assert!(MinTakeNaN
            .combine_both(f32::NAN, f32::NEG_INFINITY)
            .is_nan());
        assert!(MinTakeNaN
            .combine_both(f32::NEG_INFINITY, f32::NAN)
            .is_nan());
        assert!(MinTakeNaN.combine_both(f32::NAN, f32::NAN).is_nan());
    }
    #[test]
    fn ops_and_identity() {
        for i in -200i32..=200i32 {
            assert_eq!(And.combine_both(i, And.identity()), i);
        }
        assert_eq!(And.combine_both(i32::MAX, And.identity()), i32::MAX);
        assert_eq!(And.combine_both(i32::MIN, And.identity()), i32::MIN);
        assert_eq!(And.combine_both(0i32, And.identity()), 0i32);

        assert_eq!(And.combine_both(0u32, And.identity()), 0u32);
        assert_eq!(And.combine_both(u32::MAX, And.identity()), u32::MAX);
    }
}

/// Store several pieces of information in each node.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Default, Hash)]
pub struct Pair<A, B> {
    a: A,
    b: B,
}
impl<A, B> Pair<A, B> {
    /// Create a pair operation.
    pub fn wrap(a: A, b: B) -> Pair<A, B> {
        Pair { a, b }
    }
    /// Returns the inner operations.
    pub fn into_inner(self) -> (A, B) {
        (self.a, self.b)
    }
}
impl<TA, TB, A: Operation<TA>, B: Operation<TB>> Operation<(TA, TB)> for Pair<A, B> {
    #[inline]
    fn combine(&self, a: &(TA, TB), b: &(TA, TB)) -> (TA, TB) {
        (self.a.combine(&a.0, &b.0), self.b.combine(&a.1, &b.1))
    }
    #[inline]
    fn combine_mut(&self, a: &mut (TA, TB), b: &(TA, TB)) {
        self.a.combine_mut(&mut a.0, &b.0);
        self.b.combine_mut(&mut a.1, &b.1);
    }
    #[inline]
    fn combine_mut2(&self, a: &(TA, TB), b: &mut (TA, TB)) {
        self.a.combine_mut2(&a.0, &mut b.0);
        self.b.combine_mut2(&a.1, &mut b.1);
    }
    #[inline]
    fn combine_left(&self, a: (TA, TB), b: &(TA, TB)) -> (TA, TB) {
        (
            self.a.combine_left(a.0, &b.0),
            self.b.combine_left(a.1, &b.1),
        )
    }
    #[inline]
    fn combine_right(&self, a: &(TA, TB), b: (TA, TB)) -> (TA, TB) {
        (
            self.a.combine_right(&a.0, b.0),
            self.b.combine_right(&a.1, b.1),
        )
    }
    #[inline]
    fn combine_both(&self, a: (TA, TB), b: (TA, TB)) -> (TA, TB) {
        (self.a.combine_both(a.0, b.0), self.b.combine_both(a.1, b.1))
    }
}
impl<TA, TB, A: Invertible<TA>, B: Invertible<TB>> Invertible<(TA, TB)> for Pair<A, B> {
    #[inline(always)]
    fn uncombine(&self, a: &mut (TA, TB), b: &(TA, TB)) {
        self.a.uncombine(&mut a.0, &b.0);
        self.b.uncombine(&mut a.1, &b.1);
    }
}
impl<TA, TB, A: Commutative<TA>, B: Commutative<TB>> Commutative<(TA, TB)> for Pair<A, B> {}
impl<TA, TB, A: Identity<TA>, B: Identity<TB>> Identity<(TA, TB)> for Pair<A, B> {
    fn identity(&self) -> (TA, TB) {
        (self.a.identity(), self.b.identity())
    }
}
