use core::{
    default::Default,
    fmt::Debug,
    marker::{Copy, Send},
};

use zerocopy::{AsBytes, FromBytes};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// An All Aggregator enabling the following functions (MAX, MIN, SUM, COUNT, AVG).
#[cfg(feature = "all")]
pub mod all;
/// Incremental AVG aggregation
// #[cfg(feature = "avg")]
// pub mod avg;
/// Incremental MAX aggregation
#[cfg(feature = "max")]
pub mod max;
/// Incremental MIN aggregation
#[cfg(feature = "min")]
pub mod min;
/// Incremental SUM aggregation
#[cfg(feature = "sum")]
pub mod sum;

// #[cfg(feature = "top_n")]
// Top-N Aggregation using a nested Aggregator which has a PartialAggregate that implements `Ord`
// pub mod top_n;

/// Aggregation interface that library users must implement to use awheel
pub trait Aggregator: Default + Debug + Clone + 'static {
    /// Identity value for the Aggregator's Partial Aggregate
    const IDENTITY: Self::PartialAggregate;

    /// Indicates whether the implemented aggregator supports prefix-sum range queries
    const PREFIX_SUPPORT: bool;

    /// Input type that can be inserted into [Self::MutablePartialAggregate]
    type Input: InputBounds;

    /// Mutable Partial Aggregate type
    type MutablePartialAggregate: MutablePartialAggregateType;

    /// Partial Aggregate type
    type PartialAggregate: PartialAggregateType;

    /// Final Aggregate type
    type Aggregate: Debug + Send;

    /// Lifts input into a MutablePartialAggregate
    fn lift(input: Self::Input) -> Self::MutablePartialAggregate;

    /// Combine an input into a mutable partial aggregate
    fn combine_mutable(a: &mut Self::MutablePartialAggregate, input: Self::Input);

    /// Freeze a mutable partial aggregate into an immutable one
    fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate;

    /// Combine two partial aggregates and produce new output
    fn combine(a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate;

    /// Convert a partial aggregate to a final result
    fn lower(a: Self::PartialAggregate) -> Self::Aggregate;

    /// Combines a slice of partial aggregates into a new partial
    ///
    /// A default implementation is provided that iterates over the aggregates and combines them
    /// individually. If your aggregation supports SIMD, then implement the function accordingly.
    #[inline]
    fn combine_slice(slice: &[Self::PartialAggregate]) -> Option<Self::PartialAggregate> {
        slice.iter().fold(None, |accumulator, &item| {
            Some(match accumulator {
                Some(acc) => Self::combine(acc, item),
                None => item,
            })
        })
    }

    /// Merges two slices of partial aggregates together
    ///
    /// A default implementation is provided that iterates over the aggregates and merges them
    /// individually. If your aggregation supports SIMD, then implement the function accordingly.
    #[inline]
    fn merge_slices(s1: &mut [Self::PartialAggregate], s2: &[Self::PartialAggregate]) {
        // NOTE: merges at most s2.len() aggregates
        for (self_slot, other_slot) in s1.iter_mut().zip(s2.iter()).take(s2.len()) {
            *self_slot = Self::combine(*self_slot, *other_slot);
        }
    }

    /// Builds a prefix-sum vec given slice of partial aggregates
    ///
    /// Only used for aggregation functions that support range queries using prefix-sum
    #[inline]
    fn build_prefix(slice: &[Self::PartialAggregate]) -> Vec<Self::PartialAggregate> {
        slice
            .iter()
            .scan(Self::IDENTITY, |pa, &i| {
                *pa = Self::combine(*pa, i);
                Some(*pa)
            })
            .collect::<Vec<_>>()
    }

    /// Answers a range query in O(1) using a prefix-sum slice
    ///
    /// If the aggregator does not support prefix range query then it returns `None`
    #[inline]
    fn prefix_query(
        _slice: &[Self::PartialAggregate],
        _start: usize,
        _end: usize,
    ) -> Option<Self::PartialAggregate> {
        None
    }

    /// User-defined compression function of partial aggregates
    fn compress(_data: &[Self::PartialAggregate]) -> Option<Vec<u8>> {
        None
    }

    /// User-defined decompress function of partial aggregates
    fn decompress(_bytes: &[u8]) -> Option<Vec<Self::PartialAggregate>> {
        None
    }
}

/// Extension trait for inverse combine operations
pub trait InverseExt: Aggregator {
    /// Inverse combine two partial aggregates to a new partial aggregate
    fn inverse_combine(
        a: Self::PartialAggregate,
        b: Self::PartialAggregate,
    ) -> Self::PartialAggregate;
}
#[cfg(not(feature = "serde"))]
/// Bounds for Aggregator Input
pub trait InputBounds: Debug + Clone + Copy + Send {}
#[cfg(feature = "serde")]
/// Bounds for Aggregator Input
pub trait InputBounds:
    Debug + Clone + Copy + Send + serde::Serialize + for<'a> serde::Deserialize<'a> + 'static
{
}

#[cfg(not(feature = "serde"))]
impl<T> InputBounds for T where T: Debug + Clone + Copy + Send {}

#[cfg(feature = "serde")]
impl<T> InputBounds for T where
    T: Debug + Clone + Copy + Send + serde::Serialize + for<'a> serde::Deserialize<'a> + 'static
{
}

/// A mutable aggregate type
#[cfg(not(feature = "serde"))]
pub trait MutablePartialAggregateType: Clone {}
/// A mutable aggregate type
#[cfg(feature = "serde")]
pub trait MutablePartialAggregateType:
    Clone + serde::Serialize + for<'a> serde::Deserialize<'a>
{
}

#[cfg(not(feature = "serde"))]
impl<T> MutablePartialAggregateType for T where T: Clone {}

#[cfg(feature = "serde")]
impl<T> MutablePartialAggregateType for T where
    T: Clone + serde::Serialize + for<'a> serde::Deserialize<'a> + 'static
{
}

/// Trait bounds for a partial aggregate type
#[cfg(not(feature = "serde"))]
pub trait PartialAggregateBounds:
    Default + Debug + Clone + Copy + Send + AsBytes + FromBytes
{
}

/// Trait bounds for a partial aggregate type
#[cfg(feature = "serde")]
pub trait PartialAggregateBounds:
    Default
    + Debug
    + Clone
    + Copy
    + Send
    + AsBytes
    + FromBytes
    + serde::Serialize
    + for<'a> serde::Deserialize<'a>
{
}

#[cfg(not(feature = "serde"))]
impl<T> PartialAggregateBounds for T where
    T: Default + Debug + Clone + Copy + Send + AsBytes + FromBytes
{
}

#[cfg(feature = "serde")]
impl<T> PartialAggregateBounds for T where
    T: Default
        + Debug
        + Clone
        + Copy
        + Send
        + AsBytes
        + FromBytes
        + serde::Serialize
        + for<'a> serde::Deserialize<'a>
        + 'static
{
}

/// An immutable aggregate type
pub trait PartialAggregateType: PartialAggregateBounds {
    /// Type denoting its representation as bytes.
    /// This is `[u8; N]` where `N = size_of::<T>`.
    type Bytes: AsRef<[u8]>
        + core::ops::Index<usize, Output = u8>
        + core::ops::IndexMut<usize, Output = u8>
        + for<'a> TryFrom<&'a [u8]>
        + core::fmt::Debug
        + Default;

    /// To bytes in little endian
    fn to_le_bytes(&self) -> Self::Bytes;

    /// From bytes in little endian
    fn from_le_bytes(bytes: Self::Bytes) -> Self;

    // /// To bytes in big endian
    // fn to_be_bytes(&self) -> Self::Bytes;

    // /// From bytes in big endian
    // fn from_be_bytes(bytes: Self::Bytes) -> Self;
}

macro_rules! primitive_partial {
    ($type:ty) => {
        impl PartialAggregateType for $type {
            type Bytes = [u8; core::mem::size_of::<Self>()];
            #[inline]
            fn to_le_bytes(&self) -> Self::Bytes {
                Self::to_le_bytes(*self)
            }

            #[inline]
            fn from_le_bytes(bytes: Self::Bytes) -> Self {
                Self::from_le_bytes(bytes)
            }

            // #[inline]
            // fn from_be_bytes(bytes: Self::Bytes) -> Self {
            //     Self::from_be_bytes(bytes)
            // }
            // #[inline]
            // fn to_be_bytes(&self) -> Self::Bytes {
            //     Self::to_be_bytes(*self)
            // }
            // }
        }
    };
}

primitive_partial!(u8);
primitive_partial!(u16);
primitive_partial!(u32);
primitive_partial!(u64);
primitive_partial!(i8);
primitive_partial!(i16);
primitive_partial!(i32);
primitive_partial!(i64);
primitive_partial!(f32);
primitive_partial!(f64);
primitive_partial!(i128);
primitive_partial!(u128);

// macro_rules! tuple_partial {
//     ($t1:ty, $t2:ty) => {
//         impl PartialAggregateType for ($t1, $t2) {
//             type Bytes = [u8; core::mem::size_of::<Self>()];
//             #[inline]
//             fn to_le_bytes(&self) -> Self::Bytes {
//                 // Self::to_le_bytes(*self)
//                 unimplemented!();
//             }

//             #[inline]
//             fn from_le_bytes(_bytes: Self::Bytes) -> Self {
//                 // Self::from_le_bytes(bytes)
//                 unimplemented!();
//             }

//             // #[inline]
//             // fn from_be_bytes(bytes: Self::Bytes) -> Self {
//             //     Self::from_be_bytes(bytes)
//             // }
//             // #[inline]
//             // fn to_be_bytes(&self) -> Self::Bytes {
//             //     Self::to_be_bytes(*self)
//             // }
//             // }
//         }
//     };
// }

// tuple_partial!(u16, u16);
// tuple_partial!(u32, u32);
// tuple_partial!(u64, u64);
// tuple_partial!(f32, f32);
// tuple_partial!(f64, f64);
// tuple_partial!(i16, i16);
// tuple_partial!(i32, i32);
// tuple_partial!(i64, i64);
// tuple_partial!(i128, i128);

// macro_rules! tuple_partial {
//     ( $( $name:ident )+ ) => {
//         impl<$($name: PartialAggregateType),+> PartialAggregateType for ($($name,)+)
//         {
//         }
//     };
// }

// tuple_partial!(A);
// tuple_partial!(A B);
// tuple_partial!(A B C);
// tuple_partial!(A B C D);
// tuple_partial!(A B C D E);
// tuple_partial!(A B C D E F);
// tuple_partial!(A B C D E F G);
// tuple_partial!(A B C D E F G H);
// tuple_partial!(A B C D E F G H I);
// tuple_partial!(A B C D E F G H I J);
// tuple_partial!(A B C D E F G H I J K);
// tuple_partial!(A B C D E F G H I J K L);
