use core::fmt::Debug;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// An All Aggregator enabling the following functions (MAX, MIN, SUM, COUNT, AVG).
#[cfg(feature = "all")]
pub mod all;
/// Incremental AVG aggregation
#[cfg(feature = "avg")]
pub mod avg;
/// Incremental MAX aggregation
#[cfg(feature = "max")]
pub mod max;
/// Incremental MIN aggregation
#[cfg(feature = "min")]
pub mod min;
/// Incremental SUM aggregation
#[cfg(feature = "sum")]
pub mod sum;

#[cfg(feature = "top_n")]
/// Top-N Aggregation using a nested Aggregator which has a PartialAggregate that implements `Ord`
pub mod top_n;

/// Type alias for a Combine Simd function
pub type CombineSimdFn<P> = fn(&[P]) -> P;
/// Type alias for an Inverse Combine function.
pub type InverseFn<P> = fn(P, P) -> P;

/// Aggregation interface that library users must implement to use µWheel
///
/// µWheel provides a bunch of pre-defined aggregator implementations including:
/// - [sum]
/// - [min]
/// - [max]
/// - [avg]
/// - [all]
///
/// # Example
///
/// Here is a simple example showing how to create a SUM aggregator using u32.
/// ```
/// use uwheel::Aggregator;
///
/// #[derive(Default, Debug, Clone)]
/// struct MySumAggregator;
///
/// impl Aggregator for MySumAggregator {
///     const IDENTITY: Self::PartialAggregate = 0u32;
///     type Input = u32;
///     type MutablePartialAggregate = u32;
///     type Aggregate = u32;
///     type PartialAggregate = u32;
///
///     fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
///        input.into()
///     }
///
///     fn combine_mutable(a: &mut Self::MutablePartialAggregate, input: Self::Input) {
///        *a += input;
///     }
///
///     fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
///        a.into()
///     }
///
///     fn combine(a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
///        a + b
///     }
///
///     fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
///        a
///     }
/// }
/// ```
pub trait Aggregator: Default + Debug + Clone + 'static {
    /// Identity value for [Self::PartialAggregate].
    ///
    /// For example, for SUM types the identity value should be set to 0.
    const IDENTITY: Self::PartialAggregate;

    /// Aggregator Input type that can be converted or applied to a [Self::MutablePartialAggregate].
    type Input: InputBounds;

    /// Mutable Partial Aggregate type that can be mutated above µWheel's low watermark.
    type MutablePartialAggregate: MutablePartialAggregateType;

    /// Immutable Partial Aggregate type that defines aggregates below µWheel's low watermark.
    type PartialAggregate: PartialAggregateType;

    /// Final Aggregate type that can be lowered from a [Self::PartialAggregate].
    ///
    /// In many cases the [Self::PartialAggregate] type will be the same as [Self::Aggregate].
    /// An instance where it is not is an AVG function where the partial aggregate consists of a (sum, count)
    /// tuple and the final aggregate "average" is calculated through sum/count.
    type Aggregate: Debug + Send;

    /// Lifts [Self::Input] into a [Self::MutablePartialAggregate]
    fn lift(input: Self::Input) -> Self::MutablePartialAggregate;

    /// Combines [Self::Input] to an existing `&mut Self::MutablePartialAggregate`.
    fn combine_mutable(a: &mut Self::MutablePartialAggregate, input: Self::Input);

    /// Freezes a [Self::MutablePartialAggregate] into a [Self::PartialAggregate].
    fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate;

    /// Combine two partial aggregates and produces a new [Self::PartialAggregate].
    fn combine(a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate;

    /// Lowers a [Self::PartialAggregate] into a final [Self::Aggregate].
    fn lower(a: Self::PartialAggregate) -> Self::Aggregate;

    /// Combines a slice of partial aggregates into a new partial
    ///
    /// A default implementation is provided that iterates over the aggregates and combines them
    /// individually. If your aggregation supports SIMD, then implement the function accordingly.
    #[inline]
    #[doc(hidden)]
    fn combine_slice(slice: &[Self::PartialAggregate]) -> Option<Self::PartialAggregate> {
        match Self::combine_simd() {
            Some(combine_simd) => Some(combine_simd(slice)),
            None => Some(slice.iter().copied().fold(Self::IDENTITY, Self::combine)),
        }
    }

    /// Merges two slices of partial aggregates together
    ///
    /// A default implementation is provided that iterates over the aggregates and merges them
    /// individually. If your aggregation supports SIMD, then implement the function accordingly.
    #[inline]
    #[doc(hidden)]
    fn merge(s1: &mut [Self::PartialAggregate], s2: &[Self::PartialAggregate]) {
        // NOTE: merges at most s2.len() aggregates
        for (self_slot, other_slot) in s1.iter_mut().zip(s2.iter()).take(s2.len()) {
            *self_slot = Self::combine(*self_slot, *other_slot);
        }
    }

    /// Builds a prefix-sum vec given slice of partial aggregates
    ///
    /// Only used for aggregation functions that support range queries using prefix-sum
    #[doc(hidden)]
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
    #[doc(hidden)]
    #[inline]
    fn prefix_query(
        slice: &[Self::PartialAggregate],
        start: usize,
        end: usize,
    ) -> Option<Self::PartialAggregate> {
        Self::combine_inverse().map(|inverse| {
            if start == 0 {
                slice[end]
            } else {
                inverse(slice[end], slice[start - 1])
            }
        })
    }

    /// Returns a function that inverse combines two partial aggregates
    ///
    /// Is set to `None` by default
    fn combine_inverse() -> Option<InverseFn<Self::PartialAggregate>> {
        None
    }

    /// Returns a function that combines a slice of partial aggregates using explicit SIMD instructions.
    ///
    /// Is set to `None` by default
    fn combine_simd() -> Option<CombineSimdFn<Self::PartialAggregate>> {
        None
    }

    /// Optional compression support for partial aggregates
    ///
    /// Is set to `None` by default
    fn compression() -> Option<Compression<Self::PartialAggregate>> {
        None
    }

    /// Returns ``true`` if the Aggregator supports invertibility
    #[doc(hidden)]
    fn invertible() -> bool {
        Self::combine_inverse().is_some()
    }

    /// Returns ``true`` if the aggregator supports SIMD
    #[doc(hidden)]
    fn simd_support() -> bool {
        Self::combine_simd().is_some()
    }

    #[doc(hidden)]
    fn compression_support() -> bool {
        Self::compression().is_some()
    }
}

/// Defines how partial aggregates are to be compressed and decompressed
#[allow(dead_code)]
pub struct Compression<T> {
    pub(crate) compressor: Compressor<T>,
    pub(crate) decompressor: Decompressor<T>,
}

impl<T> Compression<T> {
    /// Creates a new Compression object
    pub fn new(compressor: Compressor<T>, decompressor: Decompressor<T>) -> Self {
        Self {
            compressor,
            decompressor,
        }
    }
}

/// Alias for a Compression function
pub type Compressor<T> = fn(&[T]) -> Vec<u8>;
/// Alias for a Decompression function
pub type Decompressor<T> = fn(&[u8]) -> Vec<T>;

/// Bounds for Aggregator Input
pub trait InputBounds: Debug + Clone + Copy + Send {}
impl<T> InputBounds for T where T: Debug + Clone + Copy + Send {}

/// A mutable partial aggregate type
#[cfg(not(feature = "serde"))]
pub trait MutablePartialAggregateType: Clone {}
/// A mutable parital aggregate type
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
pub trait PartialAggregateBounds: Default + Debug + Clone + Copy + Send {}

/// Trait bounds for a partial aggregate type
#[cfg(feature = "serde")]
pub trait PartialAggregateBounds:
    Default + Debug + Clone + Copy + Send + serde::Serialize + for<'a> serde::Deserialize<'a>
{
}

#[cfg(not(feature = "serde"))]
impl<T> PartialAggregateBounds for T where T: Default + Debug + Clone + Copy + Send {}

#[cfg(feature = "serde")]
impl<T> PartialAggregateBounds for T where
    T: Default + Debug + Clone + Copy + Send + serde::Serialize + for<'a> serde::Deserialize<'a>
{
}

/// An immutable partial aggregate type
pub trait PartialAggregateType: PartialAggregateBounds {}

macro_rules! primitive_partial {
    ($type:ty) => {
        impl PartialAggregateType for $type {}
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

macro_rules! tuple_partial {
    ( $( $name:ident )+ ) => {
        impl<$($name: PartialAggregateType),+> PartialAggregateType for ($($name,)+)
        {
        }
    };
}

tuple_partial!(A B);
tuple_partial!(A B C);
tuple_partial!(A B C D);
tuple_partial!(A B C D E);
tuple_partial!(A B C D E F);
