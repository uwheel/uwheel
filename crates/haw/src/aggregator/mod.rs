use core::{
    default::Default,
    fmt::Debug,
    marker::{Copy, Send},
};

/// An All Aggregator enabling the following functions (MAX, MIN, SUM, COUNT, AVG).
pub mod all;
/// Incremental AVG aggregation
pub mod avg;
/// Incremental MAX aggregation
pub mod max;
/// Incremental MIN aggregation
pub mod min;
/// Incremental SUM aggregation
pub mod sum;

#[cfg(feature = "top_k")]
/// Top-K Aggregation using a nested Aggregator which has a PartialAggregate that implements `Ord`
pub mod top_k;

pub use all::{AggState, AllAggregator};
pub use sum::*;

/// Aggregation interface that library users must implement to use Hierarchical Aggregation Wheels
pub trait Aggregator: Default + Debug + 'static {
    /// Input type that can be inserted into [Self::MutablePartialAggregate]
    type Input: Debug + Copy + Send;

    /// Mutable Partial Aggregate type
    type MutablePartialAggregate: Debug + Clone;

    /// Partial Aggregate type
    type PartialAggregate: PartialAggregateType;

    /// Final Aggregate type
    type Aggregate: Send;

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
}

/// Extension trait for inverse combine operations
pub trait InverseExt: Aggregator {
    /// Inverse combine two partial aggregates to a new partial aggregate
    fn inverse_combine(
        a: Self::PartialAggregate,
        b: Self::PartialAggregate,
    ) -> Self::PartialAggregate;
}

/// A Partial Aggregate Type which is used by an Aggregator
pub trait PartialAggregateType: Default + Debug + Clone + Copy + Send {}

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

tuple_partial!(A);
tuple_partial!(A B);
tuple_partial!(A B C);
tuple_partial!(A B C D);
tuple_partial!(A B C D E);
tuple_partial!(A B C D E F);
tuple_partial!(A B C D E F G);
tuple_partial!(A B C D E F G H);
tuple_partial!(A B C D E F G H I);
tuple_partial!(A B C D E F G H I J);
tuple_partial!(A B C D E F G H I J K);
tuple_partial!(A B C D E F G H I J K L);
