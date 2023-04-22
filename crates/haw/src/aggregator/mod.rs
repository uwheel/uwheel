/// An All Aggregator enabling the following functions (MAX, MIN, SUM, COUNT, AVG).
pub mod all;
/// Incremental Sum aggregation
pub mod sum;

pub use all::{AggState, AllAggregator};
pub use sum::*;

use core::{
    default::Default,
    fmt::Debug,
    marker::{Copy, Send},
};

use crate::types::PartialAggregateType;

/// Aggregation interface that library users must implement to use Hierarchical Aggregation Wheels
pub trait Aggregator: Default + Debug + 'static {
    /// Input type that can be `lifted` into a partial aggregate
    type Input: Debug + Copy + Send;
    /// Final Aggregate type
    type Aggregate: Send;
    /// Partial Aggregate type
    type PartialAggregate: PartialAggregateType;

    /// Convert the input entry to a partial aggregate
    fn lift(&self, input: Self::Input) -> Self::PartialAggregate;
    /// Combine two partial aggregates and produce new output
    fn combine(
        &self,
        a: Self::PartialAggregate,
        b: Self::PartialAggregate,
    ) -> Self::PartialAggregate;
    /// Convert a partial aggregate to a final result
    fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate;
}
