/// An All Aggregator enabling the following functions (MAX, MIN, SUM, COUNT, AVG).
pub mod all;
/// Incremental Sum aggregation
pub mod sum;

pub mod top_k;

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
    /// Input type that can be inserted into [Self::Window]
    type Input: Debug + Copy + Send;

    /// Mutable Window type
    type Window: Debug + Clone;

    /// Partial Aggregate type
    type PartialAggregate: PartialAggregateType;

    /// Final Aggregate type
    type Aggregate: Send;

    /// Inserts an entry into the window
    fn insert(&self, window: &mut Self::Window, input: Self::Input);

    /// Initiates a new Window
    fn init_window(&self, input: Self::Input) -> Self::Window;

    /// lifts a Window into an immutable PartialAggregate
    fn lift(&self, window: Self::Window) -> Self::PartialAggregate;

    /// Combine two partial aggregates and produce new output
    fn combine(
        &self,
        a: Self::PartialAggregate,
        b: Self::PartialAggregate,
    ) -> Self::PartialAggregate;
    /// Convert a partial aggregate to a final result
    fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate;
}

/// An optional interface for supporting inverse operations
pub trait Inverse: Aggregator {
    fn inverse_combine(
        &self,
        a: Self::PartialAggregate,
        b: Self::PartialAggregate,
    ) -> Self::PartialAggregate;
}
