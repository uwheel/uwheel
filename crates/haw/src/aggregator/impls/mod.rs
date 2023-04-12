/// An All Aggregator enabling the following functions (MAX, MIN, SUM, COUNT, AVG).
pub mod all;
/// Incremental Sum aggregation
pub mod sum;

pub use all::{AggState, AllAggregator};
pub use sum::*;
