/// Aggregation Wheel based on a fixed-sized circular buffer
///
/// This is the core data structure that is reused between different hierarchies (e.g., seconds, minutes, hours, days)
pub mod aggregation;
/// Hierarchical Aggregation Wheel (HAW)
pub mod wheel;
/// Wheels that are tailored for Sliding Window Aggregation
pub mod window;
/// Write-ahead Wheel
pub mod write_ahead;

pub use aggregation::{AggregationWheel, MaybeWheel};
pub use wheel::Wheel;
