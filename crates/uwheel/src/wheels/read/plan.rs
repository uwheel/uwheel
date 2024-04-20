use core::cmp::Ordering;

use super::hierarchical::{Granularity, WheelRange};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "smallvec")]
pub(crate) type WheelRanges = smallvec::SmallVec<[WheelRange; 8]>;
#[cfg(not(feature = "smallvec"))]
pub(crate) type WheelRanges = Vec<WheelRange>;

#[cfg(feature = "smallvec")]
pub(crate) type WheelAggregations = smallvec::SmallVec<[WheelAggregation; 8]>;
#[cfg(not(feature = "smallvec"))]
pub(crate) type WheelAggregations = Vec<WheelAggregation>;

/// Execution Plan Variants
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ExecutionPlan {
    /// Execution consisting of a single Wheel Aggregation
    WheelAggregation(WheelAggregation),
    /// Execution consisting of multiple Wheel Aggregations
    CombinedAggregation(CombinedAggregation),
    /// Execution can be queried through a landmark window
    LandmarkAggregation,
    /// Execution can be queried through a combination of landmark window + inverse combine
    InverseLandmarkAggregation(WheelAggregations),
}

impl ExecutionPlan {
    /// Returns `true``if execution plan is a single-wheel prefix sum or landmark`
    #[inline]
    pub fn is_prefix_or_landmark(&self) -> bool {
        match self {
            ExecutionPlan::WheelAggregation(w) => w.is_prefix(),
            ExecutionPlan::LandmarkAggregation => true,
            _ => false,
        }
    }
    /// Returns the expected aggregate cost |⊕| of the plan
    pub fn cost(&self) -> usize {
        match self {
            ExecutionPlan::WheelAggregation(w) => w.cost(),
            ExecutionPlan::CombinedAggregation(c) => c.cost(),
            ExecutionPlan::LandmarkAggregation => 6,
            ExecutionPlan::InverseLandmarkAggregation(w) => {
                w.iter().map(|m| m.cost()).sum::<usize>() + 6 + 2
            }
        }
    }
}

impl Ord for ExecutionPlan {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cost().cmp(&other.cost())
    }
}

impl PartialOrd for ExecutionPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WheelAggregation {
    pub(crate) range: WheelRange,
    pub(crate) plan: Aggregation,
    pub(crate) slots: (usize, usize),
    pub(crate) granularity: Granularity,
}

impl WheelAggregation {
    pub(crate) fn new(
        range: WheelRange,
        plan: Aggregation,
        slots: (usize, usize),
        granularity: Granularity,
    ) -> Self {
        Self {
            range,
            plan,
            slots,
            granularity,
        }
    }
    pub fn cost(&self) -> usize {
        self.plan.cost()
    }
    #[inline]
    pub fn is_prefix(&self) -> bool {
        matches!(self.plan, Aggregation::Prefix)
    }
    #[inline]
    pub fn is_scan(&self) -> bool {
        matches!(self.plan, Aggregation::Scan(_))
    }

    pub fn range(&self) -> &WheelRange {
        &self.range
    }
}

/// Aggregation method
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    /// A scan-based wheel aggregation that needs to reduce N slots
    Scan(usize),
    /// A prefix-based wheel aggregation that utilises prefix-sum for performing range-sum queries
    Prefix,
}

impl Aggregation {
    /// Returns the cost of executing the Wheel Aggregation
    pub fn cost(&self) -> usize {
        match self {
            Aggregation::Scan(slots) => *slots,
            Aggregation::Prefix => 1,
        }
    }
}

/// A Combined Aggregation Execution plan consisting of multiple Wheel Aggregations
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CombinedAggregation {
    pub(crate) aggregations: WheelAggregations,
}

impl From<WheelAggregations> for CombinedAggregation {
    fn from(aggregations: WheelAggregations) -> Self {
        Self { aggregations }
    }
}

impl CombinedAggregation {
    /// Returns the cost of executing the Combined Aggregation
    pub fn cost(&self) -> usize {
        let cost = self.aggregations.iter().fold(0, |mut acc, w_agg| {
            acc += w_agg.cost();
            acc
        });

        cost + self.aggregations.len() // include ops required to reduce the inner wheel aggregations
    }
}
