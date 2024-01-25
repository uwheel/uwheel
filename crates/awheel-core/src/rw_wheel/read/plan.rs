use super::hierarchical::WheelRange;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

/// Contains the Execution Plan of a HAW combine range query
#[derive(Debug, PartialEq, Clone)]
pub enum ExecutionPlan {
    /// Execution consisting of a single Wheel Aggregation
    Single(WheelAggregation),
    /// Execution consisting of multiple Wheel Aggregations
    Combined(CombinedAggregation),
    /// Execution can be queried through a landmark window
    Landmark,
    /// Execution can be queried through a combination of landmark window + inverse combine
    InverseLandmark(Box<ExecutionPlan>),
}

impl ExecutionPlan {
    /// Returns the cost of executing the plan
    pub fn cost(&self) -> usize {
        match self {
            ExecutionPlan::Single(w) => w.cost(),
            ExecutionPlan::Combined(c) => c.cost(),
            ExecutionPlan::Landmark => 6,
            ExecutionPlan::InverseLandmark(w) => w.cost() + 6 + 2, // 6 Wheels + 2 combine operations at end
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WheelAggregation {
    pub(crate) range: WheelRange,
    pub(crate) plan: Aggregation,
}

impl WheelAggregation {
    pub(crate) fn new(range: WheelRange, plan: Aggregation) -> Self {
        Self { range, plan }
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
#[derive(Debug, Clone, Copy, PartialEq)]
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

#[cfg(feature = "smallvec")]
pub(crate) type WheelAggregations = smallvec::SmallVec<[WheelAggregation; 8]>;
#[cfg(not(feature = "smallvec"))]
pub(crate) type WheelAggregations = Vec<WheelAggregation>;

/// A Combined Aggregation Execution plan consisting of multiple Wheel Aggregations
#[derive(Debug, Default, Clone, PartialEq)]
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
