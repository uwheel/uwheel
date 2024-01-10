use super::hierarchical::WheelRange;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Contains the Execution Plan of a HAW combine range query
#[derive(Debug, Clone)]
pub enum ExecutionPlan {
    /// Execution consisting of a single Wheel Aggregation
    Single(WheelAggregation),
    /// Execution consisting of multiple Wheel Aggregations
    Combined(CombinedAggregation),
}

impl ExecutionPlan {
    /// Returns the cost of executing the plan
    pub fn cost(&self) -> usize {
        match self {
            ExecutionPlan::Single(w) => w.cost(),
            ExecutionPlan::Combined(c) => c.cost(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
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
    pub fn range(&self) -> &WheelRange {
        &self.range
    }
}

/// Aggregation method
#[derive(Debug, Clone, Copy)]
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
#[derive(Debug, Default, Clone)]
pub struct CombinedAggregation {
    pub(crate) aggregations: Vec<WheelAggregation>,
}

impl CombinedAggregation {
    pub(crate) fn push(&mut self, agg: WheelAggregation) {
        self.aggregations.push(agg);
    }
    /// Returns the cost of executing the Combined Aggregation
    pub fn cost(&self) -> usize {
        let cost = self.aggregations.iter().fold(0, |mut acc, w_agg| {
            acc += w_agg.cost();
            acc
        });

        cost + self.aggregations.len() // include ops required to reduce the inner wheel aggregations
    }
}
