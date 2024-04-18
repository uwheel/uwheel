use core::fmt;
use uwheel_stats::Sketch;

/// Stats for [Haw]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Default)]
pub struct Stats {
    /// A sketch for recording latencies of a tick
    pub tick: Sketch,
    /// A sketch for recording latencies of interval queries
    pub interval: Sketch,
    /// A sketch for recording latencies of landmark queries
    pub landmark: Sketch,
    /// A sketch for recording latencies of combine range queries
    pub combine_range: Sketch,
    /// A sketch for recording latencies of generating combine range plans
    pub combine_range_plan: Sketch,
    /// A sketch for recording latencies of generating the execution plan
    pub exec_plan: Sketch,
    /// A sketch for recording latencies of generating combine aggregation plan
    pub combined_aggregation_plan: Sketch,
    /// A sketch for recording latencies of executing combined aggregation
    pub combined_aggregation: Sketch,
    /// A sketch for recording latencies of executing a wheel aggregation
    pub wheel_aggregation: Sketch,
    /// A sketch for recording latencies of executing inverse landmark aggregations
    pub inverse_landmark: Sketch,
}

impl core::fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("HAW Stats")
            .field("tick", &self.tick.percentiles())
            .field("interval", &self.interval.percentiles())
            .field("landmark", &self.landmark.percentiles())
            .field("combine_range", &self.combine_range.percentiles())
            .field("exec_plan", &self.exec_plan.percentiles())
            .field("combine_range_plan", &self.combine_range_plan.percentiles())
            .field(
                "combined_aggregation_plan",
                &self.combined_aggregation_plan.percentiles(),
            )
            .field(
                "combined_aggregation",
                &self.combined_aggregation.percentiles(),
            )
            .field("wheel_aggregation", &self.wheel_aggregation.percentiles())
            .field("inverse_landmark", &self.inverse_landmark.percentiles())
            .finish()
    }
}
