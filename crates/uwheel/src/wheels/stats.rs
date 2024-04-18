use core::fmt;
use uwheel_stats::Sketch;

/// Top-level Stats for an [RwWheel]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Default)]
pub struct Stats {
    /// A sketch for recording latencies of advancing time
    pub advance: Sketch,
    /// A sketch for recording latencies of scheduling overflow entries
    pub overflow_schedule: Sketch,
    /// A sketch for recording latencies of inserting records
    pub insert: Sketch,
}

impl core::fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RwWheel Stats")
            .field("advance", &self.advance.percentiles())
            .field("overflow schedule", &self.overflow_schedule.percentiles())
            .field("insert", &self.insert.percentiles())
            .finish()
    }
}
