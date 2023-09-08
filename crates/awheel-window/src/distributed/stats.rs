use awheel_stats::Sketch;
use core::fmt;

/// Window Stats
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Default)]
pub struct Stats {
    /// A sketch for recording latencies of merging pairs across workers
    pub merge_ns: Sketch,
    /// A sketch for recording latencies of advancing time
    pub advance_ns: Sketch,
    /// A sketch for recording latencies of inserting
    pub insert_ns: Sketch,
}

impl std::fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Stats")
            .field("merge_ns", &self.merge_ns.percentiles())
            .field("advance_ns", &self.advance_ns.percentiles())
            .field("insert_ns", &self.insert_ns.percentiles())
            .finish()
    }
}
