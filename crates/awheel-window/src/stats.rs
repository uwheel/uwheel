use awheel_stats::Sketch;
use core::{cell::Cell, fmt};

/// Window Stats
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Default)]
pub struct Stats {
    /// An integer representing the size of the data structure in bytes
    pub size_bytes: Cell<usize>,
    /// A sketch for recording latencies of window computations
    pub window_computation_ns: Sketch,
    /// A sketch for recording latencies of cleaning up window state after a computation
    pub cleanup_ns: Sketch,
    /// A sketch for recording latencies of advancing time
    pub advance_ns: Sketch,
    /// A sketch for recording latencies of inserting records
    pub insert_ns: Sketch,
}

impl std::fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Stats")
            .field("size_bytes", &self.size_bytes.get())
            .field(
                "window_computation_ns",
                &self.window_computation_ns.percentiles(),
            )
            .field("cleanup_ns", &self.cleanup_ns.percentiles())
            .field("advance_ns", &self.advance_ns.percentiles())
            .field("insert_ns", &self.insert_ns.percentiles())
            .finish()
    }
}
