use awheel_stats::Sketch;
use core::{cell::Cell, fmt};

/// Window Stats
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Default)]
pub struct Stats {
    pub window_combines: Cell<usize>,
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

impl Stats {
    pub fn merge_sketches(&self, other: Self) {
        self.window_computation_ns
            .merge(other.window_computation_ns);
        self.cleanup_ns.merge(other.cleanup_ns);
        self.advance_ns.merge(other.advance_ns);
        self.insert_ns.merge(other.insert_ns);
    }
    /// Returns the average number of aggregate combine operations per window
    pub fn avg_window_combines(&self) -> usize {
        self.window_combines.get() / self.window_computation_ns.count()
    }
    /// Returns the average number of inserts per window
    pub fn avg_window_inserts(&self) -> usize {
        self.insert_ns.count() / self.window_computation_ns.count()
    }
}

impl std::fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Stats")
            .field(
                "avg combines",
                &(self.window_combines.get() / self.window_computation_ns.percentiles().count),
            )
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
