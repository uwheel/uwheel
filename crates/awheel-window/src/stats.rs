use awheel_stats::Sketch;
use core::{cell::Cell, fmt};

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Default)]
pub struct Stats {
    pub size_bytes: Cell<usize>,
    pub window_computation_ns: Sketch,
    pub cleanup_ns: Sketch,
    pub advance_ns: Sketch,
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
