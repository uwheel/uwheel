use awheel_core::stats::Sketch;
use core::fmt;

#[derive(Default)]
pub struct Stats {
    pub window_computation_ns: Sketch,
    pub cleanup_ns: Sketch,
    pub advance_ns: Sketch,
    pub insert_ns: Sketch,
}

impl std::fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Stats")
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
