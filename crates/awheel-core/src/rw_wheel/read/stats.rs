use awheel_stats::Sketch;
use core::fmt;

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
}

impl core::fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("HAW Stats")
            .field("tick", &self.tick.percentiles())
            .field("interval", &self.interval.percentiles())
            .field("landmark", &self.landmark.percentiles())
            .finish()
    }
}
