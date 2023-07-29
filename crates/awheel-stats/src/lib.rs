use core::cell::RefCell;
use minstant::Instant;
use sketches_ddsketch::{Config, DDSketch};
use std::rc::Rc;

pub fn sketch_percentiles(sketch: &DDSketch) -> Percentiles {
    Percentiles {
        count: sketch.count(),
        min: sketch.min().unwrap_or(0.0),
        p50: sketch.quantile(0.5).unwrap().unwrap_or(0.0),
        p99: sketch.quantile(0.99).unwrap().unwrap_or(0.0),
        p99_9: sketch.quantile(0.999).unwrap().unwrap_or(0.0),
        p99_99: sketch.quantile(0.9999).unwrap().unwrap_or(0.0),
        p99_999: sketch.quantile(0.99999).unwrap().unwrap_or(0.0),
        max: sketch.max().unwrap_or(0.0),
    }
}

#[derive(Default, Clone, Copy)]
pub struct Percentiles {
    count: usize,
    min: f64,
    p50: f64,
    p99: f64,
    p99_9: f64,
    p99_99: f64,
    p99_999: f64,
    max: f64,
}

impl std::fmt::Debug for Percentiles {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Percentiles")
            .field("count", &self.count)
            .field("min", &format_args!("{:.2}ns", self.min))
            .field("p50", &format_args!("{:.2}ns", self.p50))
            .field("p99", &format_args!("{:.2}ns", self.p99))
            .field("p99.9", &format_args!("{:.2}ns", self.p99_9))
            .field("p99.99", &format_args!("{:.2}ns", self.p99_99))
            .field("p99.999", &format_args!("{:.2}ns", self.p99_999))
            .field("max", &format_args!("{:.2}ns", self.max))
            .finish()
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone)]
pub struct Sketch {
    inner: Rc<RefCell<DDSketch>>,
}
impl Default for Sketch {
    fn default() -> Self {
        Self {
            inner: Rc::new(RefCell::new(DDSketch::new(Config::new(0.01, 2048, 1.0e-9)))),
        }
    }
}
impl Sketch {
    #[inline]
    pub fn add(&self, data: f64) {
        self.inner.borrow_mut().add(data)
    }
    pub fn percentiles(&self) -> Percentiles {
        sketch_percentiles(&self.inner.borrow())
    }
}

// Inspired by https://github.com/spacejam/sled/blob/main/src/metrics.rs
pub struct Measure {
    start: Instant,
    sketch: Sketch,
}

impl Measure {
    #[inline]
    #[allow(unused_variables)]
    pub fn new(sketch: &Sketch) -> Measure {
        Measure {
            sketch: sketch.clone(), // clones Rc
            start: Instant::now(),
        }
    }
}

impl Drop for Measure {
    #[inline]
    fn drop(&mut self) {
        self.sketch.add(self.start.elapsed().as_nanos() as f64);
    }
}
