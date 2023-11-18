use core::cell::RefCell;
use minstant::Instant;
use sketches_ddsketch::{Config, DDSketch};
use std::rc::Rc;

pub fn sketch_percentiles(sketch: &DDSketch) -> Percentiles {
    Percentiles {
        count: sketch.count(),
        min: sketch.min().unwrap_or(0.0),
        p25: sketch.quantile(0.25).unwrap().unwrap_or(0.0),
        p50: sketch.quantile(0.5).unwrap().unwrap_or(0.0),
        p75: sketch.quantile(0.75).unwrap().unwrap_or(0.0),
        p95: sketch.quantile(0.95).unwrap().unwrap_or(0.0),
        p99: sketch.quantile(0.99).unwrap().unwrap_or(0.0),
        p99_9: sketch.quantile(0.999).unwrap().unwrap_or(0.0),
        p99_99: sketch.quantile(0.9999).unwrap().unwrap_or(0.0),
        p99_999: sketch.quantile(0.99999).unwrap().unwrap_or(0.0),
        max: sketch.max().unwrap_or(0.0),
        sum: sketch.sum().unwrap_or(0.0),
    }
}

#[derive(Default, Clone, Copy)]
pub struct Percentiles {
    pub count: usize,
    pub min: f64,
    pub p25: f64,
    pub p50: f64,
    pub p75: f64,
    pub p95: f64,
    pub p99: f64,
    pub p99_9: f64,
    pub p99_99: f64,
    pub p99_999: f64,
    pub max: f64,
    pub sum: f64,
}

impl std::fmt::Debug for Percentiles {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Percentiles")
            .field("count", &self.count)
            .field("min", &format_args!("{:.2}ns", self.min))
            .field("p25", &format_args!("{:.2}ns", self.p25))
            .field("p50", &format_args!("{:.2}ns", self.p50))
            .field("p75", &format_args!("{:.2}ns", self.p75))
            .field("p95", &format_args!("{:.2}ns", self.p95))
            .field("p99", &format_args!("{:.2}ns", self.p99))
            .field("p99.9", &format_args!("{:.2}ns", self.p99_9))
            .field("p99.99", &format_args!("{:.2}ns", self.p99_99))
            .field("p99.999", &format_args!("{:.2}ns", self.p99_999))
            .field("max", &format_args!("{:.2}ns", self.max))
            .field("sum", &self.sum)
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
    pub fn count(&self) -> usize {
        self.inner.borrow().count()
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

/// Profile the current scope with the given [Sketch]
///
/// # Example
///
/// ```rust
/// use awheel_stats::{profile_scope, Sketch};
///
/// let my_sketch = Sketch::default();
/// {
///     profile_scope!(&my_sketch);
///     // do some work
/// }
/// ```
#[macro_export]
macro_rules! profile_scope {
    ($id:expr) => {
        let _measure_scope = $crate::Measure::new($id);
    };
}
