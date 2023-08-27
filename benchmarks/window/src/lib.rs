pub mod btreemap_wheel;
pub mod external_impls;
pub mod timestamp_generator;
pub mod tree;

use awheel::{time::Duration, window::stats::Stats};
pub use timestamp_generator::{align_to_closest_thousand, TimestampGenerator};

pub use cxx::UniquePtr;

#[cxx::bridge]
pub mod bfinger_two {
    unsafe extern "C++" {
        include!("window/include/FiBA.h");

        type FiBA_SUM;

        fn create_fiba_with_sum() -> UniquePtr<FiBA_SUM>;

        fn evict(self: Pin<&mut FiBA_SUM>);
        fn bulk_evict(self: Pin<&mut FiBA_SUM>, time: &u64);
        fn insert(self: Pin<&mut FiBA_SUM>, time: &u64, value: &u64);
        fn query(&self) -> u64;
        fn range(&self, time_from: u64, time_to: u64) -> u64;

        fn oldest(&self) -> u64;
        fn youngest(&self) -> u64;

        fn size(&self) -> usize;
    }
}

#[cxx::bridge]
pub mod bfinger_four {
    unsafe extern "C++" {
        include!("window/include/FiBA.h");

        type FiBA_SUM_4;

        fn create_fiba_4_with_sum() -> UniquePtr<FiBA_SUM_4>;

        fn bulk_evict(self: Pin<&mut FiBA_SUM_4>, time: &u64);
        fn evict(self: Pin<&mut FiBA_SUM_4>);
        fn insert(self: Pin<&mut FiBA_SUM_4>, time: &u64, value: &u64);
        fn query(&self) -> u64;
        fn range(&self, time_from: u64, time_to: u64) -> u64;

        fn oldest(&self) -> u64;
        fn youngest(&self) -> u64;

        fn size(&self) -> usize;
    }
}

#[cxx::bridge]
pub mod bfinger_eight {
    unsafe extern "C++" {
        include!("window/include/FiBA.h");

        type FiBA_SUM_8;

        fn create_fiba_8_with_sum() -> UniquePtr<FiBA_SUM_8>;

        fn evict(self: Pin<&mut FiBA_SUM_8>);
        fn bulk_evict(self: Pin<&mut FiBA_SUM_8>, time: &u64);
        fn insert(self: Pin<&mut FiBA_SUM_8>, time: &u64, value: &u64);
        fn query(&self) -> u64;
        fn range(&self, time_from: u64, time_to: u64) -> u64;

        fn oldest(&self) -> u64;
        fn youngest(&self) -> u64;

        fn size(&self) -> usize;
    }
}

pub struct Run {
    pub id: String,
    pub total_insertions: u64,
    pub runtime: std::time::Duration,
    pub stats: Stats,
    pub qps: Option<f64>,
}
#[derive(Debug)]
pub struct Execution {
    pub range: Duration,
    pub slide: Duration,
}
impl Execution {
    pub const fn new(range: Duration, slide: Duration) -> Self {
        Self { range, slide }
    }
    pub fn slide_ms(&self) -> u64 {
        self.slide.whole_milliseconds() as u64
    }
    pub fn range_ms(&self) -> u64 {
        self.range.whole_milliseconds() as u64
    }
    pub fn range_seconds(&self) -> u64 {
        self.range.whole_seconds() as u64
    }
}

#[allow(dead_code)]
pub struct BenchResult {
    pub execution: Execution,
    pub runs: Vec<Run>,
}
impl BenchResult {
    pub fn new(execution: Execution, runs: Vec<Run>) -> Self {
        Self { execution, runs }
    }
    pub fn print(&self) {
        let throughput =
            |run: &Run| (run.total_insertions as f64 / run.runtime.as_secs_f64()) / 1_000_000.0;
        println!("{:#?}", self.execution);
        for run in self.runs.iter() {
            if let Some(qps) = run.qps {
                println!("Throughput {} Mops/s with {} M/qps", throughput(run), qps);
            } else {
                println!("Throughput {} Mops/s", throughput(run));
            }
            println!("{} (took {:.2}s)", run.id, run.runtime.as_secs_f64(),);
            println!("{:#?}", run.stats);
        }
    }
}
