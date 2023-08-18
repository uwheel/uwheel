pub mod fiba_wheel;
pub mod timestamp_generator;

use awheel::window::stats::Stats;
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
}
#[derive(Debug)]
pub struct Execution {
    pub range: u64,
    pub slide: u64,
}
impl Execution {
    pub const fn new(range: u64, slide: u64) -> Self {
        Self { range, slide }
    }
}

#[allow(dead_code)]
pub struct BenchResult {
    execution: Execution,
    runs: Vec<Run>,
}
impl BenchResult {
    pub fn new(execution: Execution, runs: Vec<Run>) -> Self {
        Self { execution, runs }
    }
    pub fn print(&self) {
        println!("{:#?}", self.execution);
        for run in self.runs.iter() {
            println!("{} (took {:.2}s)", run.id, run.runtime.as_secs_f64(),);
            println!("{:#?}", run.stats);
        }
    }
}
