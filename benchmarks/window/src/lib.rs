#[allow(dead_code)]
pub mod btreemap_wheel;
pub mod external_impls;
pub mod timestamp_generator;
pub mod tree;

use std::fs::File;

use awheel::{stats::Percentiles, time::Duration, window::stats::Stats};
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
        fn memory_usage(&self) -> usize;
        fn combine_operations(&self) -> usize;
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
        fn memory_usage(&self) -> usize;
        fn combine_operations(&self) -> usize;
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
        fn memory_usage(&self) -> usize;
        fn combine_operations(&self) -> usize;
    }
}

pub struct Run {
    pub id: String,
    pub total_insertions: u64,
    pub runtime: std::time::Duration,
    pub stats: Stats,
    pub qps: Option<f64>,
}

impl Run {
    pub fn to_stats(&self) -> SystemStats {
        let (advance_distribution, insert_distribution, query_distribution) = self.percetanges();
        SystemStats {
            id: self.id.clone(),
            throughput: self.throughput(),
            runtime: self.runtime.as_secs_f64(),
            memory_usage_bytes: self.stats.size_bytes.get() as u64,
            avg_window_combines: self.stats.avg_window_combines() as u64,
            avg_window_inserts: self.stats.avg_window_inserts() as u64,
            insert_latency: Latency::from(&self.stats.insert_ns.percentiles()),
            query_latency: Latency::from(&self.stats.window_computation_ns.percentiles()),
            advance_latency: Latency::from(&self.stats.advance_ns.percentiles()),
            insert_distribution,
            query_distribution,
            advance_distribution,
        }
    }
    pub fn throughput(&self) -> f64 {
        (self.total_insertions as f64 / self.runtime.as_secs_f64()) / 1_000_000.0
    }
    pub fn percetanges(&self) -> (f64, f64, f64) {
        let inserts = self.stats.insert_ns.percentiles().count as f64;
        let advances = self.stats.advance_ns.percentiles().count as f64;
        let queries = self.stats.window_computation_ns.percentiles().count as f64;

        let total = inserts + advances + queries;

        let adv_percentage: f64 = (advances / total) * 100.0;
        let insert_percentage: f64 = (inserts / total) * 100.0;
        let query_percentage: f64 = (queries / total) * 100.0;

        (adv_percentage, insert_percentage, query_percentage)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Window {
    pub range: Duration,
    pub slide: Duration,
}
impl Window {
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

#[derive(Debug, serde::Serialize)]
pub struct PlottingOutput {
    // e.g., DEBS12
    pub id: String,
    watermark_freq: usize,
    runs: Vec<Execution>,
}

impl PlottingOutput {
    pub fn flush_to_file(&self) -> serde_json::Result<()> {
        let path = format!("../results/{}.json", self.id);
        let file = File::create(path).unwrap();
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
}

impl PlottingOutput {
    pub fn from(id: &str, watermark_freq: usize, runs: Vec<BenchResult>) -> Self {
        Self {
            id: id.to_string(),
            watermark_freq,
            runs: runs.iter().map(Execution::from).collect::<Vec<_>>(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Execution {
    range: String,
    slide: String,
    systems: Vec<SystemStats>,
}

impl Execution {
    pub fn from(result: &BenchResult) -> Self {
        let window = result.window;
        let range = window.range.to_string();
        let slide = window.slide.to_string();
        Self {
            range,
            slide,
            systems: result.runs.iter().map(|r| r.to_stats()).collect::<Vec<_>>(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct SystemStats {
    id: String,
    throughput: f64,
    runtime: f64,
    memory_usage_bytes: u64,
    avg_window_combines: u64,
    avg_window_inserts: u64,
    insert_latency: Latency,
    query_latency: Latency,
    advance_latency: Latency,
    insert_distribution: f64,
    query_distribution: f64,
    advance_distribution: f64,
}

#[derive(Debug, serde::Serialize)]
pub struct Latency {
    count: usize,
    min: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    sum: f64,
}

impl Latency {
    pub fn from(percentiles: &Percentiles) -> Self {
        Self {
            count: percentiles.count,
            min: percentiles.min,
            p50: percentiles.p50,
            p95: percentiles.p95,
            p99: percentiles.p99,
            max: percentiles.max,
            sum: percentiles.sum,
        }
    }
}

#[allow(dead_code)]
pub struct BenchResult {
    pub window: Window,
    pub runs: Vec<Run>,
}
impl BenchResult {
    pub fn new(window: Window, runs: Vec<Run>) -> Self {
        Self { window, runs }
    }
    pub fn print(&self) {
        let throughput =
            |run: &Run| (run.total_insertions as f64 / run.runtime.as_secs_f64()) / 1_000_000.0;
        println!("{:#?}", self.window);
        for run in self.runs.iter() {
            if let Some(qps) = run.qps {
                println!("Throughput {} Mops/s with {} M/qps", throughput(run), qps);
            } else {
                println!("Throughput {} Mops/s", throughput(run));
            }

            let (adv, insert, query) = run.percetanges();
            println!(
                "Advance time {:.2}%, Insert time {:.2}%  Query time {:.2}%",
                adv, insert, query
            );
            println!("{} (took {:.2}s)", run.id, run.runtime.as_secs_f64(),);
            println!("{:#?}", run.stats);
        }
    }
    pub fn workload_distribution(&self) -> (f64, f64, f64) {
        self.runs.first().unwrap().percetanges()
    }
    pub fn runs(&self) -> &[Run] {
        &self.runs
    }
}

pub const SMALL_RANGE_WINDOWS: [Window; 5] = [
    Window::new(Duration::seconds(5), Duration::seconds(2)),
    Window::new(Duration::seconds(10), Duration::seconds(2)),
    Window::new(Duration::seconds(20), Duration::seconds(2)),
    Window::new(Duration::seconds(30), Duration::seconds(2)),
    Window::new(Duration::seconds(40), Duration::seconds(2)),
];

pub const BIG_RANGE_WINDOWS: [Window; 5] = [
    Window::new(Duration::seconds(30), Duration::seconds(2)),
    Window::new(Duration::minutes(1), Duration::seconds(2)),
    Window::new(Duration::minutes(15), Duration::seconds(2)),
    Window::new(Duration::minutes(30), Duration::seconds(2)),
    Window::new(Duration::hours(1), Duration::seconds(2)),
];
