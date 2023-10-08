#[allow(dead_code)]
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

impl Run {
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

// default execution
pub const EXECUTIONS: [Execution; 5] = [
    Execution::new(Duration::seconds(30), Duration::seconds(2)),
    Execution::new(Duration::minutes(1), Duration::seconds(2)),
    Execution::new(Duration::minutes(15), Duration::seconds(2)),
    Execution::new(Duration::minutes(30), Duration::seconds(2)),
    Execution::new(Duration::hours(1), Duration::seconds(2)),
];

#[cfg(feature = "plot")]
pub fn plot_window(id: &str, results: &Vec<BenchResult>) {
    // TODO: fix
    //plot_workload_distribution(id, results);
    //plot_throughput_and_memory(id, results);
    plot_latencies(id, results);
}

/*
// plotpy does not support plt.bar ?
// might have to execute python code directly here instead...
#[cfg(feature = "plot")]
fn plot_workload_distribution(id: &str, results: &Vec<BenchResult>) {
    /*
    use plotpy::{Curve, Histogram, Legend, Plot, StrError};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = EXECUTIONS
        .iter()
        .map(|m| m.range_seconds() as f64)
        .collect();

    let mut values = Vec::new();
    for result in results {
        let (advance, insert, query) = result.workload_distribution();
        dbg!((advance, insert, query));
        values.push(vec![insert, query, advance]);
    }

    let labels = [
        "Insert".to_string(),
        "Query".to_string(),
        "Advance".to_string(),
    ];

    let mut histogram = Histogram::new();
    histogram
        .set_colors(&vec!["#cd0000", "#1862ab", "#cd8c00"])
        .set_style("barstacked");

    histogram.draw(&values, &labels);

    let mut plot = Plot::new();
    plot.set_label_y("Workload distribution %");
    plot.set_label_x("Window range");
    plot.set_ymax(100.0);
    plot.add(&histogram);

    let path = format!("../results/{}_workload_distribution.png", id);
    let path = Path::new(&path);
    plot.save(&path).unwrap();
    */
}
*/

/*
// TODO: refactor code to reuse things between subplots.
#[cfg(feature = "plot")]
fn plot_throughput_and_memory(id: &str, results: &Vec<BenchResult>) {
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = EXECUTIONS
        .iter()
        .map(|m| m.range_seconds() as f64)
        .collect();
    let mut lazy_64_y = Vec::new();
    let mut lazy_512_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut eager_512_y = Vec::new();
    let mut bfinger_four_y = Vec::new();
    let mut bfinger_eight_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();

    let throughput =
        |run: &Run| (run.total_insertions as f64 / run.runtime.as_secs_f64()) / 1_000_000.0;
    for res in results {
        let runs = &res.runs;
        lazy_64_y.push(throughput(&runs[0]));
        lazy_512_y.push(throughput(&runs[1]));
        eager_64_y.push(throughput(&runs[2]));
        eager_512_y.push(throughput(&runs[3]));
        bfinger_four_y.push(throughput(&runs[4]));
        bfinger_eight_y.push(throughput(&runs[5]));
        pairs_bfinger_four_y.push(throughput(&runs[6]));
        pairs_bfinger_eight_y.push(throughput(&runs[7]));
    }

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("o");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut lazy_curve_512 = Curve::new();
    lazy_curve_512.set_label("Lazy Wheel W512");
    lazy_curve_512.set_line_color("g");
    lazy_curve_512.set_marker_style("x");
    lazy_curve_512.draw(&x, &lazy_512_y);

    let mut eager_curve_64 = Curve::new();
    eager_curve_64.set_label("Eager Wheel W64");
    eager_curve_64.set_marker_style("o");
    eager_curve_64.set_line_color("r");
    eager_curve_64.draw(&x, &eager_64_y);

    let mut eager_curve_512 = Curve::new();
    eager_curve_512.set_label("Eager Wheel W512");
    eager_curve_512.set_marker_style("x");
    eager_curve_512.set_line_color("r");
    eager_curve_512.draw(&x, &eager_512_y);

    let mut fiba_b4_curve = Curve::new();
    fiba_b4_curve.set_label("FiBA Bfinger4");
    fiba_b4_curve.set_line_color("m");
    fiba_b4_curve.set_marker_style("*");
    fiba_b4_curve.draw(&x, &bfinger_four_y);

    let mut fiba_b8_curve = Curve::new();
    fiba_b8_curve.set_label("FiBA Bfinger8");
    fiba_b8_curve.set_line_color("m");
    fiba_b8_curve.set_marker_style("^");
    fiba_b8_curve.draw(&x, &bfinger_four_y);

    let mut pairs_fiba_b4_curve = Curve::new();
    pairs_fiba_b4_curve.set_label("Pairs + FiBA Bfinger4");
    pairs_fiba_b4_curve.set_line_color("b");
    pairs_fiba_b4_curve.set_marker_style("*");
    pairs_fiba_b4_curve.draw(&x, &pairs_bfinger_four_y);

    let mut pairs_fiba_b8_curve = Curve::new();
    pairs_fiba_b8_curve.set_label("Pairs + FiBA Bfinger8");
    pairs_fiba_b8_curve.set_line_color("b");
    pairs_fiba_b8_curve.set_marker_style("^");
    pairs_fiba_b8_curve.draw(&x, &pairs_bfinger_eight_y);

    // configure plot
    let mut plot = Plot::new();
    plot.set_super_title("")
        .set_horizontal_gap(0.5)
        .set_vertical_gap(0.5)
        .set_gaps(0.3, 0.2);

    plot.set_subplot(1, 2, 1);

    plot.set_title("Throughput");
    plot.set_label_y("throughput [million records/s]");
    plot.set_log_x(true);
    plot.set_label_x("Window range");

    plot.add(&lazy_curve_64)
        .add(&lazy_curve_512)
        .add(&eager_curve_64)
        .add(&eager_curve_512)
        .add(&fiba_b4_curve)
        .add(&fiba_b8_curve)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve);

    plot.set_subplot(1, 2, 2);

    let mut lazy_64_y = Vec::new();
    let mut lazy_512_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut eager_512_y = Vec::new();
    let mut bfinger_four_y = Vec::new();
    let mut bfinger_eight_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();

    let memory = |run: &Run| run.stats.size_bytes.get() as f64;
    for res in results {
        let runs = &res.runs;
        lazy_64_y.push(memory(&runs[0]));
        lazy_512_y.push(memory(&runs[1]));
        eager_64_y.push(memory(&runs[2]));
        eager_512_y.push(memory(&runs[3]));
        bfinger_four_y.push(memory(&runs[4]));
        bfinger_eight_y.push(memory(&runs[5]));
        pairs_bfinger_four_y.push(memory(&runs[6]));
        pairs_bfinger_eight_y.push(memory(&runs[7]));
    }

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("o");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut lazy_curve_512 = Curve::new();
    lazy_curve_512.set_label("Lazy Wheel W512");
    lazy_curve_512.set_line_color("g");
    lazy_curve_512.set_marker_style("x");
    lazy_curve_512.draw(&x, &lazy_512_y);

    let mut eager_curve_64 = Curve::new();
    eager_curve_64.set_label("Eager Wheel W64");
    eager_curve_64.set_marker_style("o");
    eager_curve_64.set_line_color("r");
    eager_curve_64.draw(&x, &eager_64_y);

    let mut eager_curve_512 = Curve::new();
    eager_curve_512.set_label("Eager Wheel W512");
    eager_curve_512.set_marker_style("x");
    eager_curve_512.set_line_color("r");
    eager_curve_512.draw(&x, &eager_512_y);

    let mut fiba_b4_curve = Curve::new();
    fiba_b4_curve.set_label("FiBA Bfinger4");
    fiba_b4_curve.set_line_color("m");
    fiba_b4_curve.set_marker_style("*");
    fiba_b4_curve.draw(&x, &bfinger_four_y);

    let mut fiba_b8_curve = Curve::new();
    fiba_b8_curve.set_label("FiBA Bfinger8");
    fiba_b8_curve.set_line_color("m");
    fiba_b8_curve.set_marker_style("^");
    fiba_b8_curve.draw(&x, &bfinger_four_y);

    let mut pairs_fiba_b4_curve = Curve::new();
    pairs_fiba_b4_curve.set_label("Pairs + FiBA Bfinger4");
    pairs_fiba_b4_curve.set_line_color("b");
    pairs_fiba_b4_curve.set_marker_style("*");
    pairs_fiba_b4_curve.draw(&x, &pairs_bfinger_four_y);

    let mut pairs_fiba_b8_curve = Curve::new();
    pairs_fiba_b8_curve.set_label("Pairs + FiBA Bfinger8");
    pairs_fiba_b8_curve.set_line_color("b");
    pairs_fiba_b8_curve.set_marker_style("^");
    pairs_fiba_b8_curve.draw(&x, &pairs_bfinger_eight_y);

    plot.set_title("Memory Usage");
    plot.set_label_y("memory usage (bytes)");
    plot.set_label_x("Window range");

    plot.add(&lazy_curve_64)
        .add(&lazy_curve_512)
        .add(&eager_curve_64)
        .add(&eager_curve_512)
        .add(&fiba_b4_curve)
        .add(&fiba_b8_curve)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve);

    let mut legend = Legend::new();
    legend.set_outside(true);
    legend.set_show_frame(false);
    legend.set_num_col(4);
    legend.set_location("upper center");
    legend.set_x_coords(&[-0.9, 1.4]);
    legend.draw();

    plot.add(&legend);

    // modify manually in generated python code: plt.gca().set_xscale('log', base=2)
    #[cfg(not(feature = "sync"))]
    let path = format!("../results/{}_throughput_and_memory.png", id);
    #[cfg(feature = "sync")]
    let path = format!("../results/{}_throughput_sync.png", id);

    let path = Path::new(&path);
    plot.set_figure_size_points(250.0 * 2.0, 250.0 * 0.75);
    plot.save(&path).unwrap();
}
*/

// TODO: refactor code to reuse things between subplots.
#[cfg(feature = "plot")]
fn plot_latencies(id: &str, results: &Vec<BenchResult>) {
    use awheel::stats::Percentiles;
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = EXECUTIONS
        .iter()
        .map(|m| m.range_seconds() as f64)
        .collect();
    let mut lazy_64_y = Vec::new();
    let mut lazy_512_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut eager_512_y = Vec::new();
    let mut bfinger_four_y = Vec::new();
    let mut bfinger_eight_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();

    // plot p99 latency
    let p99_latency = |p: Percentiles| p.p99;
    for res in results {
        let runs = &res.runs;
        lazy_64_y.push(p99_latency(
            runs[0].stats.window_computation_ns.percentiles(),
        ));
        lazy_512_y.push(p99_latency(
            runs[1].stats.window_computation_ns.percentiles(),
        ));
        eager_64_y.push(p99_latency(
            runs[2].stats.window_computation_ns.percentiles(),
        ));
        eager_512_y.push(p99_latency(
            runs[3].stats.window_computation_ns.percentiles(),
        ));
        bfinger_four_y.push(p99_latency(
            runs[4].stats.window_computation_ns.percentiles(),
        ));
        bfinger_eight_y.push(p99_latency(
            runs[5].stats.window_computation_ns.percentiles(),
        ));
        pairs_bfinger_four_y.push(p99_latency(
            runs[6].stats.window_computation_ns.percentiles(),
        ));
        pairs_bfinger_eight_y.push(p99_latency(
            runs[7].stats.window_computation_ns.percentiles(),
        ));
    }

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("o");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut lazy_curve_512 = Curve::new();
    lazy_curve_512.set_label("Lazy Wheel W512");
    lazy_curve_512.set_line_color("g");
    lazy_curve_512.set_marker_style("x");
    lazy_curve_512.draw(&x, &lazy_512_y);

    let mut eager_curve_64 = Curve::new();
    eager_curve_64.set_label("Eager Wheel W64");
    eager_curve_64.set_marker_style("o");
    eager_curve_64.set_line_color("r");
    eager_curve_64.draw(&x, &eager_64_y);

    let mut eager_curve_512 = Curve::new();
    eager_curve_512.set_label("Eager Wheel W512");
    eager_curve_512.set_marker_style("x");
    eager_curve_512.set_line_color("r");
    eager_curve_512.draw(&x, &eager_512_y);

    let mut fiba_b4_curve = Curve::new();
    fiba_b4_curve.set_label("FiBA Bfinger4");
    fiba_b4_curve.set_line_color("m");
    fiba_b4_curve.set_marker_style("*");
    fiba_b4_curve.draw(&x, &bfinger_four_y);

    let mut fiba_b8_curve = Curve::new();
    fiba_b8_curve.set_label("FiBA Bfinger8");
    fiba_b8_curve.set_line_color("m");
    fiba_b8_curve.set_marker_style("^");
    fiba_b8_curve.draw(&x, &bfinger_four_y);

    let mut pairs_fiba_b4_curve = Curve::new();
    pairs_fiba_b4_curve.set_label("Pairs + FiBA Bfinger4");
    pairs_fiba_b4_curve.set_line_color("b");
    pairs_fiba_b4_curve.set_marker_style("*");
    pairs_fiba_b4_curve.draw(&x, &pairs_bfinger_four_y);

    let mut pairs_fiba_b8_curve = Curve::new();
    pairs_fiba_b8_curve.set_label("Pairs + FiBA Bfinger8");
    pairs_fiba_b8_curve.set_line_color("b");
    pairs_fiba_b8_curve.set_marker_style("^");
    pairs_fiba_b8_curve.draw(&x, &pairs_bfinger_eight_y);

    // configure plot
    let mut plot = Plot::new();
    plot.set_super_title("")
        .set_horizontal_gap(0.5)
        .set_vertical_gap(0.5)
        .set_gaps(0.3, 0.5);

    plot.set_subplot(2, 3, 1);

    plot.set_title("Query");
    plot.set_label_y("p99 latency (nanoseconds)");
    //plot.set_log_x(true);
    plot.set_log_y(true);
    plot.set_label_x("Window range");

    plot.add(&lazy_curve_64)
        .add(&lazy_curve_512)
        .add(&eager_curve_64)
        .add(&eager_curve_512)
        .add(&fiba_b4_curve)
        .add(&fiba_b8_curve)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve);

    plot.set_subplot(2, 3, 2);

    let mut lazy_64_y = Vec::new();
    let mut lazy_512_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut eager_512_y = Vec::new();
    let mut bfinger_four_y = Vec::new();
    let mut bfinger_eight_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();

    // plot p99 latency
    for res in results {
        let runs = &res.runs;
        lazy_64_y.push(p99_latency(runs[0].stats.insert_ns.percentiles()));
        lazy_512_y.push(p99_latency(runs[1].stats.insert_ns.percentiles()));
        eager_64_y.push(p99_latency(runs[2].stats.insert_ns.percentiles()));
        eager_512_y.push(p99_latency(runs[3].stats.insert_ns.percentiles()));
        bfinger_four_y.push(p99_latency(runs[4].stats.insert_ns.percentiles()));
        bfinger_eight_y.push(p99_latency(runs[5].stats.insert_ns.percentiles()));
        pairs_bfinger_four_y.push(p99_latency(runs[6].stats.insert_ns.percentiles()));
        pairs_bfinger_eight_y.push(p99_latency(runs[7].stats.insert_ns.percentiles()));
    }

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("o");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut lazy_curve_512 = Curve::new();
    lazy_curve_512.set_label("Lazy Wheel W512");
    lazy_curve_512.set_line_color("g");
    lazy_curve_512.set_marker_style("x");
    lazy_curve_512.draw(&x, &lazy_512_y);

    let mut eager_curve_64 = Curve::new();
    eager_curve_64.set_label("Eager Wheel W64");
    eager_curve_64.set_marker_style("o");
    eager_curve_64.set_line_color("r");
    eager_curve_64.draw(&x, &eager_64_y);

    let mut eager_curve_512 = Curve::new();
    eager_curve_512.set_label("Eager Wheel W512");
    eager_curve_512.set_marker_style("x");
    eager_curve_512.set_line_color("r");
    eager_curve_512.draw(&x, &eager_512_y);

    let mut fiba_b4_curve = Curve::new();
    fiba_b4_curve.set_label("FiBA Bfinger4");
    fiba_b4_curve.set_line_color("m");
    fiba_b4_curve.set_marker_style("*");
    fiba_b4_curve.draw(&x, &bfinger_four_y);

    let mut fiba_b8_curve = Curve::new();
    fiba_b8_curve.set_label("FiBA Bfinger8");
    fiba_b8_curve.set_line_color("m");
    fiba_b8_curve.set_marker_style("^");
    fiba_b8_curve.draw(&x, &bfinger_four_y);

    let mut pairs_fiba_b4_curve = Curve::new();
    pairs_fiba_b4_curve.set_label("Pairs + FiBA Bfinger4");
    pairs_fiba_b4_curve.set_line_color("b");
    pairs_fiba_b4_curve.set_marker_style("*");
    pairs_fiba_b4_curve.draw(&x, &pairs_bfinger_four_y);

    let mut pairs_fiba_b8_curve = Curve::new();
    pairs_fiba_b8_curve.set_label("Pairs + FiBA Bfinger8");
    pairs_fiba_b8_curve.set_line_color("b");
    pairs_fiba_b8_curve.set_marker_style("^");
    pairs_fiba_b8_curve.draw(&x, &pairs_bfinger_eight_y);

    //plot.set_label_y("Insert p99 latency (nanoseconds)");
    //plot.set_log_x(true);
    plot.set_title("Insert");
    plot.set_log_y(true);
    plot.set_label_x("Window range");

    plot.add(&lazy_curve_64)
        .add(&lazy_curve_512)
        .add(&eager_curve_64)
        .add(&eager_curve_512)
        .add(&fiba_b4_curve)
        .add(&fiba_b8_curve)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve);

    plot.set_subplot(2, 3, 3);

    let mut lazy_64_y = Vec::new();
    let mut lazy_512_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut eager_512_y = Vec::new();
    let mut bfinger_four_y = Vec::new();
    let mut bfinger_eight_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();

    for res in results {
        let runs = &res.runs;
        lazy_64_y.push(p99_latency(runs[0].stats.advance_ns.percentiles()));
        lazy_512_y.push(p99_latency(runs[1].stats.advance_ns.percentiles()));
        eager_64_y.push(p99_latency(runs[2].stats.advance_ns.percentiles()));
        eager_512_y.push(p99_latency(runs[3].stats.advance_ns.percentiles()));
        bfinger_four_y.push(p99_latency(runs[4].stats.advance_ns.percentiles()));
        bfinger_eight_y.push(p99_latency(runs[5].stats.advance_ns.percentiles()));
        pairs_bfinger_four_y.push(p99_latency(runs[6].stats.advance_ns.percentiles()));
        pairs_bfinger_eight_y.push(p99_latency(runs[7].stats.advance_ns.percentiles()));
    }

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("o");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut lazy_curve_512 = Curve::new();
    lazy_curve_512.set_label("Lazy Wheel W512");
    lazy_curve_512.set_line_color("g");
    lazy_curve_512.set_marker_style("x");
    lazy_curve_512.draw(&x, &lazy_512_y);

    let mut eager_curve_64 = Curve::new();
    eager_curve_64.set_label("Eager Wheel W64");
    eager_curve_64.set_marker_style("o");
    eager_curve_64.set_line_color("r");
    eager_curve_64.draw(&x, &eager_64_y);

    let mut eager_curve_512 = Curve::new();
    eager_curve_512.set_label("Eager Wheel W512");
    eager_curve_512.set_marker_style("x");
    eager_curve_512.set_line_color("r");
    eager_curve_512.draw(&x, &eager_512_y);

    let mut fiba_b4_curve = Curve::new();
    fiba_b4_curve.set_label("FiBA Bfinger4");
    fiba_b4_curve.set_line_color("m");
    fiba_b4_curve.set_marker_style("*");
    fiba_b4_curve.draw(&x, &bfinger_four_y);

    let mut fiba_b8_curve = Curve::new();
    fiba_b8_curve.set_label("FiBA Bfinger8");
    fiba_b8_curve.set_line_color("m");
    fiba_b8_curve.set_marker_style("^");
    fiba_b8_curve.draw(&x, &bfinger_four_y);

    let mut pairs_fiba_b4_curve = Curve::new();
    pairs_fiba_b4_curve.set_label("Pairs + FiBA Bfinger4");
    pairs_fiba_b4_curve.set_line_color("b");
    pairs_fiba_b4_curve.set_marker_style("*");
    pairs_fiba_b4_curve.draw(&x, &pairs_bfinger_four_y);

    let mut pairs_fiba_b8_curve = Curve::new();
    pairs_fiba_b8_curve.set_label("Pairs + FiBA Bfinger8");
    pairs_fiba_b8_curve.set_line_color("b");
    pairs_fiba_b8_curve.set_marker_style("^");
    pairs_fiba_b8_curve.draw(&x, &pairs_bfinger_eight_y);

    //plot.set_label_y("Advance p99 latency (nanoseconds)");
    //plot.set_log_x(true);
    plot.set_title("Advance");
    plot.set_log_y(true);
    plot.set_label_x("Window range");

    plot.add(&lazy_curve_64)
        .add(&lazy_curve_512)
        .add(&eager_curve_64)
        .add(&eager_curve_512)
        .add(&fiba_b4_curve)
        .add(&fiba_b8_curve)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve);

    // Throughput + Memory
    let mut lazy_64_y = Vec::new();
    let mut lazy_512_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut eager_512_y = Vec::new();
    let mut bfinger_four_y = Vec::new();
    let mut bfinger_eight_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();

    let throughput =
        |run: &Run| (run.total_insertions as f64 / run.runtime.as_secs_f64()) / 1_000_000.0;
    for res in results {
        let runs = &res.runs;
        lazy_64_y.push(throughput(&runs[0]));
        lazy_512_y.push(throughput(&runs[1]));
        eager_64_y.push(throughput(&runs[2]));
        eager_512_y.push(throughput(&runs[3]));
        bfinger_four_y.push(throughput(&runs[4]));
        bfinger_eight_y.push(throughput(&runs[5]));
        pairs_bfinger_four_y.push(throughput(&runs[6]));
        pairs_bfinger_eight_y.push(throughput(&runs[7]));
    }

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("o");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut lazy_curve_512 = Curve::new();
    lazy_curve_512.set_label("Lazy Wheel W512");
    lazy_curve_512.set_line_color("g");
    lazy_curve_512.set_marker_style("x");
    lazy_curve_512.draw(&x, &lazy_512_y);

    let mut eager_curve_64 = Curve::new();
    eager_curve_64.set_label("Eager Wheel W64");
    eager_curve_64.set_marker_style("o");
    eager_curve_64.set_line_color("r");
    eager_curve_64.draw(&x, &eager_64_y);

    let mut eager_curve_512 = Curve::new();
    eager_curve_512.set_label("Eager Wheel W512");
    eager_curve_512.set_marker_style("x");
    eager_curve_512.set_line_color("r");
    eager_curve_512.draw(&x, &eager_512_y);

    let mut fiba_b4_curve = Curve::new();
    fiba_b4_curve.set_label("FiBA Bfinger4");
    fiba_b4_curve.set_line_color("m");
    fiba_b4_curve.set_marker_style("*");
    fiba_b4_curve.draw(&x, &bfinger_four_y);

    let mut fiba_b8_curve = Curve::new();
    fiba_b8_curve.set_label("FiBA Bfinger8");
    fiba_b8_curve.set_line_color("m");
    fiba_b8_curve.set_marker_style("^");
    fiba_b8_curve.draw(&x, &bfinger_four_y);

    let mut pairs_fiba_b4_curve = Curve::new();
    pairs_fiba_b4_curve.set_label("Pairs + FiBA Bfinger4");
    pairs_fiba_b4_curve.set_line_color("b");
    pairs_fiba_b4_curve.set_marker_style("*");
    pairs_fiba_b4_curve.draw(&x, &pairs_bfinger_four_y);

    let mut pairs_fiba_b8_curve = Curve::new();
    pairs_fiba_b8_curve.set_label("Pairs + FiBA Bfinger8");
    pairs_fiba_b8_curve.set_line_color("b");
    pairs_fiba_b8_curve.set_marker_style("^");
    pairs_fiba_b8_curve.draw(&x, &pairs_bfinger_eight_y);

    plot.set_subplot(2, 3, 4);

    plot.set_title("Throughput");
    plot.set_label_y("throughput [million events/s]");
    //plot.set_log_x(true);
    plot.set_label_x("Window range");

    plot.add(&lazy_curve_64)
        .add(&lazy_curve_512)
        .add(&eager_curve_64)
        .add(&eager_curve_512)
        .add(&fiba_b4_curve)
        .add(&fiba_b8_curve)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve);

    plot.set_subplot(2, 3, 5);

    let mut lazy_64_y = Vec::new();
    let mut lazy_512_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut eager_512_y = Vec::new();
    let mut bfinger_four_y = Vec::new();
    let mut bfinger_eight_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();

    let memory = |run: &Run| run.stats.size_bytes.get() as f64;
    for res in results {
        let runs = &res.runs;
        lazy_64_y.push(memory(&runs[0]));
        lazy_512_y.push(memory(&runs[1]));
        eager_64_y.push(memory(&runs[2]));
        eager_512_y.push(memory(&runs[3]));
        bfinger_four_y.push(memory(&runs[4]));
        bfinger_eight_y.push(memory(&runs[5]));
        pairs_bfinger_four_y.push(memory(&runs[6]));
        pairs_bfinger_eight_y.push(memory(&runs[7]));
    }

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("o");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut lazy_curve_512 = Curve::new();
    lazy_curve_512.set_label("Lazy Wheel W512");
    lazy_curve_512.set_line_color("g");
    lazy_curve_512.set_marker_style("x");
    lazy_curve_512.draw(&x, &lazy_512_y);

    let mut eager_curve_64 = Curve::new();
    eager_curve_64.set_label("Eager Wheel W64");
    eager_curve_64.set_marker_style("o");
    eager_curve_64.set_line_color("r");
    eager_curve_64.draw(&x, &eager_64_y);

    let mut eager_curve_512 = Curve::new();
    eager_curve_512.set_label("Eager Wheel W512");
    eager_curve_512.set_marker_style("x");
    eager_curve_512.set_line_color("r");
    eager_curve_512.draw(&x, &eager_512_y);

    let mut fiba_b4_curve = Curve::new();
    fiba_b4_curve.set_label("FiBA Bfinger4");
    fiba_b4_curve.set_line_color("m");
    fiba_b4_curve.set_marker_style("*");
    fiba_b4_curve.draw(&x, &bfinger_four_y);

    let mut fiba_b8_curve = Curve::new();
    fiba_b8_curve.set_label("FiBA Bfinger8");
    fiba_b8_curve.set_line_color("m");
    fiba_b8_curve.set_marker_style("^");
    fiba_b8_curve.draw(&x, &bfinger_four_y);

    let mut pairs_fiba_b4_curve = Curve::new();
    pairs_fiba_b4_curve.set_label("Pairs + FiBA Bfinger4");
    pairs_fiba_b4_curve.set_line_color("b");
    pairs_fiba_b4_curve.set_marker_style("*");
    pairs_fiba_b4_curve.draw(&x, &pairs_bfinger_four_y);

    let mut pairs_fiba_b8_curve = Curve::new();
    pairs_fiba_b8_curve.set_label("Pairs + FiBA Bfinger8");
    pairs_fiba_b8_curve.set_line_color("b");
    pairs_fiba_b8_curve.set_marker_style("^");
    pairs_fiba_b8_curve.draw(&x, &pairs_bfinger_eight_y);

    plot.set_title("Space");
    plot.set_label_y("memory usage (bytes)");
    plot.set_log_y(true);
    plot.set_label_x("Window range");

    plot.add(&lazy_curve_64)
        .add(&lazy_curve_512)
        .add(&eager_curve_64)
        .add(&eager_curve_512)
        .add(&fiba_b4_curve)
        .add(&fiba_b8_curve)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve);

    let mut legend = Legend::new();
    legend.set_outside(true);
    legend.set_show_frame(false);
    legend.set_num_col(4);
    legend.set_location("upper center");
    legend.set_x_coords(&[-0.9, 3.0]);
    legend.draw();

    plot.add(&legend);

    // modify manually in generated python code: plt.gca().set_xscale('log', base=2)

    #[cfg(not(feature = "sync"))]
    let path = format!("../results/{}_latency.png", id);
    #[cfg(feature = "sync")]
    let path = format!("../results/{}_window_latency.png", id);

    let path = Path::new(&path);
    plot.set_figure_size_points(250.0 * 3.0, (250.0 * 2.0) * 0.75);

    plot.save(&path).unwrap();
}
