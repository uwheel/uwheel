use awheel::{
    aggregator::sum::U64SumAggregator,
    time::Duration,
    window::{
        eager,
        eager_window_query_cost,
        lazy,
        lazy_window_query_cost,
        stats::Stats,
        WindowExt,
    },
    Entry,
};
use clap::{ArgEnum, Parser};
use minstant::Instant;
use window::{fiba_wheel, TimestampGenerator};

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Workload {
    Insert,
    Computation,
    All,
}

const EXECUTIONS: [Execution; 5] = [
    Execution::new(30, 10),
    Execution::new(60, 10),
    Execution::new(1800, 10),
    Execution::new(3600, 10),
    Execution::new(86400, 10),
    //Execution::new(604800, 10),
];

// max possible out of order distance
// for example if 8, then may generate timestamp with lowest 1 second distance from the watermark and highest 8.
const OUT_OF_ORDER_DISTANCE: [usize; 7] = [1, 2, 4, 8, 16, 32, 64];

#[derive(Debug)]
struct Execution {
    pub range: u64,
    pub slide: u64,
}
impl Execution {
    pub const fn new(range: u64, slide: u64) -> Self {
        Self { range, slide }
    }
}

#[allow(dead_code)]
struct Result {
    execution: Execution,
    runs: Vec<Run>,
}
impl Result {
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
struct Run {
    pub id: String,
    pub total_insertions: u64,
    pub runtime: std::time::Duration,
    pub stats: Stats,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 10000)]
    windows: u64,
    #[clap(short, long, value_parser, default_value_t = 10000)]
    events_per_sec: u64,
    #[clap(short, long, value_parser, default_value_t = 5)]
    max_distance: u64,
    #[clap(short, long, value_parser, default_value_t = 30)]
    range: u64,
    #[clap(short, long, value_parser, default_value_t = 10)]
    slide: u64,
    #[clap(short, long, value_parser, default_value_t = 10)]
    ooo_degree: u64,
    #[clap(arg_enum, value_parser, default_value_t = Workload::Insert)]
    workload: Workload,
}

// calculate how many seconds are required to trigger N number of windows with a RANGE and SLIDE.
fn number_of_seconds(windows: u64, range: u64, slide: u64) -> u64 {
    (windows - 1) * slide + range
}

fn main() {
    let mut args = Args::parse();
    println!("Running with {:#?}", args);
    let range = Duration::seconds(args.range as i64);
    let slide = Duration::seconds(args.slide as i64);
    let seconds = number_of_seconds(
        args.windows,
        range.whole_seconds() as u64,
        slide.whole_seconds() as u64,
    );
    let inserts = seconds * args.events_per_sec;
    dbg!((seconds, inserts));
    dbg!(lazy_window_query_cost(range, slide));
    dbg!(eager_window_query_cost(range, slide));

    match args.workload {
        Workload::All => {
            window_computation_bench(&args);
            insert_rate_bench(&mut args);
        }
        Workload::Insert => {
            insert_rate_bench(&mut args);
        }
        Workload::Computation => {
            window_computation_bench(&args);
        }
    }

    /*
    let gate = Arc::new(AtomicBool::new(true));
    let read_wheel = eager_wheel.wheel().clone();
    let inner_gate = gate.clone();
    let handle = std::thread::spawn(move || {
        let now = Instant::now();
        let mut counter = 0;
        while inner_gate.load(Ordering::Relaxed) {
            // Execute queries on random granularities
            let pick = fastrand::usize(0..3);
            if pick == 0 {
                let _res = std::hint::black_box(
                    read_wheel.interval(Duration::seconds(fastrand::i64(1..60))),
                );
            } else if pick == 1 {
                let _res = std::hint::black_box(
                    read_wheel.interval(Duration::minutes(fastrand::i64(1..60))),
                );
            } else {
                let _res = std::hint::black_box(
                    read_wheel.interval(Duration::hours(fastrand::i64(1..24))),
                );
            }
            counter += 1;
        }
        println!(
            "Concurrent Read task ran at {} Mops/s",
            (counter as f64 / now.elapsed().as_secs_f64()) as u64 / 1_000_000
        );
    });
    run("Eager Wheel SUM", seconds, eager_wheel, &args);
    gate.store(false, Ordering::Relaxed);
    handle.join().unwrap();
    */
}

#[cfg(feature = "debug")]
#[macro_export]
macro_rules! log {
    ($( $args:expr ),*) => { println!( $( $args ),* ); }
}

#[cfg(not(feature = "debug"))]
#[macro_export]
macro_rules! log {
    ($( $args:expr ),*) => {};
}

// focus: measure computation latency (p99) of each window for growing window ranges with a slide of 10s
fn window_computation_bench(args: &Args) {
    let mut results = Vec::new();

    for exec in EXECUTIONS {
        let range = Duration::seconds(exec.range as i64);
        let slide = Duration::seconds(exec.slide as i64);
        let seconds = number_of_seconds(
            args.windows,
            range.whole_seconds() as u64,
            slide.whole_seconds() as u64,
        );
        let total_insertions = seconds * args.events_per_sec;

        let mut runs = Vec::new();

        let lazy_wheel: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .build();

        let (runtime, stats) = run(seconds, lazy_wheel, args);

        runs.push(Run {
            id: "Lazy Wheel".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let eager_wheel: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .build();

        let (runtime, stats) = run(seconds, eager_wheel, args);
        runs.push(Run {
            id: "Eager Wheel".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let cg_bfinger_four_wheel = fiba_wheel::BFingerFourWheel::new(0, range, slide);
        let (runtime, stats) = run(seconds, cg_bfinger_four_wheel, args);
        runs.push(Run {
            id: "FiBA CG BFinger4".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let cg_bfinger_eight_wheel = fiba_wheel::BFingerEightWheel::new(0, range, slide);
        let (runtime, stats) = run(seconds, cg_bfinger_eight_wheel, args);
        runs.push(Run {
            id: "FiBA CG BFinger8".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let result = Result::new(exec, runs);

        result.print();
        results.push(result);
    }
    #[cfg(feature = "plot")]
    plot_window_computation_bench(results);
}

// focus: measure runtime of 30s range and 10s slide with growing events per second
fn insert_rate_bench(args: &mut Args) {
    let range = Duration::seconds(30);
    let slide = Duration::seconds(10);
    let seconds = number_of_seconds(
        args.windows,
        range.whole_seconds() as u64,
        slide.whole_seconds() as u64,
    );

    let mut results = Vec::new();
    for distance in OUT_OF_ORDER_DISTANCE.iter() {
        args.max_distance = *distance as u64;
        let total_insertions = seconds * args.events_per_sec;
        let mut runs = Vec::new();

        let lazy_wheel: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .build();

        let (runtime, stats) = run(seconds, lazy_wheel, args);

        runs.push(Run {
            id: "Lazy Wheel".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let eager_wheel: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .build();

        let (runtime, stats) = run(seconds, eager_wheel, args);
        runs.push(Run {
            id: "Eager Wheel".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let cg_bfinger_four_wheel = fiba_wheel::BFingerFourWheel::new(0, range, slide);
        let (runtime, stats) = run(seconds, cg_bfinger_four_wheel, args);
        runs.push(Run {
            id: "FiBA CG BFinger4".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let cg_bfinger_eight_wheel = fiba_wheel::BFingerEightWheel::new(0, range, slide);
        let (runtime, stats) = run(seconds, cg_bfinger_eight_wheel, args);
        runs.push(Run {
            id: "FiBA CG BFinger8".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        results.push(runs);
        for runs in &results {
            //println!("Events per second: {}", events);
            for run in runs.iter() {
                println!("{} (took {:.2}s)", run.id, run.runtime.as_secs_f64(),);
                println!("{:#?}", run.stats);
            }
        }
    }
    #[cfg(feature = "plot")]
    plot_insert_bench(&results);
    #[cfg(feature = "plot")]
    plot_insert_bench_latency(results);
}

#[cfg(feature = "plot")]
fn plot_insert_bench(results: &Vec<Vec<Run>>) {
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = OUT_OF_ORDER_DISTANCE.iter().map(|m| *m as f64).collect();
    let mut lazy_y = Vec::new();
    let mut eager_y = Vec::new();
    let mut bfinger_four_y = Vec::new();
    let mut bfinger_eight_y = Vec::new();

    let throughput =
        |run: &Run| (run.total_insertions as f64 / run.runtime.as_secs_f64()) / 1_000_000.0;
    for runs in results {
        lazy_y.push(throughput(&runs[0]));
        eager_y.push(throughput(&runs[1]));
        bfinger_four_y.push(throughput(&runs[2]));
        bfinger_eight_y.push(throughput(&runs[3]));
    }

    let mut lazy_curve = Curve::new();
    lazy_curve.set_label("Lazy Wheel");
    lazy_curve.set_line_color("g");
    lazy_curve.set_marker_style("o");
    lazy_curve.draw(&x, &lazy_y);

    let mut eager_curve = Curve::new();
    eager_curve.set_label("Eager Wheel");
    eager_curve.set_marker_style("^");
    eager_curve.set_line_color("r");
    eager_curve.draw(&x, &eager_y);

    let mut bfinger_four_curve = Curve::new();
    bfinger_four_curve.set_label("FiBA CG BFinger 4");
    bfinger_four_curve.set_line_color("m");
    bfinger_four_curve.set_marker_style("*");
    bfinger_four_curve.draw(&x, &bfinger_four_y);

    let mut bfinger_eight_curve = Curve::new();
    bfinger_eight_curve.set_label("FiBA CG BFinger 8");
    bfinger_eight_curve.set_line_color("m");
    bfinger_eight_curve.set_marker_style("^");
    bfinger_eight_curve.draw(&x, &bfinger_eight_y);

    let mut legend = Legend::new();
    legend.draw();

    // configure plot
    let mut plot = Plot::new();
    plot.set_super_title("30s Range 10s slide (Sum Aggregation)")
        .set_horizontal_gap(0.5)
        .set_vertical_gap(0.5)
        .set_gaps(0.3, 0.2);

    plot.set_label_y("throughput [million records/s]");
    plot.set_log_x(true);
    plot.set_label_x("ooo distance [seconds]");

    plot.add(&lazy_curve)
        .add(&eager_curve)
        .add(&bfinger_four_curve)
        .add(&bfinger_eight_curve)
        .add(&legend);

    // modify manually in generated python code: plt.gca().set_xscale('log', base=2)
    let path = Path::new("../results/synthetic_window.png");
    plot.save(&path).unwrap();
}

#[cfg(feature = "plot")]
fn plot_insert_bench_latency(results: Vec<Vec<Run>>) {
    use awheel::stats::Percentiles;
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = OUT_OF_ORDER_DISTANCE.iter().map(|m| *m as f64).collect();
    let mut lazy_y = Vec::new();
    let mut eager_y = Vec::new();
    let mut bfinger_four_y = Vec::new();
    let mut bfinger_eight_y = Vec::new();

    let p99_latency = |p: Percentiles| p.p99;
    for runs in results {
        lazy_y.push(p99_latency(runs[0].stats.insert_ns.percentiles()));
        eager_y.push(p99_latency(runs[1].stats.insert_ns.percentiles()));
        bfinger_four_y.push(p99_latency(runs[2].stats.insert_ns.percentiles()));
        bfinger_eight_y.push(p99_latency(runs[3].stats.insert_ns.percentiles()));
    }

    let mut lazy_curve = Curve::new();
    lazy_curve.set_label("Lazy Wheel");
    lazy_curve.set_line_color("g");
    lazy_curve.set_marker_style("o");
    lazy_curve.draw(&x, &lazy_y);

    let mut eager_curve = Curve::new();
    eager_curve.set_label("Eager Wheel");
    eager_curve.set_marker_style("^");
    eager_curve.set_line_color("r");
    eager_curve.draw(&x, &eager_y);

    let mut bfinger_four_curve = Curve::new();
    bfinger_four_curve.set_label("FiBA CG BFinger 4");
    bfinger_four_curve.set_line_color("m");
    bfinger_four_curve.set_marker_style("*");
    bfinger_four_curve.draw(&x, &bfinger_four_y);

    let mut bfinger_eight_curve = Curve::new();
    bfinger_eight_curve.set_label("FiBA CG BFinger 8");
    bfinger_eight_curve.set_line_color("m");
    bfinger_eight_curve.set_marker_style("^");
    bfinger_eight_curve.draw(&x, &bfinger_eight_y);

    let mut legend = Legend::new();
    legend.draw();

    // configure plot
    let mut plot = Plot::new();
    plot.set_super_title("30s Range 10s slide (Sum Aggregation)")
        .set_horizontal_gap(0.5)
        .set_vertical_gap(0.5)
        .set_gaps(0.3, 0.2);

    plot.set_label_y("p99 insert latency (nanoseconds)");
    plot.set_log_x(true);
    plot.set_label_x("ooo distance [seconds]");

    plot.add(&lazy_curve)
        .add(&eager_curve)
        .add(&bfinger_four_curve)
        .add(&bfinger_eight_curve)
        .add(&legend);

    // save figure
    let path = Path::new("../results/synthetic_window_latency.png");
    plot.save(&path).unwrap();
}

#[cfg(feature = "plot")]
fn plot_window_computation_bench(results: Vec<Result>) {
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x = vec![30.0, 60.0, 1800.0, 3600.0, 86400.0, 604800.0];
    let mut lazy_y = Vec::new();
    let mut eager_y = Vec::new();
    let mut bfinger_four_y = Vec::new();
    let mut bfinger_eight_y = Vec::new();

    let to_micros = |v: f64| std::time::Duration::from_nanos(v as u64).as_micros() as f64;
    for result in &results {
        lazy_y.push(to_micros(
            result.runs[0].stats.window_computation_ns.percentiles().p99,
        ));
        eager_y.push(to_micros(
            result.runs[1].stats.window_computation_ns.percentiles().p99,
        ));
        bfinger_four_y.push(to_micros(
            result.runs[2].stats.window_computation_ns.percentiles().p99,
        ));
        bfinger_eight_y.push(to_micros(
            result.runs[3].stats.window_computation_ns.percentiles().p99,
        ));
    }

    let mut lazy_curve = Curve::new();
    lazy_curve.set_label("Lazy Wheel");
    lazy_curve.set_line_color("g");
    lazy_curve.set_marker_style("^");
    lazy_curve.draw(&x, &lazy_y);

    let mut eager_curve = Curve::new();
    eager_curve.set_label("Eager Wheel");
    eager_curve.set_line_color("r");
    eager_curve.set_marker_style("o");
    eager_curve.draw(&x, &eager_y);

    let mut bfinger_four_curve = Curve::new();
    bfinger_four_curve.set_label("FiBA CG BFinger 4");
    bfinger_four_curve.set_line_color("m");
    bfinger_four_curve.set_marker_style("*");
    bfinger_four_curve.draw(&x, &bfinger_four_y);

    let mut bfinger_eight_curve = Curve::new();
    bfinger_eight_curve.set_label("FiBA CG BFinger 8");
    bfinger_eight_curve.set_line_color("m");
    bfinger_eight_curve.set_marker_style("^");
    bfinger_eight_curve.draw(&x, &bfinger_eight_y);

    let mut legend = Legend::new();
    legend.draw();

    // configure plot
    let mut plot = Plot::new();
    plot.set_horizontal_gap(0.5)
        .set_vertical_gap(0.5)
        .set_gaps(0.3, 0.2);

    plot.set_label_y("P99 Latency (microseconds)");
    //plot.set_log_y(true);
    plot.set_label_x("Window Range (seconds)");

    plot.add(&lazy_curve)
        .add(&eager_curve)
        .add(&bfinger_four_curve)
        .add(&bfinger_eight_curve)
        .add(&legend);

    // save figure
    let path = Path::new("../results/synthetic_window_comp.png");
    plot.save(&path).unwrap();
}

fn run(
    seconds: u64,
    mut window: impl WindowExt<U64SumAggregator>,
    args: &Args,
) -> (std::time::Duration, Stats) {
    let Args {
        events_per_sec,
        windows: _,
        max_distance,
        range: _,
        slide: _,
        ooo_degree,
        workload: _,
    } = *args;
    let mut ts_generator =
        TimestampGenerator::new(0, Duration::seconds(max_distance as i64), ooo_degree as f32);
    let full = Instant::now();
    for _i in 0..seconds {
        for _i in 0..events_per_sec {
            window.insert(Entry::new(1, ts_generator.timestamp()));
        }
        ts_generator.update_watermark(ts_generator.watermark() + 1000);

        // advance window wheel
        for (_timestamp, _result) in window.advance_to(ts_generator.watermark()) {
            log!("Window at {} with data {:?}", _timestamp, _result);
        }
    }
    let runtime = full.elapsed();
    (runtime, window.stats().clone())
}
