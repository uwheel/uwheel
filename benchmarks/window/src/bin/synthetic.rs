use std::collections::BTreeMap;

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
use window::{
    external_impls::{self, PairsTree},
    tree,
    TimestampGenerator,
};

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Workload {
    Insert,
    Ooo,
    All,
}

// max possible out of order distance
// for example if 8, then may generate timestamp with lowest 1 second distance from the watermark and highest 8.
const OUT_OF_ORDER_DISTANCE: [usize; 7] = [1, 2, 4, 8, 16, 32, 64];
const EVENTS_PER_SECOND: [usize; 6] = [10, 100, 1000, 10000, 100000, 1000000];

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
            insert_rate_bench(&mut args);
            insert_random_ooo_distance(&mut args);
        }
        Workload::Insert => {
            insert_rate_bench(&mut args);
        }
        Workload::Ooo => {
            insert_random_ooo_distance(&mut args);
        }
    }
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
    for events in EVENTS_PER_SECOND.iter() {
        args.max_distance = 1;
        let total_insertions = seconds * *events as u64;
        let mut runs = Vec::new();

        let lazy_wheel_8: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(8)
            .build();

        let (runtime, stats) = run(seconds, lazy_wheel_8, args);

        println!("Finished Lazy Wheel 8");

        runs.push(Run {
            id: "Lazy Wheel 8".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let lazy_wheel_32: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(32)
            .build();

        let (runtime, stats) = run(seconds, lazy_wheel_32, args);

        println!("Finished Lazy Wheel 32");

        runs.push(Run {
            id: "Lazy Wheel 32".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let lazy_wheel_64: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(64)
            .build();

        let (runtime, stats) = run(seconds, lazy_wheel_64, args);

        println!("Finished Lazy Wheel 64");

        runs.push(Run {
            id: "Lazy Wheel 64".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let eager_wheel_8: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(8)
            .build();

        let (runtime, stats) = run(seconds, eager_wheel_8, args);

        println!("Finished Eager Wheel 8");

        runs.push(Run {
            id: "Eager Wheel 8".to_string(),
            total_insertions,
            runtime,
            stats,
        });
        let eager_wheel_32: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(32)
            .build();

        let (runtime, stats) = run(seconds, eager_wheel_32, args);

        println!("Finished Eager Wheel 32");

        runs.push(Run {
            id: "Eager Wheel 32".to_string(),
            total_insertions,
            runtime,
            stats,
        });
        let eager_wheel_64: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(64)
            .build();

        let (runtime, stats) = run(seconds, eager_wheel_64, args);

        println!("Finished Eager Wheel 64");

        runs.push(Run {
            id: "Eager Wheel 64".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let pairs_fiba_4: PairsTree<tree::FiBA4> = external_impls::PairsTree::new(0, range, slide);
        let (runtime, stats) = run(seconds, pairs_fiba_4, args);
        println!("Finished Pairs FiBA Bfinger 4 Wheel");
        runs.push(Run {
            id: "Pairs FiBA Bfinger 4".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let pairs_fiba8: PairsTree<tree::FiBA8> = external_impls::PairsTree::new(0, range, slide);
        let (runtime, stats) = run(seconds, pairs_fiba8, args);
        println!("Finished Pairs FiBA Bfinger 8 Wheel");
        runs.push(Run {
            id: "Pairs FiBA Bfinger 8".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let pairs_btreemap: PairsTree<BTreeMap<u64, _>> =
            external_impls::PairsTree::new(0, range, slide);
        let (runtime, stats) = run(seconds, pairs_btreemap, args);
        println!("Finished Pairs BTreeMap");
        runs.push(Run {
            id: "Pairs BTreeMap".to_string(),
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
    plot_insert_rate(&results)
}

// focus: measure the impact of out of order data
fn insert_random_ooo_distance(args: &mut Args) {
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

        let lazy_wheel_8: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(8)
            .build();

        let (runtime, stats) = run(seconds, lazy_wheel_8, args);

        println!("Finished Lazy Wheel 8");

        runs.push(Run {
            id: "Lazy Wheel 8".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let lazy_wheel_32: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(32)
            .build();

        let (runtime, stats) = run(seconds, lazy_wheel_32, args);

        println!("Finished Lazy Wheel 32");

        runs.push(Run {
            id: "Lazy Wheel 32".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let lazy_wheel_64: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(64)
            .build();

        let (runtime, stats) = run(seconds, lazy_wheel_64, args);

        println!("Finished Lazy Wheel 64");

        runs.push(Run {
            id: "Lazy Wheel 64".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let eager_wheel_8: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(8)
            .build();

        let (runtime, stats) = run(seconds, eager_wheel_8, args);

        println!("Finished Eager Wheel 8");

        runs.push(Run {
            id: "Eager Wheel 8".to_string(),
            total_insertions,
            runtime,
            stats,
        });
        let eager_wheel_32: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(32)
            .build();

        let (runtime, stats) = run(seconds, eager_wheel_32, args);

        println!("Finished Eager Wheel 32");

        runs.push(Run {
            id: "Eager Wheel 32".to_string(),
            total_insertions,
            runtime,
            stats,
        });
        let eager_wheel_64: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(64)
            .build();

        let (runtime, stats) = run(seconds, eager_wheel_64, args);

        println!("Finished Eager Wheel 64");

        runs.push(Run {
            id: "Eager Wheel 64".to_string(),
            total_insertions,
            runtime,
            stats,
        });
        let pairs_fiba_4: PairsTree<tree::FiBA4> = external_impls::PairsTree::new(0, range, slide);
        let (runtime, stats) = run(seconds, pairs_fiba_4, args);
        println!("Finished Pairs FiBA Bfinger 4 Wheel");
        runs.push(Run {
            id: "Pairs FiBA Bfinger 4".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let pairs_fiba8: PairsTree<tree::FiBA8> = external_impls::PairsTree::new(0, range, slide);
        let (runtime, stats) = run(seconds, pairs_fiba8, args);
        println!("Finished Pairs FiBA Bfinger 8 Wheel");
        runs.push(Run {
            id: "Pairs FiBA Bfinger 8".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let pairs_btreemap: PairsTree<BTreeMap<u64, _>> =
            external_impls::PairsTree::new(0, range, slide);
        let (runtime, stats) = run(seconds, pairs_btreemap, args);
        println!("Finished Pairs BTreeMap");
        runs.push(Run {
            id: "Pairs BTreeMap".to_string(),
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
    plot_random_ooo_distance(&results)
}

#[cfg(feature = "plot")]
fn plot_random_ooo_distance(results: &Vec<Vec<Run>>) {
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = OUT_OF_ORDER_DISTANCE.iter().map(|m| *m as f64).collect();
    let mut lazy_8_y = Vec::new();
    let mut lazy_32_y = Vec::new();
    let mut lazy_64_y = Vec::new();
    let mut eager_8_y = Vec::new();
    let mut eager_32_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();
    let mut pairs_btreemap_y = Vec::new();

    let throughput =
        |run: &Run| (run.total_insertions as f64 / run.runtime.as_secs_f64()) / 1_000_000.0;
    for runs in results {
        // let runs = &res.runs;
        lazy_8_y.push(throughput(&runs[0]));
        lazy_32_y.push(throughput(&runs[1]));
        lazy_64_y.push(throughput(&runs[2]));
        eager_8_y.push(throughput(&runs[3]));
        eager_32_y.push(throughput(&runs[4]));
        eager_64_y.push(throughput(&runs[5]));
        pairs_bfinger_four_y.push(throughput(&runs[6]));
        pairs_bfinger_eight_y.push(throughput(&runs[7]));
        pairs_btreemap_y.push(throughput(&runs[8]));
    }

    let mut lazy_curve_8 = Curve::new();
    lazy_curve_8.set_label("Lazy Wheel W8");
    lazy_curve_8.set_line_color("g");
    lazy_curve_8.set_marker_style("o");
    lazy_curve_8.draw(&x, &lazy_8_y);

    let mut lazy_curve_32 = Curve::new();
    lazy_curve_32.set_label("Lazy Wheel W32");
    lazy_curve_32.set_line_color("g");
    lazy_curve_32.set_marker_style("^");
    lazy_curve_32.draw(&x, &lazy_32_y);

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("x");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut eager_curve_8 = Curve::new();
    eager_curve_8.set_label("Eager Wheel W8");
    eager_curve_8.set_marker_style("o");
    eager_curve_8.set_line_color("r");
    eager_curve_8.draw(&x, &eager_8_y);

    let mut eager_curve_32 = Curve::new();
    eager_curve_32.set_label("Eager Wheel W32");
    eager_curve_32.set_marker_style("^");
    eager_curve_32.set_line_color("r");
    eager_curve_32.draw(&x, &eager_32_y);

    let mut eager_curve_64 = Curve::new();
    eager_curve_64.set_label("Eager Wheel W64");
    eager_curve_64.set_marker_style("x");
    eager_curve_64.set_line_color("r");
    eager_curve_64.draw(&x, &eager_64_y);

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

    let mut pairs_btreemap_curve = Curve::new();
    pairs_btreemap_curve.set_label("Pairs + BTreeMap");
    pairs_btreemap_curve.set_line_color("c");
    pairs_btreemap_curve.set_marker_style("x");
    pairs_btreemap_curve.draw(&x, &pairs_btreemap_y);

    let mut legend = Legend::new();
    legend.set_outside(true);
    legend.set_num_col(2);
    legend.draw();

    // configure plot
    let mut plot = Plot::new();
    plot.set_super_title("")
        .set_horizontal_gap(0.5)
        .set_vertical_gap(0.5)
        .set_gaps(0.3, 0.2);

    plot.set_label_y("throughput [million records/s]");
    plot.set_log_x(true);
    plot.set_label_x("ooo distance [seconds]");

    plot.add(&lazy_curve_8)
        .add(&lazy_curve_32)
        .add(&lazy_curve_64)
        .add(&eager_curve_8)
        .add(&eager_curve_32)
        .add(&eager_curve_64)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve)
        .add(&pairs_btreemap_curve)
        .add(&legend);

    // modify manually in generated python code: plt.gca().set_xscale('log', base=2)
    let path = "../results/synthetic_window_random_ooo_distance.png";

    let path = Path::new(&path);
    plot.save(&path).unwrap();
}

#[cfg(feature = "plot")]
fn plot_insert_rate(results: &Vec<Vec<Run>>) {
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = EVENTS_PER_SECOND.iter().map(|m| *m as f64).collect();
    let mut lazy_8_y = Vec::new();
    let mut lazy_32_y = Vec::new();
    let mut lazy_64_y = Vec::new();
    let mut eager_8_y = Vec::new();
    let mut eager_32_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();
    let mut pairs_btreemap_y = Vec::new();

    // let throughput =
    // |run: &Run| (run.total_insertions as f64 / run.runtime.as_secs_f64()) / 1_000_000.0;
    let runtime = |run: &Run| run.runtime.as_secs_f64();

    for runs in results {
        // let runs = &res.runs;
        lazy_8_y.push(runtime(&runs[0]));
        lazy_32_y.push(runtime(&runs[1]));
        lazy_64_y.push(runtime(&runs[2]));
        eager_8_y.push(runtime(&runs[3]));
        eager_32_y.push(runtime(&runs[4]));
        eager_64_y.push(runtime(&runs[5]));
        pairs_bfinger_four_y.push(runtime(&runs[6]));
        pairs_bfinger_eight_y.push(runtime(&runs[7]));
        pairs_btreemap_y.push(runtime(&runs[8]));
    }

    let mut lazy_curve_8 = Curve::new();
    lazy_curve_8.set_label("Lazy Wheel W8");
    lazy_curve_8.set_line_color("g");
    lazy_curve_8.set_marker_style("o");
    lazy_curve_8.draw(&x, &lazy_8_y);

    let mut lazy_curve_32 = Curve::new();
    lazy_curve_32.set_label("Lazy Wheel W32");
    lazy_curve_32.set_line_color("g");
    lazy_curve_32.set_marker_style("^");
    lazy_curve_32.draw(&x, &lazy_32_y);

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("x");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut eager_curve_8 = Curve::new();
    eager_curve_8.set_label("Eager Wheel W8");
    eager_curve_8.set_marker_style("o");
    eager_curve_8.set_line_color("r");
    eager_curve_8.draw(&x, &eager_8_y);

    let mut eager_curve_32 = Curve::new();
    eager_curve_32.set_label("Eager Wheel W32");
    eager_curve_32.set_marker_style("^");
    eager_curve_32.set_line_color("r");
    eager_curve_32.draw(&x, &eager_32_y);

    let mut eager_curve_64 = Curve::new();
    eager_curve_64.set_label("Eager Wheel W64");
    eager_curve_64.set_marker_style("x");
    eager_curve_64.set_line_color("r");
    eager_curve_64.draw(&x, &eager_64_y);

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

    let mut pairs_btreemap_curve = Curve::new();
    pairs_btreemap_curve.set_label("Pairs + BTreeMap");
    pairs_btreemap_curve.set_line_color("c");
    pairs_btreemap_curve.set_marker_style("x");
    pairs_btreemap_curve.draw(&x, &pairs_btreemap_y);

    let mut legend = Legend::new();
    legend.set_outside(true);
    legend.set_num_col(2);
    legend.draw();

    // configure plot
    let mut plot = Plot::new();
    plot.set_super_title("")
        .set_horizontal_gap(0.5)
        .set_vertical_gap(0.5)
        .set_gaps(0.3, 0.2);

    plot.set_label_y("runtime (seconds)");
    plot.set_log_x(true);
    plot.set_label_x("events per second");

    plot.add(&lazy_curve_8)
        .add(&lazy_curve_32)
        .add(&lazy_curve_64)
        .add(&eager_curve_8)
        .add(&eager_curve_32)
        .add(&eager_curve_64)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve)
        .add(&pairs_btreemap_curve)
        .add(&legend);

    // modify manually in generated python code: plt.gca().set_xscale('log', base=2)
    let path = "../results/synthetic_window_insert_rate.png";

    let path = Path::new(&path);
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
