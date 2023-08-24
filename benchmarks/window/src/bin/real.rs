use std::{cmp, collections::BTreeMap};

use awheel::{
    aggregator::sum::U64SumAggregator,
    time::Duration,
    window::{eager, lazy, stats::Stats, WindowExt},
    Entry,
};
use chrono::NaiveDateTime;
use serde::Deserialize;
use window::{
    align_to_closest_thousand,
    external_impls::{self, PairsTree},
    tree,
    BenchResult,
    Execution,
    Run,
};

const EXECUTIONS: [Execution; 8] = [
    Execution::new(Duration::seconds(30), Duration::seconds(10)),
    Execution::new(Duration::seconds(60), Duration::seconds(10)),
    Execution::new(Duration::minutes(15), Duration::seconds(10)),
    Execution::new(Duration::minutes(30), Duration::seconds(10)),
    Execution::new(Duration::hours(1), Duration::seconds(10)),
    Execution::new(Duration::hours(12), Duration::seconds(10)),
    Execution::new(Duration::days(1), Duration::seconds(10)),
    Execution::new(Duration::weeks(1), Duration::seconds(10)),
];

#[inline]
pub fn datetime_to_u64(datetime: &str) -> u64 {
    let s = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S%.f").unwrap();
    s.timestamp_millis() as u64
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CitiBikeTrip {
    tripduration: u32,
    starttime: String,
    stoptime: String,
    start_station_id: Option<u32>,
    start_station_name: Option<String>,
    start_station_latitude: Option<f64>,
    start_station_longitude: Option<f64>,
    end_station_id: Option<u32>,
    end_station_name: Option<String>,
    end_station_latitude: Option<f64>,
    end_station_longitude: Option<f64>,
    bikeid: Option<u32>,
    usertype: Option<String>,
    birth_year: Option<u32>,
    gender: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
struct CitiBikeEvent {
    trip_duration: u64,
    start_time: u64,
}

impl CitiBikeEvent {
    pub fn from(trip: CitiBikeTrip) -> Self {
        Self {
            trip_duration: trip.tripduration as u64,
            start_time: datetime_to_u64(&trip.starttime),
        }
    }
}

struct WatermarkGenerator {
    max_ooo: u64,
    current_max: u64,
}

impl WatermarkGenerator {
    pub fn new(start: u64, max_ooo: u64) -> Self {
        Self {
            max_ooo,
            current_max: start,
        }
    }
    #[inline]
    pub fn on_event(&mut self, ts: &u64) {
        self.current_max = cmp::max(self.current_max, *ts);
    }
    // generate a watermark with a max out-of-ordeness
    #[inline]
    pub fn generate_watermark(&self) -> u64 {
        self.current_max - self.max_ooo - 1
    }
}
fn nyc_citi_bike_bench_sum() {
    let path = "../data/citibike-tripdata.csv";
    let mut events = Vec::new();
    let mut rdr = csv::Reader::from_path(path).unwrap();
    for result in rdr.deserialize() {
        let record: CitiBikeTrip = result.unwrap();
        let event = CitiBikeEvent::from(record);
        events.push(event);
    }

    let watermark = datetime_to_u64("2018-08-01 00:00:00.0");
    let mut results = Vec::new();

    for exec in EXECUTIONS {
        let range = exec.range;
        let slide = exec.slide;
        let total_insertions = events.len() as u64;

        let mut runs = Vec::new();
        let lazy_wheel_64: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(64)
            .with_watermark(watermark)
            .build();

        let (runtime, stats, lazy_results) = run(lazy_wheel_64, &events);
        println!("Finished Lazy Wheel 64");

        runs.push(Run {
            id: "Lazy Wheel 64".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let lazy_wheel_256: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(256)
            .with_watermark(watermark)
            .build();

        let (runtime, stats, _lazy_results) = run(lazy_wheel_256, &events);
        println!("Finished Lazy Wheel 256");

        runs.push(Run {
            id: "Lazy Wheel 256".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let lazy_wheel_512: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(512)
            .with_watermark(watermark)
            .build();

        let (runtime, stats, _lazy_results) = run(lazy_wheel_512, &events);
        println!("Finished Lazy Wheel 512");

        runs.push(Run {
            id: "Lazy Wheel 256".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        // EAGER

        let eager_wheel_64: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(64)
            .with_watermark(watermark)
            .build();

        let (runtime, stats, eager_results) = run(eager_wheel_64, &events);

        assert_eq!(lazy_results, eager_results);

        println!("Finished Eager Wheel W64");
        runs.push(Run {
            id: "Eager Wheel W64".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let eager_wheel_256: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(256)
            .with_watermark(watermark)
            .build();

        let (runtime, stats, _eager_results) = run(eager_wheel_256, &events);

        println!("Finished Eager Wheel W256");
        runs.push(Run {
            id: "Eager Wheel W256".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let eager_wheel_512: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(512)
            .with_watermark(watermark)
            .build();

        let (runtime, stats, eager_results) = run(eager_wheel_512, &events);

        println!("Finished Eager Wheel W512");
        runs.push(Run {
            id: "Eager Wheel W512".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let pairs_fiba_4: PairsTree<tree::FiBA4> =
            external_impls::PairsTree::new(watermark, range, slide);
        let (runtime, stats, _pairs_fiba_results) = run(pairs_fiba_4, &events);
        println!("Finished Pairs FiBA Bfinger 4 Wheel");
        runs.push(Run {
            id: "Pairs FiBA Bfinger 4".to_string(),
            total_insertions,
            runtime,
            stats,
        });
        assert_eq!(eager_results, _pairs_fiba_results);

        let pairs_fiba8: PairsTree<tree::FiBA8> =
            external_impls::PairsTree::new(watermark, range, slide);
        let (runtime, stats, _) = run(pairs_fiba8, &events);
        println!("Finished Pairs FiBA Bfinger 8 Wheel");
        runs.push(Run {
            id: "Pairs FiBA Bfinger 8".to_string(),
            total_insertions,
            runtime,
            stats,
        });

        let pairs_btreemap: PairsTree<BTreeMap<u64, _>> =
            external_impls::PairsTree::new(watermark, range, slide);
        let (runtime, stats, btreemap_results) = run(pairs_btreemap, &events);
        println!("Finished Pairs BTreeMap");
        runs.push(Run {
            id: "Pairs BTreeMap".to_string(),
            total_insertions,
            runtime,
            stats,
        });
        /*
        let mismatches =
            find_first_mismatch(&eager_results, &_pairs_fiba_results, &btreemap_results);
        dbg!(mismatches);
        */
        assert_eq!(eager_results, btreemap_results);
        assert_eq!(_pairs_fiba_results, btreemap_results);

        let result = BenchResult::new(exec, runs);
        result.print();
        results.push(result);
    }
    #[cfg(feature = "plot")]
    plot_nyc_citi_bike(&results);

    #[cfg(feature = "plot")]
    plot_nyc_citi_bike_window_latency(&results);

    #[cfg(feature = "plot")]
    plot_nyc_citi_bike_insert_latency(&results);
}

fn main() {
    nyc_citi_bike_bench_sum();
    // TODO: add non-inversible aggregation function
}

fn run(
    mut window: impl WindowExt<U64SumAggregator>,
    events: &[CitiBikeEvent],
) -> (std::time::Duration, Stats, Vec<(u64, Option<u64>)>) {
    let mut watermark = datetime_to_u64("2018-08-01 00:00:00.0");
    dbg!(watermark);
    let mut generator = WatermarkGenerator::new(watermark, 2000);
    let mut counter = 0;
    let mut results = Vec::new();

    let full = minstant::Instant::now();
    for event in events {
        generator.on_event(&event.start_time);
        window.insert(Entry::new(event.trip_duration, event.start_time));

        counter += 1;

        if counter == 100 {
            let wm = align_to_closest_thousand(generator.generate_watermark());
            if wm > watermark {
                watermark = wm;
            }
            counter = 0;
            for (_timestamp, _result) in window.advance_to(watermark) {
                results.push((_timestamp, _result));
            }
        }
    }
    watermark = align_to_closest_thousand(generator.generate_watermark());
    for (_timestamp, _result) in window.advance_to(watermark) {
        results.push((_timestamp, _result));
    }

    let runtime = full.elapsed();
    (runtime, window.stats().clone(), results)
}

/*
// For debugging purposes
fn find_first_mismatch<T: PartialEq + Copy>(
    vec1: &[T],
    vec2: &[T],
    vec3: &[T],
) -> Vec<(usize, T, T, T)> {
    let min_len = std::cmp::min(vec1.len(), std::cmp::min(vec2.len(), vec3.len()));
    let mut mismatches = Vec::new();

    for i in 0..min_len {
        if vec1[i] != vec2[i] || vec1[i] != vec3[i] {
            mismatches.push((i, vec1[i], vec2[i], vec3[i]));
        }
    }
    mismatches
}
*/

#[cfg(feature = "plot")]
fn plot_nyc_citi_bike(results: &Vec<BenchResult>) {
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = EXECUTIONS
        .iter()
        .map(|m| m.range_seconds() as f64)
        .collect();
    let mut lazy_64_y = Vec::new();
    let mut lazy_256_y = Vec::new();
    let mut lazy_512_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut eager_256_y = Vec::new();
    let mut eager_512_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();
    let mut pairs_btreemap_y = Vec::new();

    let throughput =
        |run: &Run| (run.total_insertions as f64 / run.runtime.as_secs_f64()) / 1_000_000.0;
    for res in results {
        let runs = &res.runs;
        lazy_64_y.push(throughput(&runs[0]));
        lazy_256_y.push(throughput(&runs[1]));
        lazy_512_y.push(throughput(&runs[2]));
        eager_64_y.push(throughput(&runs[3]));
        eager_256_y.push(throughput(&runs[4]));
        eager_512_y.push(throughput(&runs[5]));
        pairs_bfinger_four_y.push(throughput(&runs[6]));
        pairs_bfinger_eight_y.push(throughput(&runs[7]));
        pairs_btreemap_y.push(throughput(&runs[8]));
    }

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("o");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut lazy_curve_256 = Curve::new();
    lazy_curve_256.set_label("Lazy Wheel W256");
    lazy_curve_256.set_line_color("g");
    lazy_curve_256.set_marker_style("^");
    lazy_curve_256.draw(&x, &lazy_256_y);

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

    let mut eager_curve_256 = Curve::new();
    eager_curve_256.set_label("Eager Wheel W256");
    eager_curve_256.set_marker_style("^");
    eager_curve_256.set_line_color("r");
    eager_curve_256.draw(&x, &eager_256_y);

    let mut eager_curve_512 = Curve::new();
    eager_curve_512.set_label("Eager Wheel W512");
    eager_curve_512.set_marker_style("x");
    eager_curve_512.set_line_color("r");
    eager_curve_512.draw(&x, &eager_512_y);

    /*
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
    */
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
    plot.set_label_x("Window range");

    plot.add(&lazy_curve_64)
        .add(&lazy_curve_256)
        .add(&lazy_curve_512)
        .add(&eager_curve_64)
        .add(&eager_curve_256)
        .add(&eager_curve_512)
        //.add(&bfinger_four_curve)
        //.add(&bfinger_eight_curve)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve)
        .add(&pairs_btreemap_curve)
        .add(&legend);

    // modify manually in generated python code: plt.gca().set_xscale('log', base=2)
    let path = Path::new("../results/nyc_citi_bike_window_throughput.png");
    plot.save(&path).unwrap();
}

#[cfg(feature = "plot")]
fn plot_nyc_citi_bike_window_latency(results: &Vec<BenchResult>) {
    use awheel::stats::Percentiles;
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = EXECUTIONS
        .iter()
        .map(|m| m.range_seconds() as f64)
        .collect();
    let mut lazy_64_y = Vec::new();
    let mut lazy_256_y = Vec::new();
    let mut lazy_512_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut eager_256_y = Vec::new();
    let mut eager_512_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();
    let mut pairs_btreemap_y = Vec::new();

    // plot p99 latency
    let p99_latency = |p: Percentiles| p.p99;
    for res in results {
        let runs = &res.runs;
        lazy_64_y.push(p99_latency(
            runs[0].stats.window_computation_ns.percentiles(),
        ));
        lazy_256_y.push(p99_latency(
            runs[1].stats.window_computation_ns.percentiles(),
        ));
        lazy_512_y.push(p99_latency(
            runs[2].stats.window_computation_ns.percentiles(),
        ));
        eager_64_y.push(p99_latency(
            runs[3].stats.window_computation_ns.percentiles(),
        ));
        eager_256_y.push(p99_latency(
            runs[4].stats.window_computation_ns.percentiles(),
        ));
        eager_512_y.push(p99_latency(
            runs[5].stats.window_computation_ns.percentiles(),
        ));
        pairs_bfinger_four_y.push(p99_latency(
            runs[6].stats.window_computation_ns.percentiles(),
        ));
        pairs_bfinger_eight_y.push(p99_latency(
            runs[7].stats.window_computation_ns.percentiles(),
        ));
        pairs_btreemap_y.push(p99_latency(
            runs[8].stats.window_computation_ns.percentiles(),
        ));
    }

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("o");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut lazy_curve_256 = Curve::new();
    lazy_curve_256.set_label("Lazy Wheel W256");
    lazy_curve_256.set_line_color("g");
    lazy_curve_256.set_marker_style("^");
    lazy_curve_256.draw(&x, &lazy_256_y);

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

    let mut eager_curve_256 = Curve::new();
    eager_curve_256.set_label("Eager Wheel W256");
    eager_curve_256.set_marker_style("^");
    eager_curve_256.set_line_color("r");
    eager_curve_256.draw(&x, &eager_256_y);

    let mut eager_curve_512 = Curve::new();
    eager_curve_512.set_label("Eager Wheel W512");
    eager_curve_512.set_marker_style("x");
    eager_curve_512.set_line_color("r");
    eager_curve_512.draw(&x, &eager_512_y);

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
    plot.set_super_title("NYC Citi Bike (Sum Aggregation)")
        .set_horizontal_gap(0.5)
        .set_vertical_gap(0.5)
        .set_gaps(0.3, 0.2);

    plot.set_label_y("Window computation p99 latency (nanoseconds)");
    plot.set_log_x(true);
    plot.set_label_x("Window range");

    plot.add(&lazy_curve_64)
        .add(&lazy_curve_256)
        .add(&lazy_curve_512)
        .add(&eager_curve_64)
        .add(&eager_curve_256)
        .add(&eager_curve_512)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve)
        .add(&pairs_btreemap_curve)
        .add(&legend);

    // modify manually in generated python code: plt.gca().set_xscale('log', base=2)
    let path = Path::new("../results/nyc_citi_bike_window_computation_latency.png");
    plot.save(&path).unwrap();
}

#[cfg(feature = "plot")]
fn plot_nyc_citi_bike_insert_latency(results: &Vec<BenchResult>) {
    use awheel::stats::Percentiles;
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = EXECUTIONS
        .iter()
        .map(|m| m.range_seconds() as f64)
        .collect();
    let mut lazy_64_y = Vec::new();
    let mut lazy_256_y = Vec::new();
    let mut lazy_512_y = Vec::new();
    let mut eager_64_y = Vec::new();
    let mut eager_256_y = Vec::new();
    let mut eager_512_y = Vec::new();
    let mut pairs_bfinger_four_y = Vec::new();
    let mut pairs_bfinger_eight_y = Vec::new();
    let mut pairs_btreemap_y = Vec::new();

    // plot p99 latency
    let p99_latency = |p: Percentiles| p.p99;
    for res in results {
        let runs = &res.runs;
        lazy_64_y.push(p99_latency(runs[0].stats.insert_ns.percentiles()));
        lazy_256_y.push(p99_latency(runs[1].stats.insert_ns.percentiles()));
        lazy_512_y.push(p99_latency(runs[2].stats.insert_ns.percentiles()));
        eager_64_y.push(p99_latency(runs[3].stats.insert_ns.percentiles()));
        eager_256_y.push(p99_latency(runs[4].stats.insert_ns.percentiles()));
        eager_512_y.push(p99_latency(runs[5].stats.insert_ns.percentiles()));
        pairs_bfinger_four_y.push(p99_latency(runs[6].stats.insert_ns.percentiles()));
        pairs_bfinger_eight_y.push(p99_latency(runs[7].stats.insert_ns.percentiles()));
        pairs_btreemap_y.push(p99_latency(runs[8].stats.insert_ns.percentiles()));
    }

    let mut lazy_curve_64 = Curve::new();
    lazy_curve_64.set_label("Lazy Wheel W64");
    lazy_curve_64.set_line_color("g");
    lazy_curve_64.set_marker_style("o");
    lazy_curve_64.draw(&x, &lazy_64_y);

    let mut lazy_curve_256 = Curve::new();
    lazy_curve_256.set_label("Lazy Wheel W256");
    lazy_curve_256.set_line_color("g");
    lazy_curve_256.set_marker_style("^");
    lazy_curve_256.draw(&x, &lazy_256_y);

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

    let mut eager_curve_256 = Curve::new();
    eager_curve_256.set_label("Eager Wheel W256");
    eager_curve_256.set_marker_style("^");
    eager_curve_256.set_line_color("r");
    eager_curve_256.draw(&x, &eager_256_y);

    let mut eager_curve_512 = Curve::new();
    eager_curve_512.set_label("Eager Wheel W512");
    eager_curve_512.set_marker_style("x");
    eager_curve_512.set_line_color("r");
    eager_curve_512.draw(&x, &eager_512_y);

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

    plot.set_label_y("Insert p99 latency (nanoseconds)");
    plot.set_log_x(true);
    plot.set_label_x("Window range");

    plot.add(&lazy_curve_64)
        .add(&lazy_curve_256)
        .add(&lazy_curve_512)
        .add(&eager_curve_64)
        .add(&eager_curve_256)
        .add(&eager_curve_512)
        .add(&pairs_fiba_b4_curve)
        .add(&pairs_fiba_b8_curve)
        .add(&pairs_btreemap_curve)
        .add(&legend);

    // modify manually in generated python code: plt.gca().set_xscale('log', base=2)
    let path = Path::new("../results/nyc_citi_bike_window_insert_latency.png");
    plot.save(&path).unwrap();
}
