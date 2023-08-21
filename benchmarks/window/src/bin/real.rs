use std::cmp;

use awheel::{
    aggregator::sum::U64SumAggregator,
    time::Duration,
    window::{eager, lazy, stats::Stats, WindowExt},
    Entry,
};
use chrono::NaiveDateTime;
use serde::Deserialize;
use window::{align_to_closest_thousand, btreemap_wheel, fiba_wheel, BenchResult, Execution, Run};

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

fn main() {
    let path = "../data/citibike-tripdata.csv";
    let mut events = Vec::new();
    let mut rdr = csv::Reader::from_path(path).unwrap();
    for result in rdr.deserialize() {
        let record: CitiBikeTrip = result.unwrap();
        let event = CitiBikeEvent::from(record);
        events.push(event);
    }

    let watermark = datetime_to_u64("2018-08-01 00:00:00.0");
    let mut runs = Vec::new();
    let range = Duration::seconds(30);
    let slide = Duration::seconds(10);
    let exec = Execution::new(30, 10);
    let total_insertions = events.len() as u64;
    dbg!(total_insertions);

    let lazy_wheel: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
        .with_range(range)
        .with_slide(slide)
        .with_watermark(watermark)
        .build();

    let (runtime, stats, lazy_results) = run(lazy_wheel, &events);
    println!("Finished Lazy Wheel");

    runs.push(Run {
        id: "Lazy Wheel".to_string(),
        total_insertions,
        runtime,
        stats,
    });

    let eager_wheel: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
        .with_range(range)
        .with_slide(slide)
        .with_watermark(watermark)
        .build();

    let (runtime, stats, eager_results) = run(eager_wheel, &events);
    assert_eq!(lazy_results, eager_results);

    println!("Finished Eager Wheel");
    runs.push(Run {
        id: "Eager Wheel".to_string(),
        total_insertions,
        runtime,
        stats,
    });

    let btreemap_wheel = btreemap_wheel::BTreeMapWheel::new(watermark, range, slide);
    let (runtime, stats, btreewheel_results) = run(btreemap_wheel, &events);
    println!("Finished BTreeMap Wheel");
    runs.push(Run {
        id: "BTreeMap Wheel".to_string(),
        total_insertions,
        runtime,
        stats,
    });

    assert_eq!(eager_results, btreewheel_results);

    let pairs_fiba = fiba_wheel::PairsFiBA::new(watermark, range, slide);
    let (runtime, stats, pairs_fiba_results) = run(pairs_fiba, &events);
    println!("Finished Pairs FiBA Wheel");
    runs.push(Run {
        id: "Pairs FiBA".to_string(),
        total_insertions,
        runtime,
        stats,
    });
    assert_eq!(btreewheel_results, pairs_fiba_results);

    let cg_bfinger_four_wheel = fiba_wheel::BFingerFourWheel::new(watermark, range, slide);
    let (runtime, stats, bfinger4_results) = run(cg_bfinger_four_wheel, &events);
    runs.push(Run {
        id: "FiBA CG BFinger4".to_string(),
        total_insertions,
        runtime,
        stats,
    });
    assert_eq!(pairs_fiba_results, bfinger4_results);

    let cg_bfinger_eight_wheel = fiba_wheel::BFingerEightWheel::new(watermark, range, slide);
    let (runtime, stats, bfinger_8_results) = run(cg_bfinger_eight_wheel, &events);
    runs.push(Run {
        id: "FiBA CG BFinger8".to_string(),
        total_insertions,
        runtime,
        stats,
    });
    assert_eq!(bfinger4_results, bfinger_8_results);

    let result = BenchResult::new(exec, runs);
    result.print();
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

/*
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
*/
