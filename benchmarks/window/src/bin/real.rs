use minstant::Instant;
#[cfg(feature = "sync")]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::{cmp, collections::BTreeMap};

use awheel::{
    aggregator::{min::U64MinAggregator, sum::U64SumAggregator},
    window::{eager, lazy, stats::Stats, WindowExt},
    Aggregator,
    Entry,
};
use chrono::NaiveDateTime;
use serde::Deserialize;
use window::{
    align_to_closest_thousand,
    external_impls::{self, PairsTree},
    tree,
    BenchResult,
    Run,
};

use window::EXECUTIONS;

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

        #[cfg(feature = "sync")]
        let (gate, handle) = spawn_query_thread(&lazy_wheel_64);
        let (runtime, stats, lazy_results) = run(lazy_wheel_64, &events);
        #[cfg(feature = "sync")]
        gate.store(false, Ordering::Relaxed);
        #[cfg(feature = "sync")]
        let qps = handle.join().unwrap();

        println!("Finished Lazy Wheel 64");

        runs.push(Run {
            id: "Lazy Wheel 64".to_string(),
            total_insertions,
            runtime,
            stats,
            #[cfg(feature = "sync")]
            qps: Some(qps),
            #[cfg(not(feature = "sync"))]
            qps: None,
        });

        let lazy_wheel_256: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(256)
            .with_watermark(watermark)
            .build();

        #[cfg(feature = "sync")]
        let (gate, handle) = spawn_query_thread(&lazy_wheel_256);
        let (runtime, stats, _lazy_results) = run(lazy_wheel_256, &events);
        #[cfg(feature = "sync")]
        gate.store(false, Ordering::Relaxed);
        #[cfg(feature = "sync")]
        let qps = handle.join().unwrap();

        println!("Finished Lazy Wheel 256");

        runs.push(Run {
            id: "Lazy Wheel 256".to_string(),
            total_insertions,
            runtime,
            stats,
            #[cfg(feature = "sync")]
            qps: Some(qps),
            #[cfg(not(feature = "sync"))]
            qps: None,
        });

        let lazy_wheel_512: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(512)
            .with_watermark(watermark)
            .build();

        #[cfg(feature = "sync")]
        let (gate, handle) = spawn_query_thread(&lazy_wheel_512);
        let (runtime, stats, _lazy_results) = run(lazy_wheel_512, &events);
        #[cfg(feature = "sync")]
        gate.store(false, Ordering::Relaxed);
        #[cfg(feature = "sync")]
        let qps = handle.join().unwrap();

        println!("Finished Lazy Wheel 512");

        runs.push(Run {
            id: "Lazy Wheel 256".to_string(),
            total_insertions,
            runtime,
            stats,
            #[cfg(feature = "sync")]
            qps: Some(qps),
            #[cfg(not(feature = "sync"))]
            qps: None,
        });

        // EAGER
        let eager_wheel_64: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(64)
            .with_watermark(watermark)
            .build();

        #[cfg(feature = "sync")]
        let (gate, handle) = spawn_query_thread(&eager_wheel_64);
        let (runtime, stats, eager_results) = run(eager_wheel_64, &events);
        #[cfg(feature = "sync")]
        gate.store(false, Ordering::Relaxed);
        #[cfg(feature = "sync")]
        let qps = handle.join().unwrap();

        assert_eq!(lazy_results, eager_results);

        println!("Finished Eager Wheel W64");
        runs.push(Run {
            id: "Eager Wheel W64".to_string(),
            total_insertions,
            runtime,
            stats,
            #[cfg(feature = "sync")]
            qps: Some(qps),
            #[cfg(not(feature = "sync"))]
            qps: None,
        });

        let eager_wheel_256: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(256)
            .with_watermark(watermark)
            .build();

        #[cfg(feature = "sync")]
        let (gate, handle) = spawn_query_thread(&eager_wheel_256);
        let (runtime, stats, _eager_results) = run(eager_wheel_256, &events);
        #[cfg(feature = "sync")]
        gate.store(false, Ordering::Relaxed);
        #[cfg(feature = "sync")]
        let qps = handle.join().unwrap();

        println!("Finished Eager Wheel W256");
        runs.push(Run {
            id: "Eager Wheel W256".to_string(),
            total_insertions,
            runtime,
            stats,
            #[cfg(feature = "sync")]
            qps: Some(qps),
            #[cfg(not(feature = "sync"))]
            qps: None,
        });

        let eager_wheel_512: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(512)
            .with_watermark(watermark)
            .build();

        #[cfg(feature = "sync")]
        let (gate, handle) = spawn_query_thread(&eager_wheel_512);
        let (runtime, stats, eager_results) = run(eager_wheel_512, &events);
        #[cfg(feature = "sync")]
        gate.store(false, Ordering::Relaxed);
        #[cfg(feature = "sync")]
        let qps = handle.join().unwrap();

        println!("Finished Eager Wheel W512");
        runs.push(Run {
            id: "Eager Wheel W512".to_string(),
            total_insertions,
            runtime,
            stats,
            #[cfg(feature = "sync")]
            qps: Some(qps),
            #[cfg(not(feature = "sync"))]
            qps: None,
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
            qps: None,
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
            qps: None,
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
            qps: None,
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
    window::plot_window("nyc_citi_bike", &results);
}

#[cfg(feature = "sync")]
fn spawn_query_thread(
    window: &impl WindowExt<U64SumAggregator>,
) -> (Arc<AtomicBool>, std::thread::JoinHandle<f64>) {
    use awheel::time::Duration;
    let read_wheel = window.wheel().clone();
    let gate = Arc::new(AtomicBool::new(true));
    let inner_gate = gate.clone();
    let handle = std::thread::spawn(move || {
        let watermark = datetime_to_u64("2018-08-01 00:00:00.0");
        // wait with querying until the wheel has been advanced 24 hours
        loop {
            use awheel::time;
            if read_wheel.watermark()
                > watermark + time::Duration::hours(24).whole_milliseconds() as u64
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        let now = Instant::now();
        let mut counter = 0;
        while inner_gate.load(Ordering::Relaxed) {
            // Execute queries on random granularities
            let pick = fastrand::usize(0..3);
            if pick == 0 {
                let _res = read_wheel.interval(Duration::seconds(fastrand::i64(1..60)));
            } else if pick == 1 {
                let _res = read_wheel.interval(Duration::minutes(fastrand::i64(1..60)));
            } else {
                let _res = read_wheel.interval(Duration::hours(fastrand::i64(1..24)));
            }
            counter += 1;
        }
        (counter as f64 / now.elapsed().as_secs_f64()) / 1_000_000.0
        // println!(
        //     "Concurrent Read task ran at {} Mops/s",
        //     (counter as f64 / now.elapsed().as_secs_f64()) as u64 / 1_000_000
        // );
    });
    (gate, handle)
}

fn _nyc_citi_bike_bench_min() {
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
        let lazy_wheel_64: lazy::LazyWindowWheel<U64MinAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(64)
            .with_watermark(watermark)
            .build();

        let (runtime, stats, _lazy_results) = run(lazy_wheel_64, &events);
        println!("Finished Lazy Wheel 64");

        runs.push(Run {
            id: "Lazy Wheel 64".to_string(),
            total_insertions,
            runtime,
            stats,
            qps: None,
        });

        let lazy_wheel_256: lazy::LazyWindowWheel<U64MinAggregator> = lazy::Builder::default()
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
            qps: None,
        });

        let lazy_wheel_512: lazy::LazyWindowWheel<U64MinAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(512)
            .with_watermark(watermark)
            .build();

        let (runtime, stats, _lazy_results) = run(lazy_wheel_512, &events);
        println!("Finished Lazy Wheel 512");

        runs.push(Run {
            id: "Lazy Wheel 512".to_string(),
            total_insertions,
            runtime,
            stats,
            qps: None,
        });

        let result = BenchResult::new(exec, runs);
        result.print();
        results.push(result);
    }
}

fn main() {
    nyc_citi_bike_bench_sum();
    // TODO: add non-inversible aggregation function
    // nyc_citi_bike_bench_min();
}

fn run<A: Aggregator<Input = u64, Aggregate = u64>>(
    mut window: impl WindowExt<A>,
    events: &[CitiBikeEvent],
) -> (std::time::Duration, Stats, Vec<(u64, Option<u64>)>) {
    let mut watermark = datetime_to_u64("2018-08-01 00:00:00.0");
    dbg!(watermark);
    let mut generator = WatermarkGenerator::new(watermark, 2000);
    let mut counter = 0;
    let mut results = Vec::new();

    let full = Instant::now();
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
