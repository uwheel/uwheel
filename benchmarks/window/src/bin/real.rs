use clap::{ArgEnum, Parser};
use csv::ReaderBuilder;
use minstant::Instant;
#[cfg(feature = "sync")]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::{cmp, fs::File};
use window::{external_impls::Slicing, PlottingOutput, Window};

use awheel::{
    aggregator::sum::U64SumAggregator,
    window::{eager, lazy, stats::Stats, wheels, WindowExt},
    Aggregator,
    Entry,
};
use chrono::{DateTime, NaiveDateTime};
use serde::Deserialize;
use window::{align_to_closest_thousand, external_impls::WindowTree, tree, BenchResult, Run};

use window::{BIG_RANGE_WINDOWS, SMALL_RANGE_WINDOWS};

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 100)]
    watermark_frequency: usize,
    #[clap(arg_enum, value_parser, default_value_t = Dataset::CitiBike)]
    data: Dataset,
    #[clap(arg_enum, value_parser, default_value_t = WindowType::SmallRange)]
    window_type: WindowType,
}

// DEBS 12 Event
#[derive(Debug, Deserialize)]
struct CDataPoint {
    ts: String,
    _index: u64,
    mf01: u32,
}

// DEBS 13 Event
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct FootballEvent {
    sid: u32,
    ts: u64,
}

#[inline]
pub fn datetime_to_u64(datetime: &str) -> u64 {
    let s = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S%.f").unwrap();
    s.timestamp_millis() as u64
}

pub fn debs_datetime_to_u64(datetime: &str) -> u64 {
    // Parse the timestamp string into a Chrono DateTime object
    let datetime = DateTime::parse_from_rfc3339(datetime).unwrap();
    datetime.naive_local().timestamp_millis() as u64
}

fn events_per_second(events: &[Event]) -> f64 {
    let start_time = events.first().map(|event| event.timestamp).unwrap_or(0);
    let end_time = events
        .last()
        .map(|event| event.timestamp)
        .unwrap_or(start_time);

    let total_events = events.len();
    let duration_seconds = (end_time - start_time) as f64 / 1000.0; // Convert milliseconds to seconds
    total_events as f64 / duration_seconds
}

fn calculate_out_of_order_percentage(watermark: u64, events: &[Event]) -> f64 {
    let mut out_of_order_count = 0;
    let mut total_events = 0;
    let mut current_max = watermark;

    for &event in events.iter() {
        total_events += 1;
        if event.timestamp < current_max {
            out_of_order_count += 1;
        } else {
            current_max = event.timestamp;
        }
    }

    // Calculate the percentage of out-of-order events
    (out_of_order_count as f64 / total_events as f64) * 100.0
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
struct Event {
    data: u64,
    timestamp: u64,
}

impl Event {
    pub fn from(trip: CitiBikeTrip) -> Self {
        Self {
            data: trip.tripduration as u64,
            timestamp: datetime_to_u64(&trip.starttime),
        }
    }
}

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Dataset {
    CitiBike,
    DEBS12,
    DEBS13,
}

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum WindowType {
    SmallRange,
    BigRange,
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

fn sum_aggregation(
    id: &str,
    events: Vec<Event>,
    watermark: u64,
    watermark_freq: usize,
    windows: Vec<Window>,
) {
    let mut results = Vec::new();

    for window in windows {
        let range = window.range;
        let slide = window.slide;
        let total_insertions = events.len() as u64;

        let mut runs = Vec::new();

        let wheel_64: wheels::WindowWheel<U64SumAggregator> = wheels::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(64)
            .with_watermark(watermark)
            .build();

        let (runtime, stats, _wheel_results) = run(wheel_64, &events, watermark, watermark_freq);

        dbg!("Finished Wheel 64");

        runs.push(Run {
            id: "Wheel 64".to_string(),
            total_insertions,
            runtime,
            stats,
            qps: None,
        });
        let wheel_512: wheels::WindowWheel<U64SumAggregator> = wheels::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(512)
            .with_watermark(watermark)
            .build();

        let (runtime, stats, _wheel_results) = run(wheel_512, &events, watermark, watermark_freq);

        dbg!("Finished Wheel 512");

        runs.push(Run {
            id: "Wheel 512".to_string(),
            total_insertions,
            runtime,
            stats,
            qps: None,
        });

        let lazy_wheel_64: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(64)
            .with_watermark(watermark)
            .build();

        #[cfg(feature = "sync")]
        let (gate, handle) = spawn_query_thread(&lazy_wheel_64);
        let (runtime, stats, lazy_results) = run(lazy_wheel_64, &events, watermark, watermark_freq);
        #[cfg(feature = "sync")]
        gate.store(false, Ordering::Relaxed);
        #[cfg(feature = "sync")]
        let qps = handle.join().unwrap();

        dbg!("Finished Lazy Wheel 64");

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
        let lazy_wheel_512: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(512)
            .with_watermark(watermark)
            .build();

        #[cfg(feature = "sync")]
        let (gate, handle) = spawn_query_thread(&lazy_wheel_512);
        let (runtime, stats, _lazy_results) =
            run(lazy_wheel_512, &events, watermark, watermark_freq);
        #[cfg(feature = "sync")]
        gate.store(false, Ordering::Relaxed);
        #[cfg(feature = "sync")]
        let qps = handle.join().unwrap();

        dbg!("Finished Lazy Wheel 512");

        runs.push(Run {
            id: "Lazy Wheel 512".to_string(),
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
        let (runtime, stats, eager_results) =
            run(eager_wheel_64, &events, watermark, watermark_freq);
        #[cfg(feature = "sync")]
        gate.store(false, Ordering::Relaxed);
        #[cfg(feature = "sync")]
        let qps = handle.join().unwrap();

        assert_eq!(lazy_results, eager_results);

        dbg!("Finished Eager Wheel W64");
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

        let eager_wheel_512: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(range)
            .with_slide(slide)
            .with_write_ahead(512)
            .with_watermark(watermark)
            .build();

        #[cfg(feature = "sync")]
        let (gate, handle) = spawn_query_thread(&eager_wheel_512);
        let (runtime, stats, eager_results) =
            run(eager_wheel_512, &events, watermark, watermark_freq);
        #[cfg(feature = "sync")]
        gate.store(false, Ordering::Relaxed);
        #[cfg(feature = "sync")]
        let qps = handle.join().unwrap();

        dbg!("Finished Eager Wheel W512");
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

        let fiba_4: WindowTree<tree::FiBA4> =
            WindowTree::new(watermark, range, slide, Slicing::Wheel);
        let (runtime, stats, _fiba4_results) = run(fiba_4, &events, watermark, watermark_freq);
        dbg!("Finished FiBA Bfinger4");
        runs.push(Run {
            id: "FiBA Bfinger4".to_string(),
            total_insertions,
            runtime,
            stats,
            qps: None,
        });

        pretty_assertions::assert_eq!(eager_results, _fiba4_results);
        /*
        dbg!(&find_first_mismatch(
            &lazy_results,
            &eager_results,
            &_fiba4_results
        ));
        */

        let fiba_8: WindowTree<tree::FiBA8> =
            WindowTree::new(watermark, range, slide, Slicing::Wheel);
        let (runtime, stats, _fiba8_results) = run(fiba_8, &events, watermark, watermark_freq);
        dbg!("Finished FiBA Bfinger8");
        runs.push(Run {
            id: "FiBA Bfinger8".to_string(),
            total_insertions,
            runtime,
            stats,
            qps: None,
        });
        assert_eq!(_fiba4_results, _fiba8_results);

        let fiba_4: WindowTree<tree::FiBA4> =
            WindowTree::new(watermark, range, slide, Slicing::Slide);
        let (runtime, stats, _fiba4_results) = run(fiba_4, &events, watermark, watermark_freq);
        dbg!("Finished FiBA CG Bfinger4");
        runs.push(Run {
            id: "FiBA CG Bfinger4".to_string(),
            total_insertions,
            runtime,
            stats,
            qps: None,
        });

        pretty_assertions::assert_eq!(eager_results, _fiba4_results);
        /*
        dbg!(&find_first_mismatch(
            &lazy_results,
            &eager_results,
            &_fiba4_results
        ));
        */

        let fiba_8: WindowTree<tree::FiBA8> =
            WindowTree::new(watermark, range, slide, Slicing::Slide);
        let (runtime, stats, _fiba8_results) = run(fiba_8, &events, watermark, watermark_freq);
        dbg!("Finished FiBA CG Bfinger8");
        runs.push(Run {
            id: "FiBA CG Bfinger8".to_string(),
            total_insertions,
            runtime,
            stats,
            qps: None,
        });
        assert_eq!(_fiba4_results, _fiba8_results);

        dbg!(window);

        let result = BenchResult::new(window, runs);
        result.print();
        results.push(result);
    }

    let output = PlottingOutput::from(id, watermark_freq, results);
    output.flush_to_file().unwrap();
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
        // TODO: this has to be updated to work with other datasets
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

fn main() {
    let args = Args::parse();
    let watermark_freq = args.watermark_frequency;
    println!("Running with {:#?}", args);
    let window_type = args.window_type;

    let id_gen = |id: &str| {
        let range = match window_type {
            WindowType::SmallRange => "small_range",
            WindowType::BigRange => "big_range",
        };
        format!("{}_{}", id, range)
    };

    let windows = match window_type {
        WindowType::SmallRange => SMALL_RANGE_WINDOWS,
        WindowType::BigRange => BIG_RANGE_WINDOWS,
    };

    match args.data {
        Dataset::CitiBike => {
            let path = "../data/citibike-tripdata.csv";
            let mut events = Vec::new();
            let mut rdr = csv::Reader::from_path(path).unwrap();
            println!("Preparing NYC Citi Bike Data");
            for result in rdr.deserialize() {
                let record: CitiBikeTrip = result.unwrap();
                let event = Event::from(record);
                events.push(event);
            }

            let watermark = datetime_to_u64("2018-08-01 00:00:00.0");
            let ooo_events = calculate_out_of_order_percentage(watermark, &events);
            println!("Out-of-order events {:.2}", ooo_events);
            println!("Events/s {}", events_per_second(&events));
            sum_aggregation(
                &id_gen("nyc_citi_bike"),
                events,
                watermark,
                watermark_freq,
                windows.to_vec(),
            );
        }
        Dataset::DEBS12 => {
            let watermark = debs_datetime_to_u64("2012-02-22T16:46:00.0+00:00");
            let path = "../data/debs12.csv";
            let mut events: Vec<Event> = Vec::new();
            let file = File::open(path).unwrap();
            let mut rdr = ReaderBuilder::new()
                .delimiter(b'\t')
                .flexible(true)
                .has_headers(false)
                .from_reader(file);

            println!("Preparing DEBS12 Data");
            for result in rdr.deserialize() {
                let data_point: CDataPoint = result.unwrap();
                let event = Event {
                    timestamp: debs_datetime_to_u64(&data_point.ts),
                    data: data_point.mf01 as u64,
                };
                events.push(event);
            }
            let ooo_events = calculate_out_of_order_percentage(watermark, &events);
            println!("Out-of-order events {:.2}", ooo_events);
            println!("Events/s {}", events_per_second(&events));
            sum_aggregation(
                &id_gen("debs12"),
                events,
                watermark,
                watermark_freq,
                windows.to_vec(),
            );
        }
        Dataset::DEBS13 => {
            // let watermark = debs_datetime_to_u64("2012-02-22T16:46:00.0+00:00");
            let watermark = pico_to_milli(10629342490369879);

            let path = "../data/debs13.csv";
            let mut events: Vec<Event> = Vec::new();

            fn pico_to_milli(picoseconds: u64) -> u64 {
                // Convert picoseconds to milliseconds (dividing by 1,000,000)
                // let milliseconds = picoseconds / 1_000_000;
                (picoseconds as f64 / 1e6).round() as u64
            }

            let file = File::open(path).unwrap();

            let mut rdr = ReaderBuilder::new()
                .flexible(true)
                .has_headers(false)
                .from_reader(file);
            println!("Preparing DEBS13 Data");
            for result in rdr.deserialize() {
                let record: FootballEvent = result.unwrap();
                let event = Event {
                    timestamp: pico_to_milli(record.ts),
                    data: 1, // a count-like sum aggregation
                };
                events.push(event);
            }

            let ooo_events = calculate_out_of_order_percentage(watermark, &events);
            println!("Total events {}", events.len());
            println!("Out-of-order events {:.2}", ooo_events);
            println!("Events/s {}", events_per_second(&events));
            sum_aggregation(
                &id_gen("debs13"),
                events,
                watermark,
                watermark_freq,
                windows.to_vec(),
            );
        }
    }
}

fn run<A: Aggregator<Input = u64, Aggregate = u64>>(
    mut window: impl WindowExt<A>,
    events: &[Event],
    watermark: u64,
    watermark_freq: usize,
) -> (std::time::Duration, Stats, Vec<(u64, Option<u64>)>) {
    dbg!(watermark);
    let mut watermark = watermark;
    let mut generator = WatermarkGenerator::new(watermark, 2000);
    let mut counter = 0;
    let mut _results = Vec::new();

    let full = Instant::now();
    for event in events {
        generator.on_event(&event.timestamp);
        window.insert(Entry::new(event.data, event.timestamp));

        counter += 1;

        if counter == watermark_freq {
            let wm = align_to_closest_thousand(generator.generate_watermark());
            if wm > watermark {
                watermark = wm;
            }
            counter = 0;
            for (_timestamp, _result) in window.advance_to(watermark) {
                #[cfg(feature = "debug")]
                _results.push((_timestamp, _result));
            }
        }
    }
    watermark = align_to_closest_thousand(generator.generate_watermark());
    for (_timestamp, _result) in window.advance_to(watermark) {
        #[cfg(feature = "debug")]
        _results.push((_timestamp, _result));
    }
    dbg!(watermark);

    let runtime = full.elapsed();
    (runtime, window.stats().clone(), _results)
}

// For debugging purposes
/*
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
