use std::cmp;

use awheel::{
    aggregator::sum::U64SumAggregator,
    time::Duration,
    window::{eager, lazy, stats::Stats, WindowExt},
    Entry,
};
use chrono::NaiveDateTime;
use serde::Deserialize;
use window::{align_to_closest_thousand, fiba_wheel, BenchResult, Execution, Run};

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
        //println!("{:?}", event);
        events.push(event);
    }

    let watermark = datetime_to_u64("2018-09-01 00:00:00.0");
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

    let (runtime, stats, _results) = run(eager_wheel, &events);
    println!("Finished Eager Wheel");
    runs.push(Run {
        id: "Eager Wheel".to_string(),
        total_insertions,
        runtime,
        stats,
    });

    let cg_bfinger_four_wheel = fiba_wheel::BFingerFourWheel::new(watermark, range, slide);
    let (runtime, stats, bfinger4_results) = run(cg_bfinger_four_wheel, &events);
    assert_eq!(lazy_results, bfinger4_results);
    runs.push(Run {
        id: "FiBA CG BFinger4".to_string(),
        total_insertions,
        runtime,
        stats,
    });

    /*
    let cg_bfinger_eight_wheel = fiba_wheel::BFingerEightWheel::new(watermark, range, slide);
    let (runtime, stats, bfinger_8_results) = run(cg_bfinger_eight_wheel, &events);
    runs.push(Run {
        id: "FiBA CG BFinger8".to_string(),
        total_insertions,
        runtime,
        stats,
    });
    */

    let result = BenchResult::new(exec, runs);
    result.print();
}

fn run(
    mut window: impl WindowExt<U64SumAggregator>,
    events: &[CitiBikeEvent],
) -> (std::time::Duration, Stats, Vec<(u64, Option<u64>)>) {
    let mut watermark = datetime_to_u64("2018-09-01 00:00:00.0");
    dbg!(watermark);
    let mut generator = WatermarkGenerator::new(watermark, 3500);
    let mut counter = 0;
    let mut overflow_counter = 0;
    let mut results = Vec::new();

    let full = minstant::Instant::now();
    for event in events {
        generator.on_event(&event.start_time);

        if let Err(err) = window.insert(Entry::new(
            event.trip_duration,
            //align_to_closest_thousand(event.start_time),
            event.start_time,
        )) {
            if err.is_overflow() {
                overflow_counter += 1;
            }
        }
        counter += 1;

        if counter == 10 {
            //println!("I am alive and processed {} so far", processed);
            //let wm = align_to_closest_thousand(generator.generate_watermark());
            let wm = align_to_closest_thousand(generator.generate_watermark());
            if wm > watermark {
                watermark = wm;
            }
            counter = 0;
            for (_timestamp, _result) in window.advance_to(watermark) {
                results.push((_timestamp, _result));
                if let Some(_results) = _result {
                    if _results > 0 {
                        //println!("Window at {} with data {:?}", _timestamp, _results);
                    }
                }
            }
        }
    }
    watermark = align_to_closest_thousand(generator.generate_watermark());
    for (_timestamp, _result) in window.advance_to(watermark) {
        //log!("Window at {} with data {:?}", _timestamp, _result);
    }
    println!("Overflow counter {}", overflow_counter);

    let runtime = full.elapsed();
    (runtime, window.stats().clone(), results)
}
