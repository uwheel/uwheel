// use minstant::Instant;

// use awheel::{
//     aggregator::all::{AggState, AllAggregator},
//     time_internal as time,
//     tree::wheel_tree::WheelTree,
// };
// use clap::{ArgEnum, Parser};
// use duckdb::Result;
// use olap::*;
// use sketches_ddsketch::{Config, DDSketch};
// use std::{
//     sync::{
//         atomic::{AtomicBool, Ordering},
//         Arc,
//     },
//     time::Duration,
// };

// #[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
// pub enum Workload {
//     /// AllAggregator
//     All,
//     /// F64SumAggregator
//     Sum,
// }

// #[derive(Parser, Debug)]
// #[clap(author, version, about, long_about = None)]
// struct Args {
//     #[clap(short, long, value_parser, default_value_t = 1)]
//     threads: usize,
//     #[clap(short, long, value_parser, default_value_t = 1000000)]
//     keys: usize,
//     #[clap(short, long, value_parser, default_value_t = 100)]
//     num_batches: usize,
//     #[clap(short, long, value_parser, default_value_t = 10_000)]
//     batch_size: usize,
//     #[clap(short, long, value_parser, default_value_t = 1)]
//     events_per_sec: usize,
// }

// fn main() -> Result<()> {
//     let args = Args::parse();
//     let events_per_sec = args.events_per_sec;

//     println!("Running with {:#?}", args);

//     let (_watermark, batches) = DataGenerator::generate_query_data(1000, events_per_sec);
//     let total_entries = batches.len() * events_per_sec;
//     println!("Running with total entries {}", total_entries);

//     let wheel_tree: WheelTree<u64, AllAggregator> = WheelTree::default();
//     use awheel::delta::DeltaState;
//     let mut watermark = 0;
//     for batch in batches {
//         for record in batch {
//             wheel_tree.merge_delta(
//                 record.pu_location_id,
//                 DeltaState::new(watermark, vec![Some(AggState::new(record.fare_amount))]),
//             );
//         }
//         watermark += 1000;
//     }

//     let mut handles = vec![];
//     let now = Instant::now();
//     let gate = Arc::new(AtomicBool::new(true));

//     for _ in 0..args.threads {
//         let wheel_tree = wheel_tree.clone();
//         let inner_gate = gate.clone();
//         let handle =
//             std::thread::spawn(move || {
//                 let mut sketch = DDSketch::new(Config::new(0.01, 2048, 1.0e-9));
//                 while inner_gate.load(Ordering::Relaxed) {
//                     let query = Query::random_queries_low_intervals();
//                     let now = Instant::now();
//                     match query.query_type {
//                         QueryType::Keyed(pu_location_id) => {
//                             let _res = match query.interval {
//                                 QueryInterval::Seconds(secs) => wheel_tree
//                                     .get(&pu_location_id)
//                                     .map(|rw| rw.interval(time::Duration::seconds(secs as i64))),
//                                 QueryInterval::Minutes(mins) => wheel_tree
//                                     .get(&pu_location_id)
//                                     .map(|rw| rw.interval(time::Duration::minutes(mins as i64))),
//                                 QueryInterval::Hours(hours) => wheel_tree
//                                     .get(&pu_location_id)
//                                     .map(|rw| rw.interval(time::Duration::hours(hours as i64))),
//                                 QueryInterval::Days(days) => wheel_tree
//                                     .get(&pu_location_id)
//                                     .map(|rw| rw.interval(time::Duration::days(days as i64))),
//                                 QueryInterval::Weeks(weeks) => wheel_tree
//                                     .get(&pu_location_id)
//                                     .map(|rw| rw.interval(time::Duration::weeks(weeks as i64))),
//                                 QueryInterval::Landmark => {
//                                     wheel_tree.get(&pu_location_id).map(|rw| rw.landmark())
//                                 }
//                             };
//                         }
//                         QueryType::Range(start, end) => {
//                             let range = start..end;
//                             let _res = match query.interval {
//                                 QueryInterval::Seconds(secs) => wheel_tree
//                                     .interval_range(time::Duration::seconds(secs as i64), range),
//                                 QueryInterval::Minutes(mins) => wheel_tree
//                                     .interval_range(time::Duration::minutes(mins as i64), range),
//                                 QueryInterval::Hours(hours) => wheel_tree
//                                     .interval_range(time::Duration::hours(hours as i64), range),
//                                 QueryInterval::Days(days) => wheel_tree
//                                     .interval_range(time::Duration::days(days as i64), range),
//                                 QueryInterval::Weeks(weeks) => wheel_tree
//                                     .interval_range(time::Duration::weeks(weeks as i64), range),
//                                 QueryInterval::Landmark => wheel_tree.landmark_range(range),
//                             };
//                         }
//                         QueryType::All => {
//                             let _res = match query.interval {
//                                 QueryInterval::Seconds(secs) => wheel_tree
//                                     .interval_range(time::Duration::seconds(secs as i64), ..),
//                                 QueryInterval::Minutes(mins) => wheel_tree
//                                     .interval_range(time::Duration::minutes(mins as i64), ..),
//                                 QueryInterval::Hours(hours) => wheel_tree
//                                     .interval_range(time::Duration::hours(hours as i64), ..),
//                                 QueryInterval::Days(days) => {
//                                     wheel_tree.interval_range(time::Duration::days(days as i64), ..)
//                                 }
//                                 QueryInterval::Weeks(weeks) => wheel_tree
//                                     .interval_range(time::Duration::weeks(weeks as i64), ..),
//                                 QueryInterval::Landmark => wheel_tree.landmark_range(..),
//                             };
//                         }
//                     };
//                     sketch.add(now.elapsed().as_nanos() as f64);
//                 }
//                 sketch
//             });

//         handles.push(handle);
//     }

//     std::thread::sleep(Duration::from_secs(20));
//     gate.store(false, Ordering::Relaxed);

//     let skecthes: Vec<_> = handles
//         .into_iter()
//         .map(|handle| handle.join().unwrap())
//         .collect();
//     let runtime = now.elapsed();

//     let mut sketch = DDSketch::new(Config::new(0.01, 2048, 1.0e-9));
//     for s in skecthes {
//         sketch.merge(&s).unwrap();
//     }
//     let percentiles = awheel::stats::sketch_percentiles(&sketch);
//     let total_queries = sketch.count();
//     println!("{:#?}", percentiles);

//     println!(
//         "Executed {} queries and ran with {} Mops/s (took {:.2}s)",
//         total_queries,
//         (total_queries as f64 / runtime.as_secs_f64()) as u64 / 1_000_000,
//         runtime.as_secs_f64(),
//     );

//     Ok(())
// }

fn main() {}
// use minstant::Instant;

// use awheel::{
//     aggregator::all::{AggState, AllAggregator},
//     time,
//     tree::wheel_tree::WheelTree,
// };
// use clap::{ArgEnum, Parser};
// use duckdb::Result;
// use olap::*;
// use sketches_ddsketch::{Config, DDSketch};
// use std::{
//     sync::{
//         atomic::{AtomicBool, Ordering},
//         Arc,
//     },
//     time::Duration,
// };

// #[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
// pub enum Workload {
//     /// AllAggregator
//     All,
//     /// F64SumAggregator
//     Sum,
// }

// #[derive(Parser, Debug)]
// #[clap(author, version, about, long_about = None)]
// struct Args {
//     #[clap(short, long, value_parser, default_value_t = 1)]
//     threads: usize,
//     #[clap(short, long, value_parser, default_value_t = 1000000)]
//     keys: usize,
//     #[clap(short, long, value_parser, default_value_t = 100)]
//     num_batches: usize,
//     #[clap(short, long, value_parser, default_value_t = 10_000)]
//     batch_size: usize,
//     #[clap(short, long, value_parser, default_value_t = 1)]
//     events_per_sec: usize,
// }

// fn main() -> Result<()> {
//     let args = Args::parse();
//     let events_per_sec = args.events_per_sec;

//     println!("Running with {:#?}", args);

//     let (_watermark, batches) = DataGenerator::generate_query_data(events_per_sec);
//     let total_entries = batches.len() * events_per_sec;
//     println!("Running with total entries {}", total_entries);

//     let wheel_tree: WheelTree<u64, AllAggregator> = WheelTree::default();
//     use awheel::delta::DeltaState;
//     let mut watermark = 0;
//     for batch in batches {
//         for record in batch {
//             wheel_tree.merge_delta(
//                 record.pu_location_id,
//                 DeltaState::new(watermark, vec![Some(AggState::new(record.fare_amount))]),
//             );
//         }
//         watermark += 1000;
//     }

//     let mut handles = vec![];
//     let now = Instant::now();
//     let gate = Arc::new(AtomicBool::new(true));

//     for _ in 0..args.threads {
//         let wheel_tree = wheel_tree.clone();
//         let inner_gate = gate.clone();
//         let handle =
//             std::thread::spawn(move || {
//                 let mut sketch = DDSketch::new(Config::new(0.01, 2048, 1.0e-9));
//                 while inner_gate.load(Ordering::Relaxed) {
//                     let query = Query::random_queries_low_intervals();
//                     let now = Instant::now();
//                     match query.query_type {
//                         QueryType::Keyed(pu_location_id) => {
//                             let _res = match query.interval {
//                                 QueryInterval::Seconds(secs) => wheel_tree
//                                     .get(&pu_location_id)
//                                     .map(|rw| rw.interval(time::Duration::seconds(secs as i64))),
//                                 QueryInterval::Minutes(mins) => wheel_tree
//                                     .get(&pu_location_id)
//                                     .map(|rw| rw.interval(time::Duration::minutes(mins as i64))),
//                                 QueryInterval::Hours(hours) => wheel_tree
//                                     .get(&pu_location_id)
//                                     .map(|rw| rw.interval(time::Duration::hours(hours as i64))),
//                                 QueryInterval::Days(days) => wheel_tree
//                                     .get(&pu_location_id)
//                                     .map(|rw| rw.interval(time::Duration::days(days as i64))),
//                                 QueryInterval::Weeks(weeks) => wheel_tree
//                                     .get(&pu_location_id)
//                                     .map(|rw| rw.interval(time::Duration::weeks(weeks as i64))),
//                                 QueryInterval::Landmark => {
//                                     wheel_tree.get(&pu_location_id).map(|rw| rw.landmark())
//                                 }
//                             };
//                         }
//                         QueryType::Range(start, end) => {
//                             let range = start..end;
//                             let _res = match query.interval {
//                                 QueryInterval::Seconds(secs) => wheel_tree
//                                     .interval_range(time::Duration::seconds(secs as i64), range),
//                                 QueryInterval::Minutes(mins) => wheel_tree
//                                     .interval_range(time::Duration::minutes(mins as i64), range),
//                                 QueryInterval::Hours(hours) => wheel_tree
//                                     .interval_range(time::Duration::hours(hours as i64), range),
//                                 QueryInterval::Days(days) => wheel_tree
//                                     .interval_range(time::Duration::days(days as i64), range),
//                                 QueryInterval::Weeks(weeks) => wheel_tree
//                                     .interval_range(time::Duration::weeks(weeks as i64), range),
//                                 QueryInterval::Landmark => wheel_tree.landmark_range(range),
//                             };
//                         }
//                         QueryType::All => {
//                             let _res = match query.interval {
//                                 QueryInterval::Seconds(secs) => wheel_tree
//                                     .interval_range(time::Duration::seconds(secs as i64), ..),
//                                 QueryInterval::Minutes(mins) => wheel_tree
//                                     .interval_range(time::Duration::minutes(mins as i64), ..),
//                                 QueryInterval::Hours(hours) => wheel_tree
//                                     .interval_range(time::Duration::hours(hours as i64), ..),
//                                 QueryInterval::Days(days) => {
//                                     wheel_tree.interval_range(time::Duration::days(days as i64), ..)
//                                 }
//                                 QueryInterval::Weeks(weeks) => wheel_tree
//                                     .interval_range(time::Duration::weeks(weeks as i64), ..),
//                                 QueryInterval::Landmark => wheel_tree.landmark_range(..),
//                             };
//                         }
//                     };
//                     sketch.add(now.elapsed().as_nanos() as f64);
//                 }
//                 sketch
//             });

//         handles.push(handle);
//     }

//     std::thread::sleep(Duration::from_secs(20));
//     gate.store(false, Ordering::Relaxed);

//     let skecthes: Vec<_> = handles
//         .into_iter()
//         .map(|handle| handle.join().unwrap())
//         .collect();
//     let runtime = now.elapsed();

//     let mut sketch = DDSketch::new(Config::new(0.01, 2048, 1.0e-9));
//     for s in skecthes {
//         sketch.merge(&s).unwrap();
//     }
//     let percentiles = awheel::stats::sketch_percentiles(&sketch);
//     let total_queries = sketch.count();
//     println!("{:#?}", percentiles);

//     println!(
//         "Executed {} queries and ran with {} Mops/s (took {:.2}s)",
//         total_queries,
//         (total_queries as f64 / runtime.as_secs_f64()) as u64 / 1_000_000,
//         runtime.as_secs_f64(),
//     );

//     Ok(())
// }
