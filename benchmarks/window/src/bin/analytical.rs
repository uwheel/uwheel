#![allow(clippy::let_and_return)]
use awheel::{
    aggregator::{max::U64MaxAggregator, sum::U64SumAggregator, Aggregator},
    rw_wheel::read::{
        aggregation::conf::RetentionPolicy,
        hierarchical::{HawConf, WheelRange},
    },
    time_internal::Duration as Durationz,
    Entry,
    Options,
    RwWheel,
};
use clap::{ArgEnum, Parser};
use duckdb::{arrow::datatypes::ArrowNativeTypeOp, Result};
use hdrhistogram::Histogram;
use minstant::Instant;
use std::{fs::File, time::Duration};
use window::{
    tree::{BTree, FiBA4, FiBA8, Tree},
    util::*,
};

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Workload {
    /// AllAggregator
    All,
    /// F64SumAggregator
    Sum,
}

// 2023-10-01 01:00:00 + 2023-10-08 00:00:00
// const INTERVALS: [Durationz; 2] = [Durationz::hours(1), Durationz::hours(23 + 24 * 6)];
const INTERVALS: [Durationz; 2] = [Durationz::days(1), Durationz::days(6)];

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1)]
    events_per_sec: usize,
    #[clap(short, long, value_parser, default_value_t = 1)]
    iterations: usize,
    #[clap(short, long, value_parser, default_value_t = 50000)]
    queries: usize,
    #[clap(short, long, action)]
    disk: bool,
    #[clap(arg_enum, value_parser, default_value_t = Workload::Sum)]
    workload: Workload,
}

fn main() -> Result<()> {
    let args = Args::parse();
    println!("Running with {:#?}", args);
    println!("Using {} keys", MAX_KEYS);
    let runs = run(&args);
    let output = PlottingOutput::from(args.events_per_sec, args.queries, MAX_KEYS, runs);
    output.flush_to_file().unwrap();

    Ok(())
}

#[derive(Debug, serde::Serialize)]
pub struct PlottingOutput {
    events_per_second: usize,
    total_queries: usize,
    total_keys: u64,
    runs: Vec<Run>,
}

impl PlottingOutput {
    pub fn flush_to_file(&self) -> serde_json::Result<()> {
        let path = format!(
            "../results/analytical_bench_{}_eps.json",
            self.events_per_second
        );
        let file = File::create(path).unwrap();
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
}

impl PlottingOutput {
    pub fn from(
        events_per_second: usize,
        total_queries: usize,
        total_keys: u64,
        runs: Vec<Run>,
    ) -> Self {
        Self {
            events_per_second,
            total_queries,
            total_keys,
            runs,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Run {
    watermark: u64,
    events_per_second: u64,
    wheels_memory_bytes: usize,
    btree_memory_bytes: usize,
    fiba_bfinger4_memory_bytes: usize,
    fiba_bfinger8_memory_bytes: usize,
    duckdb_memory: String,
    queries: Vec<QueryDescription>,
}

impl Run {
    #[allow(clippy::too_many_arguments)]
    pub fn from(
        watermark: u64,
        events_per_second: u64,
        wheels_memory_bytes: usize,
        fiba_bfinger4_memory_bytes: usize,
        fiba_bfinger8_memory_bytes: usize,
        btree_memory_bytes: usize,
        duckdb_memory: String,
        queries: Vec<QueryDescription>,
    ) -> Self {
        Self {
            watermark,
            events_per_second,
            wheels_memory_bytes,
            fiba_bfinger4_memory_bytes,
            fiba_bfinger8_memory_bytes,
            btree_memory_bytes,
            duckdb_memory,
            queries,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct QueryDescription {
    // e.g., Q1
    id: String,
    // WheelDB / DuckDB
    systems: Vec<Stats>,
}
impl QueryDescription {
    pub fn from(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            systems: Vec::new(),
        }
    }
    pub fn add(&mut self, system: Stats) {
        self.systems.push(system);
    }
}

fn run(args: &Args) -> Vec<Run> {
    let total_queries = args.queries;
    let events_per_sec = args.events_per_sec;
    let workload = args.workload;
    let _iterations = args.iterations;
    let start_date = START_DATE_MS;
    let mut current_date = start_date;
    let max_parallelism = 8;
    // let total_landmark_queries = 1000; // Less variance in the landmark queries and it takes up time of the execution
    let total_landmark_queries = total_queries;

    // Prepare DuckDB
    let (mut duckdb, _id) = duckdb_setup(args.disk, max_parallelism);

    // Prepare BTree
    let mut btree: BTree<U64SumAggregator> = BTree::default();
    let mut fiba_4 = FiBA4::default();
    let mut fiba_8 = FiBA8::default();

    // Prepare μWheel
    let mut haw_conf = HawConf::default();
    haw_conf.seconds.set_retention_policy(RetentionPolicy::Keep);
    haw_conf.minutes.set_retention_policy(RetentionPolicy::Keep);
    haw_conf.hours.set_retention_policy(RetentionPolicy::Keep);
    haw_conf.days.set_retention_policy(RetentionPolicy::Keep);

    let opts = Options::default()
        .with_write_ahead(604800usize.next_power_of_two())
        .with_haw_conf(haw_conf);
    let mut wheel = RwWheel::<U64SumAggregator>::with_options(start_date, opts);

    let mut runs = Vec::new();

    for interval in INTERVALS {
        let interval_as_seconds = interval.whole_seconds() as usize;
        let (watermark, batches) =
            DataGenerator::generate_query_data(current_date, events_per_sec, interval_as_seconds);
        println!(
            "Running for between start {} and end {}",
            start_date, watermark
        );
        let duckdb_batches = batches.clone();
        let btree_batches = batches.clone();

        // Generate Q1

        let q1_queries = QueryGenerator::generate_q1(total_landmark_queries);
        let q1_queries_hints = q1_queries.clone();
        let q1_queries_duckdb = q1_queries.clone();
        let q1_queries_btree = q1_queries.clone();
        let q1_queries_fiba_4 = q1_queries.clone();
        let q1_queries_fiba = q1_queries.clone();

        // Generate Q2

        let q2_queries_seconds = QueryGenerator::generate_q2_seconds(total_queries, watermark);
        let q2_queries_seconds_hints = q2_queries_seconds.clone();
        let q2_queries_seconds_duckdb = q2_queries_seconds.clone();
        let q2_queries_seconds_btree = q2_queries_seconds.clone();
        let q2_queries_fiba_4 = q2_queries_seconds.clone();
        let q2_queries_fiba = q2_queries_seconds.clone();

        // Q2 Minutes

        let q2_queries_minutes = QueryGenerator::generate_q2_minutes(total_queries, watermark);
        let q2_queries_minutes_hints = q2_queries_minutes.clone();
        let q2_queries_minutes_duckdb = q2_queries_minutes.clone();
        let q2_queries_minutes_btree = q2_queries_minutes.clone();
        let q2_queries_minutes_fiba_4 = q2_queries_minutes.clone();
        let q2_queries_minutes_fiba = q2_queries_minutes.clone();

        // Q2 Hours

        let q2_queries_hours = QueryGenerator::generate_q2_hours(total_queries, watermark);
        let q2_queries_hours_hints = q2_queries_hours.clone();
        let q2_queries_hours_btree = q2_queries_hours.clone();
        let q2_queries_hours_duckdb = q2_queries_hours.clone();
        let q2_queries_hours_fiba_4 = q2_queries_hours.clone();
        let q2_queries_hours_fiba = q2_queries_hours.clone();

        println!("Inserting data, may take a while...");

        for batch in btree_batches {
            for record in batch {
                btree.insert(record.do_time, record.fare_amount);
                fiba_4.insert(record.do_time, record.fare_amount);
                fiba_8.insert(record.do_time, record.fare_amount);
            }
        }

        // Insert data to DuckDB
        for batch in duckdb_batches {
            duckdb_append_batch(batch, &mut duckdb).unwrap();
        }

        for batch in batches {
            // 1 batch represents 1 second of data
            for record in batch {
                let entry = Entry::new(record.fare_amount, record.do_time);
                wheel.insert(entry);
            }
        }

        wheel.advance(interval);

        println!("Now running queries");

        dbg!(wheel.watermark());
        dbg!(watermark);

        let mut q1_results = QueryDescription::from("q1");
        let mut q2_seconds_results = QueryDescription::from("q2-seconds");
        let mut q2_minutes_results = QueryDescription::from("q2-minutes");
        let mut q2_hours_results = QueryDescription::from("q2-hours");

        let btree_q1 = tree_run("BTree Q1", watermark, &btree, q1_queries_btree);
        q1_results.add(Stats::from("BTree", &btree_q1));
        println!("BTree Q1 {:?}", btree_q1.0);

        let btree_q2 = tree_run("BTree Q2", watermark, &btree, q2_queries_seconds_btree);
        println!("BTree Q2 Seconds {:?}", btree_q2.0);
        q2_seconds_results.add(Stats::from("BTree", &btree_q2));

        let btree_q2_minutes = tree_run(
            "BTree Q2 Minutes",
            watermark,
            &btree,
            q2_queries_minutes_btree,
        );
        println!("BTree Q2 Minutes {:?}", btree_q2_minutes.0);
        q2_minutes_results.add(Stats::from("BTree", &btree_q2_minutes));

        let btree_q2_hours = tree_run("BTree Q2 Hours", watermark, &btree, q2_queries_hours_btree);
        println!("BTree Q2 Hours  {:?}", btree_q2_hours.0);
        q2_hours_results.add(Stats::from("BTree", &btree_q2_hours));

        let fiba_4_q1 = tree_run("FiBA Bfinger4 Q1", watermark, &fiba_4, q1_queries_fiba_4);
        q1_results.add(Stats::from("FiBA Bfinger4", &fiba_4_q1));
        println!("FiBA Bfinger4 Q1 {:?}", fiba_4_q1.0);

        let fiba_4_q2 = tree_run("FiBA Bfinger4 Q2", watermark, &fiba_4, q2_queries_fiba_4);
        q2_seconds_results.add(Stats::from("FiBA Bfinger4", &fiba_4_q2));
        println!("FiBA Bfinger4 Q2 Seconds {:?}", fiba_4_q2.0);
        println!("avg ops: {}, worst ops: {}", fiba_4_q2.2, fiba_4_q2.3);

        let fiba_q2_minutes = tree_run(
            "FiBA Q2 Minutes",
            watermark,
            &fiba_8,
            q2_queries_minutes_fiba_4,
        );
        q2_minutes_results.add(Stats::from("FiBA Bfinger4", &fiba_q2_minutes));
        println!("FiBA Bfinger4 Q2 Minutes {:?}", fiba_q2_minutes.0);
        println!(
            "avg ops: {}, worst ops: {}",
            fiba_q2_minutes.2, fiba_q2_minutes.3
        );

        let fiba_q2_hours = tree_run(
            "FiBA Bfinger4 Q2 hours",
            watermark,
            &fiba_8,
            q2_queries_hours_fiba_4,
        );
        q2_hours_results.add(Stats::from("FiBA Bfinger4", &fiba_q2_hours));
        println!("FiBA Bfinger4 Q2 Hours {:?}", fiba_q2_hours.0);
        println!(
            "avg ops: {}, worst ops: {}",
            fiba_q2_hours.2, fiba_q2_hours.3
        );

        let fiba_q1 = tree_run("FiBA Bfinger8 Q1", watermark, &fiba_8, q1_queries_fiba);
        q1_results.add(Stats::from("FiBA Bfinger8", &fiba_q1));
        println!("FiBA Bfinger8 Q1 {:?}", fiba_q1.0);

        let fiba_q2 = tree_run("FiBA Bfinger8 Q2 ", watermark, &fiba_8, q2_queries_fiba);
        q2_seconds_results.add(Stats::from("FiBA Bfinger8", &fiba_q2));
        println!("FiBA Bfinger8 Q2 Seconds {:?}", fiba_q2.0);
        println!("avg ops: {}, worst ops: {}", fiba_q2.2, fiba_q2.3);

        let fiba_q2_minutes = tree_run(
            "FiBA Bfinger8 Q2 Minutes",
            watermark,
            &fiba_8,
            q2_queries_minutes_fiba,
        );
        q2_minutes_results.add(Stats::from("FiBA Bfinger8", &fiba_q2_minutes));
        println!("FiBA Bfinger8 Q2 Minutes {:?}", fiba_q2_minutes.0);
        println!(
            "avg ops: {}, worst ops: {}",
            fiba_q2_minutes.2, fiba_q2_minutes.3
        );

        let fiba_q2_hours = tree_run(
            "FiBA Bfinger8 Q2 hours",
            watermark,
            &fiba_8,
            q2_queries_hours_fiba,
        );
        q2_hours_results.add(Stats::from("FiBA Bfinger8", &fiba_q2_hours));
        println!("FiBA Bfinger8 Q2 Hours {:?}", fiba_q2_hours.0);
        println!(
            "avg ops: {}, worst ops: {}",
            fiba_q2_hours.2, fiba_q2_hours.3
        );

        let duckdb_id_fmt = |threads: usize| format!("duckdb-threads-{}", threads);

        // let duckdb_q1 = duckdb_run(
        //     "DuckDB Q1",
        //     watermark,
        //     workload,
        //     &duckdb,
        //     &q1_queries_duckdb,
        // );

        // q1_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q1));

        // println!("DuckDB Q1 {:?}", duckdb_q1.0);

        // let duckdb_q2_seconds = duckdb_run(
        //     "DuckDB Q2 Seconds",
        //     watermark,
        //     workload,
        //     &duckdb,
        //     &q2_queries_seconds_duckdb,
        // );

        // q2_seconds_results.add(Stats::from(
        //     duckdb_id_fmt(max_parallelism),
        //     &duckdb_q2_seconds,
        // ));

        // println!("DuckDB Q2 Seconds {:?}", duckdb_q2_seconds.0);

        // ADJUST threads to 1
        let max_parallelism = 1;
        duckdb_set_threads(max_parallelism, &duckdb);

        let mut duckdb_q1 = duckdb_run(
            "DuckDB Q1",
            watermark,
            workload,
            &duckdb,
            &q1_queries_duckdb,
        );

        // should have same ops
        duckdb_q1.2 = btree_q1.2;
        duckdb_q1.3 = btree_q1.3;

        q1_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q1));

        println!("DuckDB Q1 {:?}", duckdb_q1.0);

        let duckdb_q2_seconds = duckdb_run(
            "DuckDB Q2 Seconds",
            watermark,
            workload,
            &duckdb,
            &q2_queries_seconds_duckdb,
        );

        q2_seconds_results.add(Stats::from(
            duckdb_id_fmt(max_parallelism),
            &duckdb_q2_seconds,
        ));

        println!("DuckDB Q2 Seconds {:?}", duckdb_q2_seconds.0);

        let duckdb_q2_minutes = duckdb_run(
            "DuckDB Q2 Minutes",
            watermark,
            workload,
            &duckdb,
            &q2_queries_minutes_duckdb,
        );

        q2_minutes_results.add(Stats::from(
            duckdb_id_fmt(max_parallelism),
            &duckdb_q2_minutes,
        ));
        println!("DuckDB Q2 Minutes {:?}", duckdb_q2_minutes.0);

        let duckdb_q2_hours = duckdb_run(
            "DuckDB Q2 Hours",
            watermark,
            workload,
            &duckdb,
            &q2_queries_hours_duckdb,
        );

        q2_hours_results.add(Stats::from(
            duckdb_id_fmt(max_parallelism),
            &duckdb_q2_hours,
        ));

        println!("DuckDB Q2 Hours {:?}", duckdb_q2_hours.0);

        wheel.read().set_optimizer_hints(false);

        let wheel_q1 = awheel_run("μWheel Q1", watermark, &wheel, q1_queries);
        println!("μWheel Q1 {:?}", wheel_q1.0);
        q1_results.add(Stats::from("μWheel", &wheel_q1));

        let wheel_q2_seconds =
            awheel_run("μWheel Q2 Seconds", watermark, &wheel, q2_queries_seconds);
        q2_seconds_results.add(Stats::from("μWheel", &wheel_q2_seconds));
        println!("μWheel Q2 Seconds {:?}", wheel_q2_seconds.0);
        println!(
            "avg ops: {} worst ops: {}",
            wheel_q2_seconds.2, wheel_q2_seconds.3
        );

        let wheel_q2_minutes =
            awheel_run("μWheel Q2 Minutes", watermark, &wheel, q2_queries_minutes);
        q2_minutes_results.add(Stats::from("μWheel", &wheel_q2_minutes));
        println!("μWheel Q2 Minutes {:?}", wheel_q2_minutes.0);
        println!(
            "avg ops: {} worst ops: {}",
            wheel_q2_minutes.2, wheel_q2_minutes.3
        );

        let wheel_q2_hours = awheel_run("μWheel Q2 Hours", watermark, &wheel, q2_queries_hours);
        q2_hours_results.add(Stats::from("μWheel", &wheel_q2_hours));
        println!("μWheel Q2 Hours {:?}", wheel_q2_hours.0);
        println!(
            "avg ops: {} worst ops: {}",
            wheel_q2_hours.2, wheel_q2_hours.3
        );

        wheel.read().set_optimizer_hints(true);

        let wheel_q1 = awheel_run("μWheel-hints Q1", watermark, &wheel, q1_queries_hints);
        println!("μWheel-hints Q1 {:?}", wheel_q1.0);
        q1_results.add(Stats::from("μWheel-hints", &wheel_q1));

        let wheel_q2_seconds = awheel_run(
            "μWheel Q2 Seconds",
            watermark,
            &wheel,
            q2_queries_seconds_hints,
        );
        q2_seconds_results.add(Stats::from("μWheel-hints", &wheel_q2_seconds));
        println!("μWheel-hints Q2 Seconds {:?}", wheel_q2_seconds.0);
        println!(
            "avg ops: {} worst ops: {}",
            wheel_q2_seconds.2, wheel_q2_seconds.3
        );

        let wheel_q2_minutes = awheel_run(
            "μWheel-hints Q2 Minutes",
            watermark,
            &wheel,
            q2_queries_minutes_hints,
        );
        q2_minutes_results.add(Stats::from("μWheel-hints", &wheel_q2_minutes));
        println!("μWheel-hints Q2 Minutes {:?}", wheel_q2_minutes.0);
        println!(
            "avg ops: {} worst ops: {}",
            wheel_q2_minutes.2, wheel_q2_minutes.3
        );

        let wheel_q2_hours = awheel_run(
            "μWheel-hints Q2 Hours",
            watermark,
            &wheel,
            q2_queries_hours_hints,
        );
        q2_hours_results.add(Stats::from("μWheel-hints", &wheel_q2_hours));
        println!("μWheel-hints Q2 Hours {:?}", wheel_q2_hours.0);
        println!(
            "avg ops: {} worst ops: {}",
            wheel_q2_hours.2, wheel_q2_hours.3
        );

        let duck_info = duckdb_memory_usage(&duckdb);
        let wheels_memory_bytes = wheel.size_bytes();
        let fiba_bfinger4_memory_bytes = fiba_4.size_bytes();
        let fiba_bfinger8_memory_bytes = fiba_8.size_bytes();
        let btree_memory_bytes = btree.size_bytes();
        let duckdb_memory_bytes = duck_info.memory_usage;

        // update watermark
        current_date = watermark;

        let queries = vec![
            q1_results,
            q2_seconds_results,
            q2_minutes_results,
            q2_hours_results,
        ];
        let run = Run::from(
            watermark,
            events_per_sec as u64,
            wheels_memory_bytes,
            fiba_bfinger4_memory_bytes,
            fiba_bfinger8_memory_bytes,
            btree_memory_bytes,
            duckdb_memory_bytes,
            queries,
        );
        runs.push(run);
    }
    runs
}

fn tree_run<A: Aggregator, T: Tree<A>>(
    _id: &str,
    _watermark: u64,
    tree: &T,
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>, usize, usize)
where
    A::PartialAggregate: Sync + Ord + PartialEq,
{
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let mut worst_case_ops = 0;
    let mut sum = 0;
    let mut count = 0;

    let full = Instant::now();
    for query in queries {
        let now = Instant::now();
        match query.query_type {
            QueryType::All => {
                let _res = match query.interval {
                    TimeInterval::Range(start, end) => {
                        // converto to ms.
                        let start_ms = start * 1000;
                        let end_ms = end * 1000;

                        let (agg, cost) = tree.analyze_range_query(start_ms, end_ms);
                        worst_case_ops = U64MaxAggregator::combine(worst_case_ops, cost as u64);
                        sum += cost;
                        count += 1;
                        agg
                    }
                    TimeInterval::Landmark => {
                        let (agg, cost) = tree.analyze_query();
                        worst_case_ops = U64MaxAggregator::combine(worst_case_ops, cost as u64);
                        sum += cost;
                        count += 1;
                        agg
                    }
                };
                #[cfg(feature = "debug")]
                dbg!(_res);

                // assert!(_res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
            _ => panic!("not supposed to happen"),
        };
    }
    let runtime = full.elapsed();
    let avg_ops = sum.checked_div(count);
    (runtime, hist, avg_ops.unwrap_or(0), worst_case_ops as usize)
}

fn awheel_run<A: Aggregator + Clone>(
    _id: &str,
    _watermark: u64,
    wheel: &RwWheel<A>,
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>, usize, usize)
where
    A::PartialAggregate: Sync + Ord + PartialEq,
{
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let mut worst_case_ops = 0;
    let mut sum = 0;
    let mut count = 0;

    let full = Instant::now();
    for query in queries {
        let now = Instant::now();
        match query.query_type {
            QueryType::All => {
                let _res = match query.interval {
                    TimeInterval::Range(start, end) => {
                        let (start_date, end_date) = into_offset_date_time_start_end(start, end);

                        let (agg, cost) = wheel
                            .read()
                            .as_ref()
                            .analyze_combine_range(WheelRange::new(start_date, end_date));
                        worst_case_ops = U64MaxAggregator::combine(worst_case_ops, cost as u64);
                        sum += cost;
                        count += 1;
                        agg
                    }
                    TimeInterval::Landmark => {
                        let (agg, cost) = wheel.read().as_ref().analyze_landmark();
                        sum += cost;
                        count += 1;
                        agg
                    }
                };
                #[cfg(feature = "debug")]
                dbg!(_res);

                // assert!(_res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
            _ => panic!("not supposed to happen"),
        };
    }
    let runtime = full.elapsed();
    let avg_ops = sum.checked_div(count).unwrap_or(0);
    (runtime, hist, avg_ops, worst_case_ops as usize)
}

fn duckdb_run(
    _id: &str,
    _watermark: u64,
    workload: Workload,
    db: &duckdb::Connection,
    queries: &[Query],
) -> (Duration, Histogram<u64>, usize, usize) {
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();

    let mut worst_case_ops = 0;
    let mut sum = 0;
    let mut count = 0;

    let mut base_str = match workload {
        Workload::All => "SELECT AVG(fare_amount), SUM(fare_amount), MIN(fare_amount), MAX(fare_amount), COUNT(fare_amount) FROM rides",
        Workload::Sum => "SELECT SUM(fare_amount) FROM rides",
    };
    // Prepare SQL queries
    let sql_queries: Vec<String> = queries
        .iter()
        .map(|query| {
            // Generate Key clause. If no key then leave as empty ""
            let key_clause = match query.query_type {
                QueryType::TopK => {
                    // modiy the base str
                    base_str = "SELECT pu_location_id, SUM(fare_amount) FROM rides";

                    if let TimeInterval::Landmark = query.interval {
                        "".to_string()
                    } else {
                        "WHERE".to_string()
                    }
                }

                QueryType::Keyed(pu_location_id) => {
                    if let TimeInterval::Landmark = query.interval {
                        format!("WHERE pu_location_id={}", pu_location_id)
                    } else {
                        format!("WHERE pu_location_id={} AND", pu_location_id)
                    }
                }
                QueryType::Range(start, end) => {
                    if let TimeInterval::Landmark = query.interval {
                        format!("WHERE pu_location_id BETWEEN {} AND {}", start, end)
                    } else {
                        format!("WHERE pu_location_id BETWEEN {} AND {} AND", start, end)
                    }
                }
                QueryType::All => {
                    if let TimeInterval::Landmark = query.interval {
                        "".to_string()
                    } else {
                        "WHERE".to_string()
                    }
                }
            };
            let interval = match query.interval {
                TimeInterval::Range(start, end) => {
                    let start_ms = start * 1000;
                    let end_ms = end * 1000;
                    let aggregates = (end_ms - start_ms) / 1000;
                    sum += aggregates;
                    worst_case_ops = U64MaxAggregator::combine(worst_case_ops, aggregates);
                    count += 1;
                    format!("do_time >= {} AND do_time < {}", start_ms, end_ms)
                }
                TimeInterval::Landmark => "".to_string(),
            };

            let full_query = match query.query_type {
                QueryType::TopK => {
                    let order_by =
                        "GROUP BY pu_location_id, fare_amount ORDER BY fare_amount DESC LIMIT 10";
                    format!("{} {} {} {}", base_str, key_clause, interval, order_by)
                }
                _ => format!("{} {} {}", base_str, key_clause, interval),
            };
            // println!("QUERY {}", full_query);
            full_query
        })
        .collect();

    let full = Instant::now();
    for sql_query in sql_queries {
        let now = Instant::now();
        match workload {
            Workload::All => duckdb_query_all(&sql_query, db).unwrap(),
            Workload::Sum => duckdb_query_sum(&sql_query, db).unwrap(),
        }
        hist.record(now.elapsed().as_nanos() as u64).unwrap();
    }
    let runtime = full.elapsed();
    let avg_ops = sum.div_checked(count).unwrap_or(0);
    (runtime, hist, avg_ops as usize, worst_case_ops as usize)
}

#[derive(Debug, serde::Serialize)]
pub struct Stats {
    id: String,
    latency: Latency,
    runtime: f64,
    avg_ops: usize,
    worst_ops: usize,
}

impl Stats {
    pub fn from(id: impl Into<String>, metrics: &(Duration, Histogram<u64>, usize, usize)) -> Self {
        let runtime = metrics.0.as_secs_f64();
        let latency = Latency::from(&metrics.1);

        Self {
            id: id.into(),
            latency,
            runtime,
            avg_ops: metrics.2,
            worst_ops: metrics.3,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Latency {
    count: u64,
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

impl Latency {
    pub fn from(hist: &Histogram<u64>) -> Self {
        Self {
            count: hist.len(),
            min: hist.min(),
            p50: hist.value_at_quantile(0.5),
            p95: hist.value_at_quantile(0.95),
            p99: hist.value_at_quantile(0.99),
            max: hist.max(),
        }
    }
}

fn _print_hist(id: &str, hist: &Histogram<u64>) {
    println!(
        "{} latencies:\t\t\t\t\t\tmin: {: >4}ns\tp50: {: >4}ns\tp99: {: \
         >4}ns\tp99.9: {: >4}ns\tp99.99: {: >4}ns\tp99.999: {: >4}ns\t max: {: >4}ns \t count: {}",
        id,
        Duration::from_nanos(hist.min()).as_nanos(),
        Duration::from_nanos(hist.value_at_quantile(0.5)).as_nanos(),
        Duration::from_nanos(hist.value_at_quantile(0.99)).as_nanos(),
        Duration::from_nanos(hist.value_at_quantile(0.999)).as_nanos(),
        Duration::from_nanos(hist.value_at_quantile(0.9999)).as_nanos(),
        Duration::from_nanos(hist.value_at_quantile(0.99999)).as_nanos(),
        Duration::from_nanos(hist.max()).as_nanos(),
        hist.len(),
    );
}
