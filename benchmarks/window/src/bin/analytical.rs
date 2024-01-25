#![allow(clippy::let_and_return)]
use awheel::{
    aggregator::{sum::U64SumAggregator, Aggregator},
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
use duckdb::Result;
use hdrhistogram::Histogram;
use minstant::Instant;
use std::{fs::File, time::Duration};
use window::{
    tree::{BTree, FiBA8, Tree},
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
// 1hr, 7 days.
// const INTERVALS: [Durationz; 2] = [Durationz::hours(1), Durationz::hours(23 + 24 * 6)];
// const INTERVALS: [Durationz; 2] = [Durationz::days(1), Durationz::days(14)];
const INTERVALS: [Durationz; 1] = [Durationz::days(1)];
// const INTERVALS: [Durationz; 3] = [
//     Durationz::hours(1),  // 2023-10-01 01:00:00
//     Durationz::hours(23), // 2023-10-02 00:00:00
//     Durationz::days(6),   // 2023-10-08 00:00:00
// ];

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
    wheeldb_memory_bytes: usize,
    duckdb_memory: String,
    queries: Vec<QueryDescription>,
}

impl Run {
    pub fn from(
        watermark: u64,
        events_per_second: u64,
        wheeldb_memory_bytes: usize,
        duckdb_memory: String,
        queries: Vec<QueryDescription>,
    ) -> Self {
        Self {
            watermark,
            events_per_second,
            wheeldb_memory_bytes,
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
    let mut fiba_8 = FiBA8::default();

    // Prepare AggregateWheels
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
        let q1_queries_duckdb = q1_queries.clone();
        let q1_queries_btree = q1_queries.clone();
        let q1_queries_fiba = q1_queries.clone();

        // Generate Q2

        let q2_queries_seconds = QueryGenerator::generate_q2_seconds(total_queries, watermark);
        let q2_queries_seconds_duckdb = q2_queries_seconds.clone();
        let q2_queries_seconds_btree = q2_queries_seconds.clone();
        let q2_queries_fiba = q2_queries_seconds.clone();

        println!("Inserting data, may take a while...");

        for batch in btree_batches {
            for record in batch {
                btree.insert(record.do_time, record.fare_amount);
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
        let mut q2_results = QueryDescription::from("q2");

        let btree_q1 = tree_run("BTree Q1", watermark, &btree, q1_queries_btree);
        q1_results.add(Stats::from("BTree", &btree_q1));
        println!("BTree Q1 {:?}", btree_q1.0);

        let btree_q2 = tree_run("BTree Q2", watermark, &btree, q2_queries_seconds_btree);
        println!("BTree Q2 {:?}", btree_q2.0);
        q2_results.add(Stats::from("BTree", &btree_q2));

        let fiba_q1 = tree_run("FiBA Q1", watermark, &fiba_8, q1_queries_fiba);
        q1_results.add(Stats::from("FiBA", &fiba_q1));
        println!("FiBA Q1 {:?}", fiba_q1.0);

        let fiba_q2 = tree_run("FiBA Q2", watermark, &fiba_8, q2_queries_fiba);
        q2_results.add(Stats::from("FiBA", &fiba_q2));
        println!("FiBA Q2 {:?}", fiba_q2.0);

        let duckdb_id_fmt = |threads: usize| format!("duckdb-threads-{}", threads);

        let duckdb_q1 = duckdb_run(
            "DuckDB Q1",
            watermark,
            workload,
            &duckdb,
            &q1_queries_duckdb,
        );

        q1_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q1));

        println!("DuckDB Q1 {:?}", duckdb_q1.0);

        let duckdb_q2_seconds = duckdb_run(
            "DuckDB Q2",
            watermark,
            workload,
            &duckdb,
            &q2_queries_seconds_duckdb,
        );

        q2_results.add(Stats::from(
            duckdb_id_fmt(max_parallelism),
            &duckdb_q2_seconds,
        ));

        println!("DuckDB Q2 {:?}", duckdb_q2_seconds.0);

        // ADJUST threads to 1
        let max_parallelism = 1;
        duckdb_set_threads(max_parallelism, &duckdb);

        let duckdb_q1 = duckdb_run(
            "DuckDB Q1",
            watermark,
            workload,
            &duckdb,
            &q1_queries_duckdb,
        );

        q1_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q1));

        println!("DuckDB Q1 {:?}", duckdb_q1.0);

        let duckdb_q2_seconds = duckdb_run(
            "DuckDB Q2 Seconds",
            watermark,
            workload,
            &duckdb,
            &q2_queries_seconds_duckdb,
        );

        q2_results.add(Stats::from(
            duckdb_id_fmt(max_parallelism),
            &duckdb_q2_seconds,
        ));

        println!("DuckDB Q2 {:?}", duckdb_q2_seconds.0);

        let wheel_q1 = awheel_run("AggregateWheels Q1", watermark, &wheel, q1_queries);
        println!("AggregateWheels Q1 {:?}", wheel_q1.0);
        q1_results.add(Stats::from("AggregateWheels", &wheel_q1));

        let wheel_q2_seconds = awheel_run(
            "AggregateWheels Q2 Seconds",
            watermark,
            &wheel,
            q2_queries_seconds,
        );
        q2_results.add(Stats::from("AggregateWheels", &wheel_q2_seconds));
        println!("AggregateWheels Q2 {:?}", wheel_q2_seconds.0);

        let duck_info = duckdb_memory_usage(&duckdb);
        let wheeldb_memory_bytes = wheel.size_bytes();
        let duckdb_memory_bytes = duck_info.memory_usage;

        // update watermark
        current_date = watermark;

        let queries = vec![q1_results, q2_results];
        let run = Run::from(
            watermark,
            events_per_sec as u64,
            wheeldb_memory_bytes,
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
) -> (Duration, Histogram<u64>)
where
    A::PartialAggregate: Sync + Ord + PartialEq,
{
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();

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

                        tree.range_query(start_ms, end_ms)
                    }
                    TimeInterval::Landmark => tree.query(),
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
    (runtime, hist)
}

fn awheel_run<A: Aggregator + Clone>(
    _id: &str,
    _watermark: u64,
    wheel: &RwWheel<A>,
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>)
where
    A::PartialAggregate: Sync + Ord + PartialEq,
{
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();

    let full = Instant::now();
    for query in queries {
        let now = Instant::now();
        match query.query_type {
            QueryType::All => {
                let _res = match query.interval {
                    TimeInterval::Range(start, end) => {
                        let (start, end) = into_offset_date_time_start_end(start, end);
                        wheel
                            .read()
                            .as_ref()
                            .combine_range(WheelRange::new(start, end))
                    }
                    TimeInterval::Landmark => wheel.read().landmark(),
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
    (runtime, hist)
}

fn duckdb_run(
    _id: &str,
    _watermark: u64,
    workload: Workload,
    db: &duckdb::Connection,
    queries: &[Query],
) -> (Duration, Histogram<u64>) {
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
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
    (runtime, hist)
}

#[derive(Debug, serde::Serialize)]
pub struct Stats {
    id: String,
    latency: Latency,
    runtime: f64,
}

impl Stats {
    pub fn from(id: impl Into<String>, metrics: &(Duration, Histogram<u64>)) -> Self {
        let runtime = metrics.0.as_secs_f64();
        let latency = Latency::from(&metrics.1);

        Self {
            id: id.into(),
            latency,
            runtime,
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
