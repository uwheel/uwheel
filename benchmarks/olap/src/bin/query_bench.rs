use awheel::{
    aggregator::{sum::U64SumAggregator, Aggregator},
    time_internal::Duration as Durationz,
    tree::wheel_tree::WheelTree,
    Entry,
    Options,
    RwWheel,
};
use clap::{ArgEnum, Parser};
use duckdb::Result;
use hdrhistogram::Histogram;
use minstant::Instant;
use olap::*;
use std::{collections::HashMap, fs::File, time::Duration};

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Workload {
    /// AllAggregator
    All,
    /// F64SumAggregator
    Sum,
}

const INTERVALS: [Durationz; 2] = [Durationz::days(1), Durationz::days(6)];

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
        let path = format!("../results/olap_bench_{}_eps.json", self.events_per_second);
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
    // let total_landmark_queries = if total_queries == 1 { 1 } else { 1000 };
    let total_landmark_queries = 1000; // Less variance in the landmark queries and it takes up time of the execution

    // Prepare DuckDB
    let (mut duckdb, _id) = duckdb_setup(args.disk, max_parallelism);

    // Prepare WheelDB

    let mut wheels = HashMap::with_capacity(MAX_KEYS as usize);
    let opts = Options::default().with_write_ahead(604800usize.next_power_of_two());
    let mut star = RwWheel::<U64SumAggregator>::with_options(start_date, opts);

    let mut tree: WheelTree<u64, U64SumAggregator> = WheelTree::default();

    for id in 0..MAX_KEYS {
        let wheel = RwWheel::<U64SumAggregator>::with_options(start_date, opts);
        // populate the tree with the Read Wheel
        tree.insert(id, wheel.read().clone());
        wheels.insert(id, wheel);
    }

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

        // Generate Q1

        let q1_queries = QueryGenerator::generate_q1(total_landmark_queries);
        let q1_queries_duckdb = q1_queries.clone();

        // Generate Q2

        let q2_queries_seconds = QueryGenerator::generate_q2_seconds(total_queries, watermark);
        let q2_queries_seconds_duckdb = q2_queries_seconds.clone();

        // Generate Q3

        let q3_queries = QueryGenerator::generate_q3(total_landmark_queries);
        let q3_queries_duckdb = q3_queries.clone();

        // Generate Q4

        let q4_queries_seconds = QueryGenerator::generate_q4_seconds(total_queries, watermark);
        let q4_queries_seconds_duckdb = q4_queries_seconds.clone();

        // Generate Q5

        let q5_queries = QueryGenerator::generate_q5(total_queries);
        let q5_queries_duckdb = q5_queries.clone();

        // Generate Q6

        let q6_queries_seconds = QueryGenerator::generate_q6_seconds(total_queries, watermark);
        let q6_queries_seconds_duckdb = q6_queries_seconds.clone();

        // Generate Q7

        let q7_queries = QueryGenerator::generate_q7(total_queries);
        let q7_queries_duckdb = q7_queries.clone();

        let q8_queries = QueryGenerator::generate_q8(total_queries, watermark);
        let q8_queries_duckdb = q8_queries.clone();

        println!("Inserting data to DuckDB and WheelDB, may take a while...");

        // Insert data to DuckDB
        for batch in duckdb_batches {
            duckdb_append_batch(batch, &mut duckdb).unwrap();
        }

        // Insert data to WheelDB

        for batch in batches {
            // 1 batch represents 1 second of data
            for record in batch {
                let wheel = wheels.get_mut(&record.pu_location_id).unwrap();
                let entry = Entry::new(record.fare_amount, record.do_time);
                wheel.insert(entry);
                star.insert(entry)
            }
        }

        // advance all wheels by interval

        for wheel in wheels.values_mut() {
            wheel.advance(interval);
        }

        star.advance(interval);

        tree.insert_star(star.read().clone());
        println!("Now running queries");

        dbg!(star.watermark());
        dbg!(watermark);

        let mut q1_results = QueryDescription::from("q1");
        let mut q2_seconds_results = QueryDescription::from("q2");
        let mut q3_results = QueryDescription::from("q3");
        let mut q4_seconds_results = QueryDescription::from("q4");
        let mut q5_results = QueryDescription::from("q5");
        let mut q6_seconds_results = QueryDescription::from("q6");
        let mut q7_results = QueryDescription::from("q7");
        let mut q8_results = QueryDescription::from("q8");

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

        let duckdb_q3 = duckdb_run(
            "DuckDB Q3",
            watermark,
            workload,
            &duckdb,
            &q3_queries_duckdb,
        );
        println!("DuckDB Q3 {:?}", duckdb_q3.0);
        q3_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q3));

        let duckdb_q4_seconds = duckdb_run(
            "DuckDB Q4 Seconds",
            watermark,
            workload,
            &duckdb,
            &q4_queries_seconds_duckdb,
        );

        q4_seconds_results.add(Stats::from(
            duckdb_id_fmt(max_parallelism),
            &duckdb_q4_seconds,
        ));

        let duckdb_q5 = duckdb_run(
            "DuckDB Q5",
            watermark,
            workload,
            &duckdb,
            &q5_queries_duckdb,
        );
        q5_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q5));

        let duckdb_q6_seconds = duckdb_run(
            "DuckDB Q6 Seconds",
            watermark,
            workload,
            &duckdb,
            &q6_queries_seconds_duckdb,
        );

        q6_seconds_results.add(Stats::from(
            duckdb_id_fmt(max_parallelism),
            &duckdb_q6_seconds,
        ));

        let duckdb_q7 = duckdb_run(
            "DuckDB Q7",
            watermark,
            workload,
            &duckdb,
            &q7_queries_duckdb,
        );

        println!("DuckDB Q7 {:?}", duckdb_q7.0);

        q7_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q7));

        let duckdb_q8 = duckdb_run(
            "DuckDB Q8",
            watermark,
            workload,
            &duckdb,
            &q8_queries_duckdb,
        );

        println!("DuckDB Q8 {:?}", duckdb_q8.0);
        q8_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q8));

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

        q2_seconds_results.add(Stats::from(
            duckdb_id_fmt(max_parallelism),
            &duckdb_q2_seconds,
        ));

        println!("DuckDB Q2 Seconds {:?}", duckdb_q2_seconds.0);

        let duckdb_q3 = duckdb_run(
            "DuckDB Q3",
            watermark,
            workload,
            &duckdb,
            &q3_queries_duckdb,
        );
        println!("DuckDB Q3 {:?}", duckdb_q3.0);
        q3_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q3));

        let duckdb_q4_seconds = duckdb_run(
            "DuckDB Q4 Seconds",
            watermark,
            workload,
            &duckdb,
            &q4_queries_seconds_duckdb,
        );

        q4_seconds_results.add(Stats::from(
            duckdb_id_fmt(max_parallelism),
            &duckdb_q4_seconds,
        ));

        let duckdb_q5 = duckdb_run(
            "DuckDB Q5",
            watermark,
            workload,
            &duckdb,
            &q5_queries_duckdb,
        );
        q5_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q5));

        let duckdb_q6_seconds = duckdb_run(
            "DuckDB Q6 Seconds",
            watermark,
            workload,
            &duckdb,
            &q6_queries_seconds_duckdb,
        );

        q6_seconds_results.add(Stats::from(
            duckdb_id_fmt(max_parallelism),
            &duckdb_q6_seconds,
        ));

        let duckdb_q7 = duckdb_run(
            "DuckDB Q7",
            watermark,
            workload,
            &duckdb,
            &q7_queries_duckdb,
        );
        println!("DuckDB Q7 {:?}", duckdb_q7.0);

        q7_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q7));

        let duckdb_q8 = duckdb_run(
            "DuckDB Q8",
            watermark,
            workload,
            &duckdb,
            &q8_queries_duckdb,
        );

        println!("DuckDB Q8 {:?}", duckdb_q8.0);
        q8_results.add(Stats::from(duckdb_id_fmt(max_parallelism), &duckdb_q8));

        let wheeldb_id_fmt = |threads: usize| format!("wheeldb-threads-{}", threads);

        let wheel_threads = 1;

        let wheel_q1 = awheel_run("WheelDB Q1", watermark, &tree, q1_queries);
        println!("WheelDB Q1 {:?}", wheel_q1.0);
        q1_results.add(Stats::from(wheeldb_id_fmt(wheel_threads), &wheel_q1));

        let wheel_q2_seconds =
            awheel_run("WheelDB Q2 Seconds", watermark, &tree, q2_queries_seconds);
        q2_seconds_results.add(Stats::from(
            wheeldb_id_fmt(wheel_threads),
            &wheel_q2_seconds,
        ));
        println!("WheelDB Q2 {:?}", wheel_q2_seconds.0);

        let wheel_q3 = awheel_run("WheelDB Q3", watermark, &tree, q3_queries);
        q3_results.add(Stats::from(wheeldb_id_fmt(wheel_threads), &wheel_q3));

        let wheel_q4_seconds = awheel_run("WheelDB Q4", watermark, &tree, q4_queries_seconds);
        q4_seconds_results.add(Stats::from(
            wheeldb_id_fmt(wheel_threads),
            &wheel_q4_seconds,
        ));
        println!("WheelDB Q4 {:?}", wheel_q4_seconds.0);

        let wheel_q5 = awheel_run("WheelTree Q5", watermark, &tree, q5_queries);
        q5_results.add(Stats::from(wheeldb_id_fmt(wheel_threads), &wheel_q5));
        println!("WheelDB Q5 {:?}", wheel_q5.0);

        let wheel_q6_seconds = awheel_run("WheelTree Q6", watermark, &tree, q6_queries_seconds);
        q6_seconds_results.add(Stats::from(
            wheeldb_id_fmt(wheel_threads),
            &wheel_q6_seconds,
        ));

        let wheel_q7 = awheel_run("WheelTree Q7", watermark, &tree, q7_queries);
        q7_results.add(Stats::from(wheeldb_id_fmt(wheel_threads), &wheel_q7));
        println!("WheelDB Q7 {:?}", wheel_q7.0);

        let wheel_q8 = awheel_run("WheelTree Q8", watermark, &tree, q8_queries);
        q8_results.add(Stats::from(wheeldb_id_fmt(wheel_threads), &wheel_q8));
        println!("WheelDB Q8 {:?}", wheel_q8.0);

        let wheeldb_memory_bytes = tree.memory_usage_bytes();
        dbg!(wheeldb_memory_bytes);

        let duck_info = duckdb_memory_usage(&duckdb);
        let duckdb_memory_bytes = duck_info.memory_usage;

        // update watermark
        current_date = watermark;

        let queries = vec![
            q1_results,
            q2_seconds_results,
            q3_results,
            q4_seconds_results,
            q5_results,
            q6_seconds_results,
            q7_results,
        ];
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

fn awheel_run<A: Aggregator + Clone>(
    _id: &str,
    _watermark: u64,
    wheel: &WheelTree<u64, A>,
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
            QueryType::TopK => {
                let _res = match query.interval {
                    TimeInterval::Range(start, end) => {
                        let (start, end) = into_offset_date_time_start_end(start, end);
                        wheel.top_k_with_time_filter(10, start, end)
                    }
                    TimeInterval::Landmark => wheel.top_k_landmark(10),
                };
                #[cfg(feature = "debug")]
                dbg!(_res);
                // assert!(_res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
            QueryType::Keyed(pu_location_id) => {
                let _res = match query.interval {
                    TimeInterval::Range(start, end) => {
                        let (start, end) = into_offset_date_time_start_end(start, end);
                        wheel
                            .get(&pu_location_id)
                            .map(|w| w.as_ref().combine_range_smart(start, end))
                    }
                    TimeInterval::Landmark => wheel.get(&pu_location_id).map(|rw| rw.landmark()),
                };
                #[cfg(feature = "debug")]
                dbg!(_res);
                // assert!(_res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
            QueryType::Range(start, end) => {
                let range = start..=end;
                let _res = match query.interval {
                    TimeInterval::Range(start, end) => {
                        let (start, end) = into_offset_date_time_start_end(start, end);
                        wheel.range_with_time_filter_smart(range, start, end)
                    }
                    TimeInterval::Landmark => wheel.landmark_range(range),
                };
                #[cfg(feature = "debug")]
                dbg!(_res);
                //assert!(res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
            QueryType::All => {
                let _res = match query.interval {
                    TimeInterval::Range(start, end) => {
                        let (start, end) = into_offset_date_time_start_end(start, end);
                        wheel.range_with_time_filter_smart(.., start, end)
                    }
                    TimeInterval::Landmark => wheel.landmark_range(..),
                };
                #[cfg(feature = "debug")]
                dbg!(_res);

                // assert!(_res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
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
    let sql_queries: Vec<String> = queries
        .into_iter()
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
        //duckdb_query(&sql_query, db).unwrap();
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
        let latency = Latency::from(&metrics.1);
        let runtime = metrics.0.as_secs_f64();
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
