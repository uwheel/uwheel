use awheel::{
    aggregator::{sum::F64SumAggregator, Aggregator},
    time_internal::NumericalDuration,
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
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    thread::available_parallelism,
    time::Duration,
};

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Workload {
    /// AllAggregator
    All,
    /// F64SumAggregator
    Sum,
}

const EVENTS_PER_SEC: [usize; 1] = [1];

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
    let mut args = Args::parse();

    println!("Running with {:#?}", args);

    let mut runs = Vec::new();
    for events_per_sec in EVENTS_PER_SEC {
        args.events_per_sec = events_per_sec;
        let run = run(&args);
        // result.print();
        runs.push(run);
    }
    let output = PlottingOutput::from(args.queries, runs);
    output.flush_to_file().unwrap();

    Ok(())
}

#[derive(Debug, serde::Serialize)]
pub struct PlottingOutput {
    total_queries: usize,
    runs: Vec<Run>,
}

impl PlottingOutput {
    pub fn flush_to_file(&self) -> serde_json::Result<()> {
        let path = format!("../results/olap_bench.json");
        let file = File::create(path).unwrap();
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
}

impl PlottingOutput {
    pub fn from(total_queries: usize, runs: Vec<Run>) -> Self {
        Self {
            total_queries,
            runs,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Run {
    events_per_second: u64,
    queries: Vec<QueryDescription>,
}

impl Run {
    pub fn from(events_per_second: u64, queries: Vec<QueryDescription>) -> Self {
        Self {
            events_per_second,
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

fn run(args: &Args) -> Run {
    let total_queries = args.queries;
    let events_per_sec = args.events_per_sec;
    let workload = args.workload;
    let _iterations = args.iterations;
    let start_date = START_DATE_MS;

    // Generate the data to be inserted to DuckDB & WheelDB
    let (watermark, batches) = DataGenerator::generate_query_data(start_date, events_per_sec);
    let duckdb_batches = batches.clone();

    // Generate Q1

    let q1_queries = QueryGenerator::generate_q1(total_queries);
    let q1_queries_duckdb = q1_queries.clone();

    // Generate Q2

    let q2_queries_seconds = QueryGenerator::generate_q2_seconds(total_queries);
    let q2_queries_seconds_duckdb = q2_queries_seconds.clone();

    // Generate Q3

    let q3_queries = QueryGenerator::generate_q3(total_queries);
    let q3_queries_duckdb = q3_queries.clone();

    // Generate Q4

    let q4_queries_seconds = QueryGenerator::generate_q4_seconds(total_queries);
    let q4_queries_seconds_duckdb = q4_queries_seconds.clone();

    // Generate Q5

    let q5_queries = QueryGenerator::generate_q5(total_queries);
    let q5_queries_duckdb = q5_queries.clone();

    // Generate Q6

    let q6_queries_seconds = QueryGenerator::generate_q6_seconds(total_queries);
    let q6_queries_seconds_duckdb = q6_queries_seconds.clone();

    let total_entries = batches.len() * events_per_sec;
    println!("Running with total entries {}", total_entries);

    let max_parallelism = available_parallelism().unwrap().get();

    // Prepare DuckDB
    let (mut duckdb, id) = duckdb_setup(args.disk, max_parallelism);
    for batch in duckdb_batches {
        duckdb_append_batch(batch, &mut duckdb).unwrap();
    }
    println!("Finished preparing {}", id,);

    dbg!(watermark);

    let mut q1_results = QueryDescription::from("q1");
    let mut q2_seconds_results = QueryDescription::from("q2");
    let mut q3_results = QueryDescription::from("q3");
    let mut q4_seconds_results = QueryDescription::from("q4");
    let mut q5_results = QueryDescription::from("q5");
    let mut q6_seconds_results = QueryDescription::from("q6");

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

    fn get_unique_ids(data: &Vec<Vec<RideData>>) -> Vec<u64> {
        let mut unique_ids = HashSet::new();

        for ride_data_vec in data {
            for ride_data in ride_data_vec {
                unique_ids.insert(ride_data.pu_location_id);
            }
        }

        unique_ids.into_iter().collect()
    }

    fn fill_tree<A: Aggregator<Input = f64> + Clone>(
        start_time: u64,
        batches: Vec<Vec<RideData>>,
        tree: &mut WheelTree<u64, A>,
    ) where
        A::PartialAggregate: Sync,
    {
        let unique_ids = get_unique_ids(&batches);
        let mut wheels = HashMap::with_capacity(unique_ids.len());
        let opts = Options::default().with_write_ahead(604800usize.next_power_of_two());
        let mut star = RwWheel::<A>::with_options(start_time, opts);
        for id in unique_ids {
            let wheel = RwWheel::<A>::with_options(start_time, opts);
            wheels.insert(id, wheel);
        }
        for batch in batches {
            // 1 batch represents 1 second of data
            for record in batch {
                let wheel = wheels.get_mut(&record.pu_location_id).unwrap();
                let entry = Entry::new(record.fare_amount, record.do_time);
                wheel.insert(entry);
                star.insert(entry)
            }
        }

        // advance all wheels by 7 days
        for wheel in wheels.values_mut() {
            wheel.advance(7.days());
        }

        // advance star wheel
        star.advance(7.days());
        tree.insert_star(star.read().clone());

        // insert the filled wheels into the tree
        for (id, wheel) in wheels {
            tree.insert(id, wheel.read().clone());
        }
    }

    let wheeldb_id_fmt = |threads: usize| format!("wheeldb-threads-{}", threads);

    let wheel_threads = 1;
    let mut tree: WheelTree<u64, F64SumAggregator> = WheelTree::default();
    println!("Preparing WheelDB");
    fill_tree(start_date, batches, &mut tree);
    println!("Finished preparing WheelDB");

    let wheel_q1 = awheel_run("WheelDB Q1", watermark, &tree, q1_queries);
    println!("WheelTree Q1 {:?}", wheel_q1.0);
    q1_results.add(Stats::from(wheeldb_id_fmt(wheel_threads), &wheel_q1));

    let wheel_q2_seconds = awheel_run("WheelDB Q2 Seconds", watermark, &tree, q2_queries_seconds);
    q2_seconds_results.add(Stats::from(
        wheeldb_id_fmt(wheel_threads),
        &wheel_q2_seconds,
    ));
    println!("WheelTree Q2 Seconds {:?}", wheel_q2_seconds.0);

    let wheel_q3 = awheel_run("WheelDB Q3", watermark, &tree, q3_queries);
    q3_results.add(Stats::from(wheeldb_id_fmt(wheel_threads), &wheel_q3));

    let wheel_q4_seconds = awheel_run("WheelDB Q4", watermark, &tree, q4_queries_seconds);
    q4_seconds_results.add(Stats::from(
        wheeldb_id_fmt(wheel_threads),
        &wheel_q4_seconds,
    ));
    println!("WheelTree Q4 {:?}", wheel_q4_seconds.0);

    let wheel_q5 = awheel_run("WheelTree Q5", watermark, &tree, q5_queries);
    q5_results.add(Stats::from(wheeldb_id_fmt(wheel_threads), &wheel_q5));
    println!("WheelTree Q5 {:?}", wheel_q5.0);

    let wheel_q6_seconds = awheel_run("WheelTree Q6", watermark, &tree, q6_queries_seconds);
    q6_seconds_results.add(Stats::from(
        wheeldb_id_fmt(wheel_threads),
        &wheel_q6_seconds,
    ));

    let queries = vec![
        q1_results,
        q2_seconds_results,
        q3_results,
        q4_seconds_results,
        q5_results,
        q6_seconds_results,
    ];
    Run::from(events_per_sec as u64, queries)
}

fn awheel_run<A: Aggregator + Clone>(
    _id: &str,
    _watermark: u64,
    wheel: &WheelTree<u64, A>,
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>)
where
    A::PartialAggregate: Sync + PartialEq,
{
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let full = Instant::now();
    for query in queries {
        let now = Instant::now();
        match query.query_type {
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
    let base_str = match workload {
        Workload::All => "SELECT AVG(fare_amount), SUM(fare_amount), MIN(fare_amount), MAX(fare_amount), COUNT(fare_amount) FROM rides",
        Workload::Sum => "SELECT SUM(fare_amount) FROM rides",
    };
    let sql_queries: Vec<String> = queries
        .into_iter()
        .map(|query| {
            // Generate Key clause. If no key then leave as empty ""
            let key_clause = match query.query_type {
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
            // Generate SQL str to be executed
            let full_query = format!("{} {} {}", base_str, key_clause, interval);
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
