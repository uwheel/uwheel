use minstant::Instant;

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
use olap::*;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Workload {
    /// AllAggregator
    All,
    /// F64SumAggregator
    Sum,
}

const EVENTS_PER_SEC: [usize; 3] = [1, 10, 100];

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1000000)]
    keys: usize,
    #[clap(short, long, value_parser, default_value_t = 100)]
    num_batches: usize,
    #[clap(short, long, value_parser, default_value_t = 10_000)]
    batch_size: usize,
    #[clap(short, long, value_parser, default_value_t = 1)]
    events_per_sec: usize,
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

    let mut results = Vec::new();
    for events_per_sec in EVENTS_PER_SEC {
        args.events_per_sec = events_per_sec;
        let result = run(&args);
        result.print();
        results.push(result);
    }

    Ok(())
}

struct BenchResult {
    pub total_queries: usize,
    pub total_entries: usize,
    pub duckdb_q1: (Duration, Histogram<u64>),
    pub duckdb_q2: (Duration, Histogram<u64>),
    pub duckdb_q3: (Duration, Histogram<u64>),
    pub duckdb_q4: (Duration, Histogram<u64>),
    pub duckdb_q5: (Duration, Histogram<u64>),
    pub duckdb_q6: (Duration, Histogram<u64>),

    pub wheel_q1: (Duration, Histogram<u64>),
    pub wheel_q2: (Duration, Histogram<u64>),
    pub wheel_q3: (Duration, Histogram<u64>),
    pub wheel_q4: (Duration, Histogram<u64>),
    pub wheel_q5: (Duration, Histogram<u64>),
    pub wheel_q6: (Duration, Histogram<u64>),
}
impl BenchResult {
    pub fn print(&self) {
        let print_fn = |id: &str, total_queries: usize, runtime: f64, hist: &Histogram<u64>| {
            println!(
                "{} ran with {} queries/s (took {:.2}s)",
                id,
                total_queries as f64 / runtime,
                runtime,
            );
            print_hist(id, hist);
        };

        print_fn(
            "DuckDB Q1",
            self.total_queries,
            self.duckdb_q1.0.as_secs_f64(),
            &self.duckdb_q1.1,
        );
        print_fn(
            "DuckDB Q2",
            self.total_queries,
            self.duckdb_q2.0.as_secs_f64(),
            &self.duckdb_q2.1,
        );
        print_fn(
            "DuckDB Q3",
            self.total_queries,
            self.duckdb_q3.0.as_secs_f64(),
            &self.duckdb_q3.1,
        );
        print_fn(
            "DuckDB Q4",
            self.total_queries,
            self.duckdb_q4.0.as_secs_f64(),
            &self.duckdb_q4.1,
        );

        print_fn(
            "DuckDB Q5",
            self.total_queries,
            self.duckdb_q5.0.as_secs_f64(),
            &self.duckdb_q5.1,
        );
        print_fn(
            "DuckDB Q6",
            self.total_queries,
            self.duckdb_q6.0.as_secs_f64(),
            &self.duckdb_q6.1,
        );

        // Wheel

        print_fn(
            "WheelTree Q1",
            self.total_queries,
            self.wheel_q1.0.as_secs_f64(),
            &self.wheel_q1.1,
        );

        print_fn(
            "WheelTree Q2",
            self.total_queries,
            self.wheel_q2.0.as_secs_f64(),
            &self.wheel_q2.1,
        );
        print_fn(
            "WheelTree Q3",
            self.total_queries,
            self.wheel_q3.0.as_secs_f64(),
            &self.wheel_q3.1,
        );
        print_fn(
            "WheelTree Q4",
            self.total_queries,
            self.wheel_q4.0.as_secs_f64(),
            &self.wheel_q4.1,
        );

        print_fn(
            "WheelTree Q5",
            self.total_queries,
            self.wheel_q5.0.as_secs_f64(),
            &self.wheel_q5.1,
        );

        print_fn(
            "WheelTree Q6",
            self.total_queries,
            self.wheel_q6.0.as_secs_f64(),
            &self.wheel_q6.1,
        );
    }
}

fn run(args: &Args) -> BenchResult {
    let total_queries = args.queries;
    let events_per_sec = args.events_per_sec;
    let workload = args.workload;
    let start_date = START_DATE_MS;

    let (watermark, batches) = DataGenerator::generate_query_data(start_date, events_per_sec);
    let duckdb_batches = batches.clone();

    // Generate Q1

    let q1_queries = QueryGenerator::generate_q1(total_queries);
    let q1_queries_duckdb = q1_queries.clone();

    // Generate Q2

    let q2_queries = QueryGenerator::generate_q2(total_queries);
    let q2_queries_duckdb = q2_queries.clone();

    // Generate Q3

    let q3_queries = QueryGenerator::generate_q3(total_queries);
    let q3_queries_duckdb = q3_queries.clone();

    // Generate Q4

    let q4_queries = QueryGenerator::generate_q4(total_queries);
    let q4_queries_duckdb = q4_queries.clone();

    // Generate Q5

    let q5_queries = QueryGenerator::generate_q5(total_queries);
    let q5_queries_duckdb = q5_queries.clone();

    // Generate Q6

    let q6_queries = QueryGenerator::generate_q6(total_queries);
    let q6_queries_duckdb = q6_queries.clone();

    let total_entries = batches.len() * events_per_sec;
    println!("Running with total entries {}", total_entries);

    // Prepare DuckDB
    let (mut duckdb, id) = duckdb_setup(args.disk);
    for batch in duckdb_batches {
        duckdb_append_batch(batch, &mut duckdb).unwrap();
    }
    println!("Finished preparing {}", id,);

    dbg!(watermark);

    let duckdb_q1 = duckdb_run("DuckDB Q1", watermark, workload, &duckdb, q1_queries_duckdb);
    println!("DuckDB Q1 {:?}", duckdb_q1.0);

    let duckdb_q2 = duckdb_run("DuckDB Q2", watermark, workload, &duckdb, q2_queries_duckdb);
    println!("DuckDB Q2 {:?}", duckdb_q2.0);

    let duckdb_q3 = duckdb_run("DuckDB Q3", watermark, workload, &duckdb, q3_queries_duckdb);
    println!("DuckDB Q3 {:?}", duckdb_q3.0);

    let duckdb_q4 = duckdb_run("DuckDB Q4", watermark, workload, &duckdb, q4_queries_duckdb);
    println!("DuckDB Q4 {:?}", duckdb_q4.0);

    let duckdb_q5 = duckdb_run("DuckDB Q5", watermark, workload, &duckdb, q5_queries_duckdb);
    println!("DuckDB Q5 {:?}", duckdb_q5.0);

    let duckdb_q6 = duckdb_run("DuckDB Q6", watermark, workload, &duckdb, q6_queries_duckdb);
    println!("DuckDB Q6 {:?}", duckdb_q6.0);

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
        tree: &WheelTree<u64, A>,
    ) where
        A::PartialAggregate: Sync,
    {
        let unique_ids = get_unique_ids(&batches);
        let mut wheels = HashMap::with_capacity(unique_ids.len());
        let opts = Options::default().with_write_ahead(604800usize.next_power_of_two());
        for id in unique_ids {
            let wheel = RwWheel::<A>::with_options(start_time, opts);
            wheels.insert(id, wheel);
        }
        // let wheels = HashMap<u64, RwWheel<A>>;
        for batch in batches {
            // 1 batch represents 1 second of data
            for record in batch {
                let wheel = wheels.get_mut(&record.pu_location_id).unwrap();
                wheel.insert(Entry::new(record.fare_amount, record.do_time));
            }
        }

        // advance all wheels by 7 days
        for wheel in wheels.values_mut() {
            wheel.advance(7.days());
            // dbg!(wheel.read().as_ref().current_time_in_cycle());
        }

        // insert the filled wheels into the tree
        for (id, wheel) in wheels {
            tree.insert(id, wheel.read().clone());
        }
    }

    let tree: WheelTree<u64, F64SumAggregator> = WheelTree::default();
    println!("Preparing WheelTree");
    fill_tree(start_date, batches, &tree);
    println!("Finished preparing WheelTree");

    let wheel_q1 = awheel_run("WheelTree Q1", watermark, &tree, q1_queries);
    println!("WheelTree Q1 {:?}", wheel_q1.0);

    let wheel_q2 = awheel_run("WheelTree Q2", watermark, &tree, q2_queries);
    println!("WheelTree Q2 {:?}", wheel_q2.0);

    let wheel_q3 = awheel_run("WheelTree Q3", watermark, &tree, q3_queries);
    println!("WheelTree Q3 {:?}", wheel_q3.0);

    let wheel_q4 = awheel_run("WheelTree Q4", watermark, &tree, q4_queries);
    println!("WheelTree Q4 {:?}", wheel_q4.0);

    let wheel_q5 = awheel_run("WheelTree Q5", watermark, &tree, q5_queries);
    println!("WheelTree Q5 {:?}", wheel_q5.0);

    let wheel_q6 = awheel_run("WheelTree Q6", watermark, &tree, q6_queries);
    println!("WheelTree Q6 {:?}", wheel_q6.0);

    BenchResult {
        total_queries,
        total_entries,
        duckdb_q1,
        duckdb_q2,
        duckdb_q3,
        duckdb_q4,
        duckdb_q5,
        duckdb_q6,
        wheel_q1,
        wheel_q2,
        wheel_q3,
        wheel_q4,
        wheel_q5,
        wheel_q6,
    }
}

fn awheel_run<A: Aggregator + Clone>(
    _id: &str,
    _watermark: u64,
    wheel: &WheelTree<u64, A>,
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>)
where
    A::PartialAggregate: Sync,
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
                            .map(|w| w.as_ref().combine_range(start, end))
                    }
                    TimeInterval::Landmark => wheel.get(&pu_location_id).map(|rw| rw.landmark()),
                };
                assert!(_res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
            QueryType::Range(start, end) => {
                let range = start..end;
                let _res = match query.interval {
                    TimeInterval::Range(start, end) => {
                        let (start, end) = into_offset_date_time_start_end(start, end);
                        wheel.range_with_time_filter(range, start, end)
                    }
                    TimeInterval::Landmark => wheel.landmark_range(range),
                };
                //assert!(res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
            QueryType::All => {
                let _res = match query.interval {
                    TimeInterval::Range(start, end) => {
                        let (start, end) = into_offset_date_time_start_end(start, end);
                        wheel.range_with_time_filter(.., start, end)
                    }
                    TimeInterval::Landmark => wheel.landmark_range(..),
                };
                // dbg!(_res);
                assert!(_res.is_some());
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
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>) {
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let base_str = match workload {
        Workload::All => "SELECT AVG(fare_amount), SUM(fare_amount), MIN(fare_amount), MAX(fare_amount), COUNT(fare_amount) FROM rides",
        Workload::Sum => "SELECT SUM(fare_amount) FROM rides",
    };
    let sql_queries: Vec<String> = queries
        .iter()
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

fn print_hist(id: &str, hist: &Histogram<u64>) {
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
