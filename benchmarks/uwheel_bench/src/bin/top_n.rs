#![allow(dead_code)]
use minstant::Instant;

use clap::Parser;
use duckdb::Result;
use hdrhistogram::Histogram;
use std::time::Duration;
use uwheel::{
    aggregator::{max::U64MaxAggregator, sum::U64SumAggregator, top_n::TopNAggregator},
    rw_wheel::read::{
        aggregation::conf::RetentionPolicy,
        hierarchical::{HawConf, WheelRange},
    },
    time_internal::Duration as Durationz,
    Aggregator,
    Entry,
    Options,
    ReadWheel,
    RwWheel,
};
use uwheel_bench::util::{duckdb_append_batch, duckdb_query_topn, Query, QueryType, *};

// struct BenchResult {
//     total_queries: usize,
//     total_entries: usize,
//     events_per_sec: usize,
//     duckdb_low: (Duration, Histogram<u64>),
//     duckdb_high: (Duration, Histogram<u64>),
//     wheel_low: (Duration, Histogram<u64>),
//     wheel_high: (Duration, Histogram<u64>),
// }
// impl BenchResult {
//     pub fn new(
//         total_queries: usize,
//         total_entries: usize,
//         events_per_sec: usize,
//         duckdb_low: (Duration, Histogram<u64>),
//         duckdb_high: (Duration, Histogram<u64>),
//         wheel_low: (Duration, Histogram<u64>),
//         wheel_high: (Duration, Histogram<u64>),
//     ) -> Self {
//         Self {
//             total_queries,
//             total_entries,
//             events_per_sec,
//             duckdb_low,
//             duckdb_high,
//             wheel_low,
//             wheel_high,
//         }
//     }
//     pub fn print(&self) {
//         let print_fn = |id: &str, total_queries: usize, runtime: f64, hist: &Histogram<u64>| {
//             println!(
//                 "{} ran with {} queries/s (took {:.2}s)",
//                 id,
//                 total_queries as f64 / runtime,
//                 runtime,
//             );
//             print_hist(id, hist);
//         };

//         print_fn(
//             "DuckDB Low",
//             self.total_queries,
//             self.duckdb_low.0.as_secs_f64(),
//             &self.duckdb_low.1,
//         );
//         print_fn(
//             "DuckDB High",
//             self.total_queries,
//             self.duckdb_high.0.as_secs_f64(),
//             &self.duckdb_high.1,
//         );
//         print_fn(
//             "Wheel Low",
//             self.total_queries,
//             self.wheel_low.0.as_secs_f64(),
//             &self.wheel_low.1,
//         );
//         print_fn(
//             "Wheel High",
//             self.total_queries,
//             self.wheel_high.0.as_secs_f64(),
//             &self.wheel_high.1,
//         );
//     }
// }

const EVENTS_PER_SEC: [usize; 4] = [1, 10, 100, 1000];
// const INTERVALS: [Durationz; 2] = [Durationz::days(1), Durationz::days(6)];
const INTERVALS: [Durationz; 1] = [Durationz::days(1)];

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1000000)]
    keys: usize,
    #[clap(short, long, value_parser, default_value_t = 100)]
    num_batches: usize,
    #[clap(short, long, value_parser, default_value_t = 10_000)]
    batch_size: usize,
    #[clap(short, long, value_parser, default_value_t = 10)]
    events_per_sec: usize,
    #[clap(short, long, value_parser, default_value_t = 1)]
    queries: usize,
    #[clap(short, long, action)]
    disk: bool,
}

fn main() -> Result<()> {
    let mut args = Args::parse();

    println!("Running with {:#?}", args);
    // let mut results = Vec::new();
    for events_per_sec in EVENTS_PER_SEC {
        args.events_per_sec = events_per_sec;
        // let result = run(&args);
        run(&args);
        // result.print();
        // results.push(result);
    }

    Ok(())
}

fn run(args: &Args) {
    let total_queries = args.queries;
    let events_per_sec = args.events_per_sec;

    let start_date = START_DATE_MS;
    let current_date = start_date;
    println!("Running with {:#?}", args);

    // let duckdb_low = duckdb_run(
    //     "DuckDB TopN Low Intervals",
    //     watermark,
    //     &duckdb,
    //     topn_queries_low_interval.clone(),
    // );
    // let duckdb_high = duckdb_run(
    //     "DuckDB TopN High Intervals",
    //     watermark,
    //     &duckdb,
    //     topn_queries_high_interval.clone(),
    // );

    // Prepare AggregateWheels
    let mut haw_conf = HawConf::default();
    haw_conf.seconds.set_retention_policy(RetentionPolicy::Keep);
    haw_conf.minutes.set_retention_policy(RetentionPolicy::Keep);
    haw_conf.hours.set_retention_policy(RetentionPolicy::Keep);
    haw_conf.days.set_retention_policy(RetentionPolicy::Keep);

    let opts = Options::default()
        .with_write_ahead(604800usize.next_power_of_two())
        .with_haw_conf(haw_conf);

    let mut rw_wheel: RwWheel<TopNAggregator<u64, 10, U64SumAggregator>> =
        RwWheel::with_options(start_date, opts);

    let print_fn =
        |id: &str, avg_ops: usize, total_queries: usize, runtime: f64, hist: &Histogram<u64>| {
            println!(
                "{} ran with avg ops {} and {} queries/s (took {:.2}s)",
                id,
                avg_ops,
                total_queries as f64 / runtime,
                runtime,
            );
            print_hist(id, hist);
        };

    for interval in INTERVALS {
        let interval_as_seconds = interval.whole_seconds() as usize;

        let (watermark, batches) =
            DataGenerator::generate_query_data(current_date, events_per_sec, interval_as_seconds);

        let duckdb_batches = batches.clone();

        let q2_queries_seconds = QueryGenerator::generate_q2_seconds(total_queries, watermark);
        let q2_queries_seconds_expensive = q2_queries_seconds.clone();
        let q2_queries_seconds_duckdb = q2_queries_seconds.clone();

        // let topn_queries_low_interval = QueryGenerator::generate_low_interval_olap(total_queries);
        // let topn_queries_high_interval = QueryGenerator::generate_high_interval_olap(total_queries);

        let total_entries = batches.len() * events_per_sec;
        println!("Running with total entries {}", total_entries);

        // Prepare DuckDB
        let (mut duckdb, id) = duckdb_setup(args.disk, 1, true);
        for batch in duckdb_batches {
            duckdb_append_batch(batch, &mut duckdb, true).unwrap();
        }
        println!("Finished preparing {}", id,);

        for batch in batches {
            for record in batch {
                rw_wheel.insert(Entry::new(
                    (record.pu_location_id, record.fare_amount),
                    record.do_time,
                ));
            }
        }
        rw_wheel.advance(interval);
        println!("Finished preparing RwWheel");

        rw_wheel.read().set_optimizer_hints(false);

        let wheel_cheap = uwheel_run(
            "Wheel TopN Cheap Hint",
            watermark,
            rw_wheel.read(),
            q2_queries_seconds,
        );
        let qps = total_queries as f64 / wheel_cheap.0.as_secs_f64();
        dbg!(qps, wheel_cheap.2);

        print_fn(
            "μWheel",
            total_queries,
            wheel_cheap.2,
            wheel_cheap.0.as_secs_f64(),
            &wheel_cheap.1,
        );

        rw_wheel.read().set_optimizer_hints(true);

        let wheel_expensive = uwheel_run(
            "Wheel TopN Expensive Hint",
            watermark,
            rw_wheel.read(),
            q2_queries_seconds_expensive,
        );

        let qps = total_queries as f64 / wheel_expensive.0.as_secs_f64();
        dbg!(qps, wheel_expensive.2);

        print_fn(
            "μWheel-hints",
            total_queries,
            wheel_expensive.2,
            wheel_expensive.0.as_secs_f64(),
            &wheel_expensive.1,
        );

        let duckdb_seconds = duckdb_run(
            "DuckDB TopN Q2 Seconds",
            watermark,
            &duckdb,
            q2_queries_seconds_duckdb.clone(),
        );

        print_fn(
            "DuckDB",
            total_queries,
            duckdb_seconds.2,
            duckdb_seconds.0.as_secs_f64(),
            &duckdb_seconds.1,
        );

        // Set Hint Expensive

        // let wheel_high = uwheel_run(
        //     "RwWheel TopN High Intervals",
        //     watermark,
        //     rw_wheel.read(),
        //     topn_queries_high_interval,
        // );
    }

    // BenchResult::new(
    //     total_queries,
    //     total_entries,
    //     events_per_sec,
    //     duckdb_low,
    //     duckdb_high,
    //     wheel_low,
    //     wheel_high,
    // )
}

fn uwheel_run(
    _id: &str,
    _watermark: u64,
    wheel: &ReadWheel<TopNAggregator<u64, 10, U64SumAggregator>>,
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>, usize) {
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let mut worst_case_ops = 0;
    let mut sum = 0;
    let mut count = 0;

    let full = Instant::now();

    for query in queries {
        if let QueryType::All = query.query_type {
            let now = Instant::now();
            let _res = match query.interval {
                TimeInterval::Range(start, end) => {
                    let (start, end) = into_offset_date_time_start_end(start, end);
                    let (agg, cost) = wheel
                        .as_ref()
                        .analyze_combine_range(WheelRange::new(start, end));
                    // dbg!(&agg);

                    worst_case_ops = U64MaxAggregator::combine(worst_case_ops, cost as u64);
                    sum += cost;
                    count += 1;
                    agg
                }
                TimeInterval::Landmark => {
                    let (agg, cost) = wheel.as_ref().analyze_landmark();
                    sum += cost;
                    count += 1;
                    agg
                }
            };
            hist.record(now.elapsed().as_nanos() as u64).unwrap();
        }
    }
    let runtime = full.elapsed();
    let avg_ops = sum / count;
    (runtime, hist, avg_ops)
}

fn duckdb_run(
    _id: &str,
    _watermark: u64,
    db: &duckdb::Connection,
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>, usize) {
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let base_str = "SELECT pu_location_id, SUM(fare_amount) FROM rides";
    let sql_queries: Vec<String> = queries
        .iter()
        .map(|query| {
            // Generate Key clause. If no key then leave as empty ""
            let key_clause = match query.query_type {
                QueryType::TopK => unimplemented!(),
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
            let order_by =
                "GROUP BY pu_location_id, fare_amount ORDER BY fare_amount DESC LIMIT 10";
            let full_query = format!("{} {} {} {}", base_str, key_clause, interval, order_by);
            //println!("QUERY {}", full_query);
            full_query
        })
        .collect();

    let full = Instant::now();
    for sql_query in sql_queries {
        let now = Instant::now();
        duckdb_query_topn(&sql_query, db).unwrap();
        hist.record(now.elapsed().as_nanos() as u64).unwrap();
    }
    let runtime = full.elapsed();
    (runtime, hist, 0)
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
