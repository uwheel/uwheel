use minstant::Instant;

use clap::{ArgEnum, Parser};
use duckdb::Result;
use haw::{
    aggregator::{Aggregator, AllAggregator, F64SumAggregator},
    time,
    Entry,
    RwTreeWheel,
};
use hdrhistogram::Histogram;
use olap::*;
use std::time::Duration;

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Workload {
    /// AllAggregator
    All,
    /// F64SumAggregator
    Sum,
    TopK,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1000000)]
    keys: usize,
    #[clap(short, long, value_parser, default_value_t = 100)]
    num_batches: usize,
    #[clap(short, long, value_parser, default_value_t = 10_000)]
    batch_size: usize,
    #[clap(short, long, value_parser, default_value_t = 10_000_0)]
    events_per_min: usize,
    #[clap(short, long, value_parser, default_value_t = 10000)]
    queries: usize,
    #[clap(short, long, action)]
    disk: bool,
    #[clap(arg_enum, value_parser, default_value_t = Workload::All)]
    workload: Workload,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let total_queries = args.queries;
    let events_per_min = args.events_per_min;
    let workload = args.workload;

    println!("Running with {:#?}", args);

    let (watermark, batches) = DataGenerator::generate_query_data(events_per_min);
    let duckdb_batches = batches.clone();

    let point_queries_low_interval =
        QueryGenerator::generate_low_interval_point_queries(total_queries);
    let point_queries_high_interval =
        QueryGenerator::generate_high_interval_point_queries(total_queries);
    let olap_queries_low_interval = QueryGenerator::generate_low_interval_olap(total_queries);
    let olap_queries_high_interval = QueryGenerator::generate_high_interval_olap(total_queries);

    let random_queries_low_interval =
        QueryGenerator::generate_low_interval_random_queries(total_queries);
    let random_queries_high_interval =
        QueryGenerator::generate_high_interval_random_queries(total_queries);

    let olap_range_queries_low_interval =
        QueryGenerator::generate_low_interval_range_queries(total_queries);
    let olap_range_queries_high_interval =
        QueryGenerator::generate_high_interval_range_queries(total_queries);

    let haw_olap_queries_low_interval = olap_queries_low_interval.clone();
    let haw_olap_queries_high_interval = olap_queries_high_interval.clone();

    let total_entries = batches.len() * events_per_min;
    println!("Running with total entries {}", total_entries);

    // Prepare DuckDB
    let (mut duckdb, id) = duckdb_setup(args.disk);
    for batch in duckdb_batches {
        duckdb_append_batch(batch, &mut duckdb).unwrap();
    }
    println!("Finished preparing {}", id,);

    dbg!(watermark);

    duckdb_run(
        "DuckDB ALL Low Intervals",
        watermark,
        workload,
        &duckdb,
        olap_queries_low_interval,
    );
    duckdb_run(
        "DuckDB ALL High Intervals",
        watermark,
        workload,
        &duckdb,
        olap_queries_high_interval,
    );
    duckdb_run(
        "DuckDB POINT Low Intervals",
        watermark,
        workload,
        &duckdb,
        point_queries_low_interval.clone(),
    );
    duckdb_run(
        "DuckDB POINT High Intervals",
        watermark,
        workload,
        &duckdb,
        point_queries_high_interval.clone(),
    );
    duckdb_run(
        "DuckDB RANGE Low Intervals",
        watermark,
        workload,
        &duckdb,
        olap_range_queries_low_interval.clone(),
    );
    duckdb_run(
        "DuckDB RANGE High Intervals",
        watermark,
        workload,
        &duckdb,
        olap_range_queries_high_interval.clone(),
    );
    duckdb_run(
        "DuckDB RANDOM Low Intervals",
        watermark,
        workload,
        &duckdb,
        random_queries_low_interval.clone(),
    );
    duckdb_run(
        "DuckDB RANDOM High Intervals",
        watermark,
        workload,
        &duckdb,
        random_queries_high_interval.clone(),
    );

    fn fill_rw_tree<A: Aggregator<Input = f64> + Clone>(
        batches: Vec<Vec<RideData>>,
        rw_tree: &mut RwTreeWheel<u64, A>,
    ) {
        for batch in batches {
            for record in batch {
                rw_tree
                    .insert(
                        record.pu_location_id,
                        Entry::new(record.fare_amount, record.do_time),
                    )
                    .unwrap();
            }
            use haw::time::NumericalDuration;
            rw_tree.advance(60.seconds());
        }
    }

    match workload {
        Workload::All => {
            let mut rw_tree: RwTreeWheel<u64, AllAggregator> = RwTreeWheel::new(0);
            fill_rw_tree(batches, &mut rw_tree);
            haw_run(
                "RwTreeWheel ALL Low Intervals",
                watermark,
                &rw_tree,
                haw_olap_queries_low_interval,
            );
            haw_run(
                "RwTreeWheel ALL High Intervals",
                watermark,
                &rw_tree,
                haw_olap_queries_high_interval,
            );
            haw_run(
                "RwTreeWheel POINT Low Intervals",
                watermark,
                &rw_tree,
                point_queries_low_interval,
            );
            haw_run(
                "RwTreeWheel POINT High Intervals",
                watermark,
                &rw_tree,
                point_queries_high_interval,
            );

            haw_run(
                "RwTreeWheel RANGE Low Intervals",
                watermark,
                &rw_tree,
                olap_range_queries_low_interval,
            );
            haw_run(
                "RwTreeWheel RANGE High Intervals",
                watermark,
                &rw_tree,
                olap_range_queries_high_interval,
            );
            haw_run(
                "RwTreeWheel RANDOM Low Intervals",
                watermark,
                &rw_tree,
                random_queries_low_interval,
            );
            haw_run(
                "RwTreeWheel RANDOM High Intervals",
                watermark,
                &rw_tree,
                random_queries_high_interval,
            );
        }
        Workload::Sum => {
            let mut rw_tree: RwTreeWheel<u64, F64SumAggregator> = RwTreeWheel::new(0);
            fill_rw_tree(batches, &mut rw_tree);
            haw_run(
                "RwTreeWheel ALL Low Intervals",
                watermark,
                &rw_tree,
                haw_olap_queries_low_interval,
            );
            haw_run(
                "RwTreeWheel ALL High Intervals",
                watermark,
                &rw_tree,
                haw_olap_queries_high_interval,
            );
            haw_run(
                "RwTreeWheel POINT Low Intervals",
                watermark,
                &rw_tree,
                point_queries_low_interval,
            );
            haw_run(
                "RwTreeWheel POINT High Intervals",
                watermark,
                &rw_tree,
                point_queries_high_interval,
            );

            haw_run(
                "RwTreeWheel RANGE Low Intervals",
                watermark,
                &rw_tree,
                olap_range_queries_low_interval,
            );
            haw_run(
                "RwTreeWheel RANGE High Intervals",
                watermark,
                &rw_tree,
                olap_range_queries_high_interval,
            );
            haw_run(
                "RwTreeWheel RANDOM Low Intervals",
                watermark,
                &rw_tree,
                random_queries_low_interval,
            );
            haw_run(
                "RwTreeWheel RANDOM High Intervals",
                watermark,
                &rw_tree,
                random_queries_high_interval,
            );
        }
        Workload::TopK => {
            unimplemented!();
        }
    };
    /*
    println!("Finished preparing HAW");
    haw_run(
        "RwTreeWheel ALL Low Intervals",
        watermark,
        &rw_tree,
        haw_olap_queries_low_interval,
    );
    haw_run(
        "RwTreeWheel ALL High Intervals",
        watermark,
        &rw_tree,
        haw_olap_queries_high_interval,
    );
    haw_run(
        "RwTreeWheel POINT Low Intervals",
        watermark,
        &rw_tree,
        point_queries_low_interval,
    );
    haw_run(
        "RwTreeWheel POINT High Intervals",
        watermark,
        &rw_tree,
        point_queries_high_interval,
    );

    haw_run(
        "RwTreeWheel RANGE Low Intervals",
        watermark,
        &rw_tree,
        olap_range_queries_low_interval,
    );
    haw_run(
        "RwTreeWheel RANGE High Intervals",
        watermark,
        &rw_tree,
        olap_range_queries_high_interval,
    );
    */

    Ok(())
}
fn haw_run<A: Aggregator + Clone>(
    id: &str,
    _watermark: u64,
    wheel: &RwTreeWheel<u64, A>,
    queries: Vec<Query>,
) {
    let total_queries = queries.len();
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let full = Instant::now();
    for query in queries {
        let now = Instant::now();
        match query.query_type {
            QueryType::Keyed(pu_location_id) => {
                let _res = match query.interval {
                    QueryInterval::Seconds(secs) => wheel
                        .read()
                        .get(&pu_location_id)
                        .map(|rw| rw.interval(time::Duration::seconds(secs as i64))),
                    QueryInterval::Minutes(mins) => wheel
                        .read()
                        .get(&pu_location_id)
                        .map(|rw| rw.interval(time::Duration::minutes(mins as i64))),
                    QueryInterval::Hours(hours) => wheel
                        .read()
                        .get(&pu_location_id)
                        .map(|rw| rw.interval(time::Duration::hours(hours as i64))),
                    QueryInterval::Days(days) => wheel
                        .read()
                        .get(&pu_location_id)
                        .map(|rw| rw.interval(time::Duration::days(days as i64))),
                    QueryInterval::Landmark => {
                        wheel.read().get(&pu_location_id).map(|rw| rw.landmark())
                    }
                };
                //assert!(res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
            QueryType::Range(start, end) => {
                let range = start..end;
                let _res = match query.interval {
                    QueryInterval::Seconds(secs) => wheel
                        .read()
                        .interval_range(time::Duration::seconds(secs as i64), range),
                    QueryInterval::Minutes(mins) => wheel
                        .read()
                        .interval_range(time::Duration::minutes(mins as i64), range),
                    QueryInterval::Hours(hours) => wheel
                        .read()
                        .interval_range(time::Duration::hours(hours as i64), range),
                    QueryInterval::Days(days) => wheel
                        .read()
                        .interval_range(time::Duration::days(days as i64), range),
                    QueryInterval::Landmark => wheel.read().landmark_range(range),
                };
                //assert!(res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
            QueryType::All => {
                let _res = match query.interval {
                    QueryInterval::Seconds(secs) => {
                        wheel.interval(time::Duration::seconds(secs as i64))
                    }
                    QueryInterval::Minutes(mins) => {
                        wheel.interval(time::Duration::minutes(mins as i64))
                    }
                    QueryInterval::Hours(hours) => {
                        wheel.interval(time::Duration::hours(hours as i64))
                    }
                    QueryInterval::Days(days) => wheel.interval(time::Duration::days(days as i64)),
                    QueryInterval::Landmark => wheel.landmark(),
                };
                //assert!(res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
        };
    }
    let runtime = full.elapsed();
    println!(
        "{} ran with {} queries/s (took {:.2}s)",
        id,
        total_queries as f64 / runtime.as_secs_f64(),
        runtime.as_secs_f64(),
    );

    print_hist(id, &hist);
}

fn duckdb_run(
    id: &str,
    watermark: u64,
    workload: Workload,
    db: &duckdb::Connection,
    queries: Vec<Query>,
) {
    let total_queries = queries.len();
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let base_str = match workload {
        Workload::All => "SELECT AVG(fare_amount), SUM(fare_amount), MIN(fare_amount), MAX(fare_amount), COUNT(fare_amount) FROM rides",
        Workload::Sum => "SELECT SUM(fare_amount) FROM rides",
        Workload::TopK => unimplemented!(),
        //Workload::Avg => "SELECT AVG(fare_amount) FROM rides",
        //Workload::Min => "SELECT MIN(fare_amount) FROM rides",
        //Workload::Max => "SELECT MAX(fare_amount) FROM rides",
    };
    let sql_queries: Vec<String> = queries
        .iter()
        .map(|query| {
            // Generate Key clause. If no key then leave as empty ""
            let key_clause = match query.query_type {
                QueryType::Keyed(pu_location_id) => {
                    if let QueryInterval::Landmark = query.interval {
                        format!("WHERE pu_location_id={}", pu_location_id)
                    } else {
                        format!("WHERE pu_location_id={} AND", pu_location_id)
                    }
                }
                QueryType::Range(start, end) => {
                    if let QueryInterval::Landmark = query.interval {
                        format!("WHERE pu_location_id BETWEEN {} AND {}", start, end)
                    } else {
                        format!("WHERE pu_location_id BETWEEN {} AND {} AND", start, end)
                    }
                }
                QueryType::All => {
                    if let QueryInterval::Landmark = query.interval {
                        "".to_string()
                    } else {
                        "WHERE".to_string()
                    }
                }
            };
            let interval = match query.interval {
                QueryInterval::Seconds(secs) => {
                    let start_ts = watermark.saturating_sub(
                        time::Duration::seconds(secs.into()).whole_milliseconds() as u64,
                    );
                    format!("do_time >= {} AND do_time < {}", start_ts, watermark)
                }
                QueryInterval::Minutes(mins) => {
                    let start_ts = watermark.saturating_sub(
                        time::Duration::minutes(mins.into()).whole_milliseconds() as u64,
                    );
                    format!("do_time >= {} AND do_time < {}", start_ts, watermark)
                }
                QueryInterval::Hours(hours) => {
                    let start_ts = watermark.saturating_sub(
                        time::Duration::hours(hours.into()).whole_milliseconds() as u64,
                    );
                    format!("do_time >= {} AND do_time < {}", start_ts, watermark)
                }
                QueryInterval::Days(days) => {
                    let start_ts = watermark.saturating_sub(
                        time::Duration::days(days.into()).whole_milliseconds() as u64,
                    );
                    format!("do_time >= {} AND do_time < {}", start_ts, watermark)
                }
                QueryInterval::Landmark => "".to_string(),
            };
            // Generate SQL str to be executed
            let full_query = format!("{} {} {}", base_str, key_clause, interval);
            //println!("QUERY {}", full_query);
            full_query
        })
        .collect();

    let full = Instant::now();
    for sql_query in sql_queries {
        let now = Instant::now();
        match workload {
            Workload::All => duckdb_query_all(&sql_query, db).unwrap(),
            Workload::Sum => duckdb_query_sum(&sql_query, db).unwrap(),
            Workload::TopK => unimplemented!(),
        }
        //duckdb_query(&sql_query, db).unwrap();
        hist.record(now.elapsed().as_nanos() as u64).unwrap();
    }
    let runtime = full.elapsed();

    println!(
        "{} ran with {} queries/s (took {:.2}s)",
        id,
        total_queries as f64 / runtime.as_secs_f64(),
        runtime.as_secs_f64(),
    );
    print_hist(id, &hist);
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
