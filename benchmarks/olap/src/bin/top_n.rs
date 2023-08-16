use minstant::Instant;

use awheel::{
    aggregator::{sum::U64SumAggregator, top_n::TopNAggregator},
    time,
    Entry,
    ReadWheel,
    RwWheel,
};
use clap::Parser;
use duckdb::Result;
use hdrhistogram::Histogram;
use olap::*;
use std::time::Duration;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1000000)]
    keys: usize,
    #[clap(short, long, value_parser, default_value_t = 100)]
    num_batches: usize,
    #[clap(short, long, value_parser, default_value_t = 10_000)]
    batch_size: usize,
    #[clap(short, long, value_parser, default_value_t = 100)]
    events_per_min: usize,
    #[clap(short, long, value_parser, default_value_t = 1000)]
    queries: usize,
    #[clap(short, long, action)]
    disk: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let total_queries = args.queries;
    let events_per_min = args.events_per_min;

    println!("Running with {:#?}", args);

    let (watermark, batches) = DataGenerator::generate_query_data(events_per_min);
    let duckdb_batches = batches.clone();

    let topn_queries_low_interval = QueryGenerator::generate_low_interval_olap(total_queries);
    let topn_queries_high_interval = QueryGenerator::generate_high_interval_olap(total_queries);

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
        "DuckDB TopN Low Intervals",
        watermark,
        &duckdb,
        topn_queries_low_interval.clone(),
    );
    duckdb_run(
        "DuckDB TopN High Intervals",
        watermark,
        &duckdb,
        topn_queries_high_interval.clone(),
    );

    let mut rw_wheel: RwWheel<TopNAggregator<u64, 10, U64SumAggregator>> = RwWheel::new(0);
    for batch in batches {
        for record in batch {
            rw_wheel.insert(Entry::new(
                (record.pu_location_id, record.fare_amount as u64),
                record.do_time,
            ));
        }
        use awheel::time::NumericalDuration;
        rw_wheel.advance(60.seconds());
    }
    println!("Finished preparing RwWheel");
    awheel_run(
        "RwWheel TopN Low Intervals",
        watermark,
        rw_wheel.read(),
        topn_queries_low_interval,
    );
    awheel_run(
        "RwWheel TopN High Intervals",
        watermark,
        rw_wheel.read(),
        topn_queries_high_interval,
    );

    Ok(())
}
fn awheel_run(
    id: &str,
    _watermark: u64,
    wheel: &ReadWheel<TopNAggregator<u64, 10, U64SumAggregator>>,
    queries: Vec<Query>,
) {
    let total_queries = queries.len();
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let full = Instant::now();
    for query in queries {
        if let QueryType::All = query.query_type {
            let now = Instant::now();
            let _res = match query.interval {
                QueryInterval::Seconds(secs) => {
                    wheel.interval(time::Duration::seconds(secs as i64))
                }
                QueryInterval::Minutes(mins) => {
                    wheel.interval(time::Duration::minutes(mins as i64))
                }
                QueryInterval::Hours(hours) => wheel.interval(time::Duration::hours(hours as i64)),
                QueryInterval::Days(days) => wheel.interval(time::Duration::days(days as i64)),
                QueryInterval::Landmark => wheel.landmark(),
            };
            hist.record(now.elapsed().as_nanos() as u64).unwrap();
        }
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

fn duckdb_run(id: &str, watermark: u64, db: &duckdb::Connection, queries: Vec<Query>) {
    let total_queries = queries.len();
    let mut hist = hdrhistogram::Histogram::<u64>::new(4).unwrap();
    let base_str = "SELECT pu_location_id, SUM(fare_amount) FROM rides";
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
