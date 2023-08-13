use minstant::Instant;

use awheel::{
    aggregator::{all::AllAggregator, Aggregator},
    time,
    tree::RwTreeWheel,
    Entry,
};
use clap::{ArgEnum, Parser};
use duckdb::Result;
use hdrhistogram::Histogram;
use olap::*;
use std::time::Duration;

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Workload {
    /// AllAggregator
    All,
    /// F64SumAggregator
    Sum,
}

//const EVENTS_PER_MINS: [usize; 4] = [10, 100, 1000, 10000];
const EVENTS_PER_MINS: [usize; 3] = [10, 100, 1000];

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
    #[clap(short, long, value_parser, default_value_t = 50000)]
    queries: usize,
    #[clap(short, long, action)]
    disk: bool,
    #[clap(arg_enum, value_parser, default_value_t = Workload::All)]
    workload: Workload,
}

fn main() -> Result<()> {
    let mut args = Args::parse();

    println!("Running with {:#?}", args);

    let mut results = Vec::new();
    for events_per_min in EVENTS_PER_MINS {
        args.events_per_min = events_per_min;
        let result = run(&args);
        result.print();
        results.push(result);
    }
    #[cfg(feature = "plot")]
    plot_queries(&results);

    Ok(())
}

struct BenchResult {
    pub total_queries: usize,
    pub total_entries: usize,
    pub duckdb_low_all: (Duration, Histogram<u64>),
    pub duckdb_high_all: (Duration, Histogram<u64>),
    pub duckdb_low_point: (Duration, Histogram<u64>),
    pub duckdb_high_point: (Duration, Histogram<u64>),
    pub duckdb_low_range: (Duration, Histogram<u64>),
    pub duckdb_high_range: (Duration, Histogram<u64>),
    pub duckdb_low_random: (Duration, Histogram<u64>),
    pub duckdb_high_random: (Duration, Histogram<u64>),

    pub wheel_low_all: (Duration, Histogram<u64>),
    pub wheel_high_all: (Duration, Histogram<u64>),
    pub wheel_low_point: (Duration, Histogram<u64>),
    pub wheel_high_point: (Duration, Histogram<u64>),
    pub wheel_low_range: (Duration, Histogram<u64>),
    pub wheel_high_range: (Duration, Histogram<u64>),
    pub wheel_low_random: (Duration, Histogram<u64>),
    pub wheel_high_random: (Duration, Histogram<u64>),
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
            "DuckDB All Low Intervals",
            self.total_queries,
            self.duckdb_low_all.0.as_secs_f64(),
            &self.duckdb_low_all.1,
        );
        print_fn(
            "DuckDB All High Intervals",
            self.total_queries,
            self.duckdb_high_all.0.as_secs_f64(),
            &self.duckdb_high_all.1,
        );
        print_fn(
            "DuckDB Point Low Intervals",
            self.total_queries,
            self.duckdb_low_point.0.as_secs_f64(),
            &self.duckdb_low_point.1,
        );
        print_fn(
            "DuckDB Point High Intervals",
            self.total_queries,
            self.duckdb_high_point.0.as_secs_f64(),
            &self.duckdb_high_point.1,
        );

        print_fn(
            "DuckDB Range Low Intervals",
            self.total_queries,
            self.duckdb_low_range.0.as_secs_f64(),
            &self.duckdb_low_range.1,
        );
        print_fn(
            "DuckDB Range High Intervals",
            self.total_queries,
            self.duckdb_high_range.0.as_secs_f64(),
            &self.duckdb_high_range.1,
        );

        print_fn(
            "DuckDB Random Low Intervals",
            self.total_queries,
            self.duckdb_low_random.0.as_secs_f64(),
            &self.duckdb_low_random.1,
        );
        print_fn(
            "DuckDB Random High Intervals",
            self.total_queries,
            self.duckdb_high_random.0.as_secs_f64(),
            &self.duckdb_high_random.1,
        );

        // Wheel

        print_fn(
            "RwWheelTree All Low Intervals",
            self.total_queries,
            self.wheel_low_all.0.as_secs_f64(),
            &self.wheel_low_all.1,
        );
        print_fn(
            "RwWheelTree All High Intervals",
            self.total_queries,
            self.wheel_high_all.0.as_secs_f64(),
            &self.wheel_high_all.1,
        );
        print_fn(
            "RwWheelTree Point Low Intervals",
            self.total_queries,
            self.wheel_low_point.0.as_secs_f64(),
            &self.wheel_low_point.1,
        );
        print_fn(
            "RwWheelTree Point High Intervals",
            self.total_queries,
            self.wheel_high_point.0.as_secs_f64(),
            &self.wheel_high_point.1,
        );

        print_fn(
            "RwWheelTree Range Low Intervals",
            self.total_queries,
            self.wheel_low_range.0.as_secs_f64(),
            &self.wheel_low_range.1,
        );

        print_fn(
            "RwWheelTree Range High Intervals",
            self.total_queries,
            self.wheel_high_range.0.as_secs_f64(),
            &self.wheel_high_range.1,
        );

        print_fn(
            "RwWheelTree Random Low Intervals",
            self.total_queries,
            self.wheel_low_random.0.as_secs_f64(),
            &self.wheel_low_random.1,
        );
        print_fn(
            "RwWheelTree Random High Intervals",
            self.total_queries,
            self.wheel_high_random.0.as_secs_f64(),
            &self.wheel_high_random.1,
        );
    }
}

fn run(args: &Args) -> BenchResult {
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

    let awheel_olap_queries_low_interval = olap_queries_low_interval.clone();
    let awheel_olap_queries_high_interval = olap_queries_high_interval.clone();

    let total_entries = batches.len() * events_per_min;
    println!("Running with total entries {}", total_entries);

    // Prepare DuckDB
    let (mut duckdb, id) = duckdb_setup(args.disk);
    for batch in duckdb_batches {
        duckdb_append_batch(batch, &mut duckdb).unwrap();
    }
    println!("Finished preparing {}", id,);

    dbg!(watermark);

    let duckdb_low_all = duckdb_run(
        "DuckDB ALL Low Intervals",
        watermark,
        workload,
        &duckdb,
        olap_queries_low_interval,
    );
    let duckdb_high_all = duckdb_run(
        "DuckDB ALL High Intervals",
        watermark,
        workload,
        &duckdb,
        olap_queries_high_interval,
    );
    let duckdb_low_point = duckdb_run(
        "DuckDB POINT Low Intervals",
        watermark,
        workload,
        &duckdb,
        point_queries_low_interval.clone(),
    );
    let duckdb_high_point = duckdb_run(
        "DuckDB POINT High Intervals",
        watermark,
        workload,
        &duckdb,
        point_queries_high_interval.clone(),
    );
    let duckdb_low_range = duckdb_run(
        "DuckDB RANGE Low Intervals",
        watermark,
        workload,
        &duckdb,
        olap_range_queries_low_interval.clone(),
    );
    let duckdb_high_range = duckdb_run(
        "DuckDB RANGE High Intervals",
        watermark,
        workload,
        &duckdb,
        olap_range_queries_high_interval.clone(),
    );
    let duckdb_low_random = duckdb_run(
        "DuckDB RANDOM Low Intervals",
        watermark,
        workload,
        &duckdb,
        random_queries_low_interval.clone(),
    );
    let duckdb_high_random = duckdb_run(
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
            use awheel::time::NumericalDuration;
            rw_tree.advance(60.seconds());
        }
    }
    let mut rw_tree: RwTreeWheel<u64, AllAggregator> = RwTreeWheel::new(0);
    fill_rw_tree(batches, &mut rw_tree);
    let wheel_low_all = awheel_run(
        "RwTreeWheel ALL Low Intervals",
        watermark,
        &rw_tree,
        awheel_olap_queries_low_interval,
    );
    let wheel_high_all = awheel_run(
        "RwTreeWheel ALL High Intervals",
        watermark,
        &rw_tree,
        awheel_olap_queries_high_interval,
    );
    let wheel_low_point = awheel_run(
        "RwTreeWheel POINT Low Intervals",
        watermark,
        &rw_tree,
        point_queries_low_interval,
    );
    let wheel_high_point = awheel_run(
        "RwTreeWheel POINT High Intervals",
        watermark,
        &rw_tree,
        point_queries_high_interval,
    );

    let wheel_low_range = awheel_run(
        "RwTreeWheel RANGE Low Intervals",
        watermark,
        &rw_tree,
        olap_range_queries_low_interval,
    );
    let wheel_high_range = awheel_run(
        "RwTreeWheel RANGE High Intervals",
        watermark,
        &rw_tree,
        olap_range_queries_high_interval,
    );
    let wheel_low_random = awheel_run(
        "RwTreeWheel RANDOM Low Intervals",
        watermark,
        &rw_tree,
        random_queries_low_interval,
    );
    let wheel_high_random = awheel_run(
        "RwTreeWheel RANDOM High Intervals",
        watermark,
        &rw_tree,
        random_queries_high_interval,
    );
    BenchResult {
        total_queries,
        total_entries,
        duckdb_low_all,
        duckdb_high_all,
        duckdb_low_point,
        duckdb_high_point,
        duckdb_low_range,
        duckdb_high_range,
        duckdb_low_random,
        duckdb_high_random,
        wheel_low_all,
        wheel_high_all,
        wheel_low_point,
        wheel_high_point,
        wheel_low_range,
        wheel_high_range,
        wheel_low_random,
        wheel_high_random,
    }
}

fn awheel_run<A: Aggregator + Clone>(
    _id: &str,
    _watermark: u64,
    wheel: &RwTreeWheel<u64, A>,
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>) {
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
                    QueryInterval::Weeks(weeks) => wheel
                        .read()
                        .get(&pu_location_id)
                        .map(|rw| rw.interval(time::Duration::weeks(weeks as i64))),
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
                    QueryInterval::Weeks(weeks) => wheel
                        .read()
                        .interval_range(time::Duration::weeks(weeks as i64), range),
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
                    QueryInterval::Weeks(weeks) => {
                        wheel.interval(time::Duration::weeks(weeks as i64))
                    }
                    QueryInterval::Landmark => wheel.landmark(),
                };
                //assert!(res.is_some());
                hist.record(now.elapsed().as_nanos() as u64).unwrap();
            }
        };
    }
    let runtime = full.elapsed();
    (runtime, hist)
}

fn duckdb_run(
    _id: &str,
    watermark: u64,
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
                QueryInterval::Weeks(weeks) => {
                    let start_ts = watermark.saturating_sub(
                        time::Duration::weeks(weeks.into()).whole_milliseconds() as u64,
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

#[cfg(feature = "plot")]
fn plot_queries(results: &Vec<BenchResult>) {
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = results.iter().map(|e| e.total_entries as f64).collect();
    let mut duckdb_low_all_y = Vec::new();
    let mut duckdb_high_all_y = Vec::new();
    let mut duckdb_low_point_y = Vec::new();
    let mut duckdb_high_point_y = Vec::new();
    let mut duckdb_low_range_y = Vec::new();
    let mut duckdb_high_range_y = Vec::new();
    let mut duckdb_low_random_y = Vec::new();
    let mut duckdb_high_random_y = Vec::new();

    let mut wheel_low_all_y = Vec::new();
    let mut wheel_high_all_y = Vec::new();
    let mut wheel_low_point_y = Vec::new();
    let mut wheel_high_point_y = Vec::new();
    let mut wheel_low_range_y = Vec::new();
    let mut wheel_high_range_y = Vec::new();
    let mut wheel_low_random_y = Vec::new();
    let mut wheel_high_random_y = Vec::new();

    for result in results {
        let throughput = result.total_queries as f64 / result.duckdb_low_all.0.as_secs_f64();
        duckdb_low_all_y.push(throughput);

        let throughput = result.total_queries as f64 / result.duckdb_high_all.0.as_secs_f64();
        duckdb_high_all_y.push(throughput);

        let throughput = result.total_queries as f64 / result.duckdb_low_point.0.as_secs_f64();
        duckdb_low_point_y.push(throughput);

        let throughput = result.total_queries as f64 / result.duckdb_high_point.0.as_secs_f64();
        duckdb_high_point_y.push(throughput);

        let throughput = result.total_queries as f64 / result.duckdb_low_range.0.as_secs_f64();
        duckdb_low_range_y.push(throughput);

        let throughput = result.total_queries as f64 / result.duckdb_high_range.0.as_secs_f64();
        duckdb_high_range_y.push(throughput);

        let throughput = result.total_queries as f64 / result.duckdb_low_random.0.as_secs_f64();
        duckdb_low_random_y.push(throughput);

        let throughput = result.total_queries as f64 / result.duckdb_high_random.0.as_secs_f64();
        duckdb_high_random_y.push(throughput);

        let throughput = result.total_queries as f64 / result.wheel_low_all.0.as_secs_f64();
        wheel_low_all_y.push(throughput);

        let throughput = result.total_queries as f64 / result.wheel_high_all.0.as_secs_f64();
        wheel_high_all_y.push(throughput);

        let throughput = result.total_queries as f64 / result.wheel_low_point.0.as_secs_f64();
        wheel_low_point_y.push(throughput);

        let throughput = result.total_queries as f64 / result.wheel_high_point.0.as_secs_f64();
        wheel_high_point_y.push(throughput);

        let throughput = result.total_queries as f64 / result.wheel_low_range.0.as_secs_f64();
        wheel_low_range_y.push(throughput);

        let throughput = result.total_queries as f64 / result.wheel_high_range.0.as_secs_f64();
        wheel_high_range_y.push(throughput);

        let throughput = result.total_queries as f64 / result.wheel_low_random.0.as_secs_f64();
        wheel_low_random_y.push(throughput);

        let throughput = result.total_queries as f64 / result.wheel_high_random.0.as_secs_f64();
        wheel_high_random_y.push(throughput);
    }

    let mut duckdb_low_all_curve = Curve::new();
    duckdb_low_all_curve.set_label("DuckDB Low ALL");
    duckdb_low_all_curve.set_line_color("r");
    duckdb_low_all_curve.set_marker_style("^");
    duckdb_low_all_curve.draw(&x, &duckdb_low_all_y);

    let mut duckdb_high_all_curve = Curve::new();
    duckdb_high_all_curve.set_label("DuckDB High ALL");
    duckdb_high_all_curve.set_line_color("r");
    duckdb_high_all_curve.set_marker_style("o");
    duckdb_high_all_curve.draw(&x, &duckdb_high_all_y);

    let mut duckdb_low_point_curve = Curve::new();
    duckdb_low_point_curve.set_label("DuckDB Low Point");
    duckdb_low_point_curve.set_line_color("m");
    duckdb_low_point_curve.set_marker_style("^");
    duckdb_low_point_curve.draw(&x, &duckdb_low_point_y);

    let mut duckdb_high_point_curve = Curve::new();
    duckdb_high_point_curve.set_label("DuckDB High Point");
    duckdb_high_point_curve.set_line_color("m");
    duckdb_high_point_curve.set_marker_style("o");
    duckdb_high_point_curve.draw(&x, &duckdb_high_point_y);

    let mut duckdb_low_range_curve = Curve::new();
    duckdb_low_range_curve.set_label("DuckDB Low Range");
    duckdb_low_range_curve.set_line_color("y");
    duckdb_low_range_curve.set_marker_style("^");
    duckdb_low_range_curve.draw(&x, &duckdb_low_range_y);

    let mut duckdb_high_range_curve = Curve::new();
    duckdb_high_range_curve.set_label("DuckDB High Range");
    duckdb_high_range_curve.set_line_color("y");
    duckdb_high_range_curve.set_marker_style("o");
    duckdb_high_range_curve.draw(&x, &duckdb_high_range_y);

    let mut duckdb_low_random_curve = Curve::new();
    duckdb_low_random_curve.set_label("DuckDB Low Random");
    duckdb_low_random_curve.set_line_color("c");
    duckdb_low_random_curve.set_marker_style("^");
    duckdb_low_random_curve.draw(&x, &duckdb_low_random_y);

    let mut duckdb_high_random_curve = Curve::new();
    duckdb_high_random_curve.set_label("DuckDB High Random");
    duckdb_high_random_curve.set_line_color("c");
    duckdb_high_random_curve.set_marker_style("o");
    duckdb_high_random_curve.draw(&x, &duckdb_high_range_y);

    let mut wheel_low_all_curve = Curve::new();
    wheel_low_all_curve.set_label("Wheel Low ALL");
    wheel_low_all_curve.set_line_color("k");
    wheel_low_all_curve.set_marker_style("^");
    wheel_low_all_curve.draw(&x, &wheel_low_all_y);

    let mut wheel_high_all_curve = Curve::new();
    wheel_high_all_curve.set_label("Wheel High ALL");
    wheel_high_all_curve.set_line_color("k");
    wheel_high_all_curve.set_marker_style("o");
    wheel_high_all_curve.draw(&x, &wheel_high_all_y);

    let mut wheel_low_point_curve = Curve::new();
    wheel_low_point_curve.set_label("Wheel Low Point");
    wheel_low_point_curve.set_line_color("#FFA833");
    wheel_low_point_curve.set_marker_style("^");
    wheel_low_point_curve.draw(&x, &wheel_low_point_y);

    let mut wheel_high_point_curve = Curve::new();
    wheel_high_point_curve.set_label("Wheel High Point");
    wheel_high_point_curve.set_line_color("#FFA833");
    wheel_high_point_curve.set_marker_style("o");
    wheel_high_point_curve.draw(&x, &wheel_high_point_y);

    let mut wheel_low_range_curve = Curve::new();
    wheel_low_range_curve.set_label("Wheel Low Range");
    wheel_low_range_curve.set_line_color("#FF33FC");
    wheel_low_range_curve.set_marker_style("^");
    wheel_low_range_curve.draw(&x, &wheel_low_range_y);

    let mut wheel_high_range_curve = Curve::new();
    wheel_high_range_curve.set_label("Wheel High Range");
    wheel_high_range_curve.set_line_color("#FF33FC");
    wheel_high_range_curve.set_marker_style("o");
    wheel_high_range_curve.draw(&x, &wheel_high_range_y);

    let mut wheel_low_random_curve = Curve::new();
    wheel_low_random_curve.set_label("Wheel Low Random");
    wheel_low_random_curve.set_line_color("#33FFBC");
    wheel_low_random_curve.set_marker_style("^");
    wheel_low_random_curve.draw(&x, &wheel_low_random_y);

    let mut wheel_high_random_curve = Curve::new();
    wheel_high_random_curve.set_label("Wheel High Random");
    wheel_high_random_curve.set_line_color("#33FFBC");
    wheel_high_random_curve.set_marker_style("o");
    wheel_high_random_curve.draw(&x, &wheel_high_random_y);

    let mut legend = Legend::new();
    legend.set_outside(true);
    legend.set_num_col(2);
    legend.draw();

    // configure plot
    let mut plot = Plot::new();
    plot.set_horizontal_gap(0.5)
        .set_vertical_gap(0.5)
        .set_gaps(0.3, 0.2);

    plot.set_label_y("Throughput (queries/s)");
    plot.set_log_y(true);
    plot.set_log_x(true);
    plot.set_label_x("Total insert records");

    plot.add(&duckdb_low_all_curve)
        .add(&duckdb_high_all_curve)
        .add(&duckdb_low_point_curve)
        .add(&duckdb_high_point_curve)
        .add(&duckdb_low_range_curve)
        .add(&duckdb_high_range_curve)
        .add(&duckdb_low_random_curve)
        .add(&duckdb_high_random_curve)
        .add(&wheel_low_all_curve)
        .add(&wheel_high_all_curve)
        .add(&wheel_low_point_curve)
        .add(&wheel_high_point_curve)
        .add(&wheel_low_range_curve)
        .add(&wheel_high_range_curve)
        .add(&wheel_low_random_curve)
        .add(&wheel_high_random_curve)
        .add(&legend);

    let path = Path::new("../results/query_throughput.png");
    plot.save(&path).unwrap();
}
