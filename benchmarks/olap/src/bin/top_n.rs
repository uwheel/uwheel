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

struct BenchResult {
    total_queries: usize,
    total_entries: usize,
    duckdb_low: (Duration, Histogram<u64>),
    duckdb_high: (Duration, Histogram<u64>),
    wheel_low: (Duration, Histogram<u64>),
    wheel_high: (Duration, Histogram<u64>),
}
impl BenchResult {
    pub fn new(
        total_queries: usize,
        total_entries: usize,
        duckdb_low: (Duration, Histogram<u64>),
        duckdb_high: (Duration, Histogram<u64>),
        wheel_low: (Duration, Histogram<u64>),
        wheel_high: (Duration, Histogram<u64>),
    ) -> Self {
        Self {
            total_queries,
            total_entries,
            duckdb_low,
            duckdb_high,
            wheel_low,
            wheel_high,
        }
    }
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
            "DuckDB Low",
            self.total_queries,
            self.duckdb_low.0.as_secs_f64(),
            &self.duckdb_low.1,
        );
        print_fn(
            "DuckDB High",
            self.total_queries,
            self.duckdb_high.0.as_secs_f64(),
            &self.duckdb_high.1,
        );
        print_fn(
            "Wheel Low",
            self.total_queries,
            self.wheel_low.0.as_secs_f64(),
            &self.wheel_low.1,
        );
        print_fn(
            "Wheel High",
            self.total_queries,
            self.wheel_high.0.as_secs_f64(),
            &self.wheel_high.1,
        );
    }
}

const EVENTS_PER_MINS: [usize; 4] = [10, 100, 1000, 10000];

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
    plot_top_n_throughput(&results);

    #[cfg(feature = "plot")]
    plot_top_n_latency(results);

    Ok(())
}

fn run(args: &Args) -> BenchResult {
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

    let duckdb_low = duckdb_run(
        "DuckDB TopN Low Intervals",
        watermark,
        &duckdb,
        topn_queries_low_interval.clone(),
    );
    let duckdb_high = duckdb_run(
        "DuckDB TopN High Intervals",
        watermark,
        &duckdb,
        topn_queries_high_interval.clone(),
    );

    let mut rw_wheel: RwWheel<TopNAggregator<u64, 10, U64SumAggregator>> = RwWheel::new(0);
    for batch in batches {
        for record in batch {
            rw_wheel
                .write()
                .insert(Entry::new(
                    (record.pu_location_id, record.fare_amount as u64),
                    record.do_time,
                ))
                .unwrap();
        }
        use awheel::time::NumericalDuration;
        rw_wheel.advance(60.seconds());
    }
    println!("Finished preparing RwWheel");
    let wheel_low = awheel_run(
        "RwWheel TopN Low Intervals",
        watermark,
        rw_wheel.read(),
        topn_queries_low_interval,
    );
    let wheel_high = awheel_run(
        "RwWheel TopN High Intervals",
        watermark,
        rw_wheel.read(),
        topn_queries_high_interval,
    );

    BenchResult::new(
        total_queries,
        total_entries,
        duckdb_low,
        duckdb_high,
        wheel_low,
        wheel_high,
    )
}

fn awheel_run(
    _id: &str,
    _watermark: u64,
    wheel: &ReadWheel<TopNAggregator<u64, 10, U64SumAggregator>>,
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>) {
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
    (runtime, hist)
}

fn duckdb_run(
    _id: &str,
    watermark: u64,
    db: &duckdb::Connection,
    queries: Vec<Query>,
) -> (Duration, Histogram<u64>) {
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
fn plot_top_n_throughput(results: &Vec<BenchResult>) {
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = results.iter().map(|e| e.total_entries as f64).collect();
    let mut duckdb_low_y = Vec::new();
    let mut duckdb_high_y = Vec::new();
    let mut wheel_low_y = Vec::new();
    let mut wheel_high_y = Vec::new();

    for result in results {
        let duckdb_throughput = result.total_queries as f64 / result.duckdb_low.0.as_secs_f64();
        duckdb_low_y.push(duckdb_throughput);
        let duckdb_throughput = result.total_queries as f64 / result.duckdb_high.0.as_secs_f64();
        duckdb_high_y.push(duckdb_throughput);
        let wheel_throughput = result.total_queries as f64 / result.wheel_low.0.as_secs_f64();
        wheel_low_y.push(wheel_throughput);
        let wheel_throughput = result.total_queries as f64 / result.wheel_high.0.as_secs_f64();
        wheel_high_y.push(wheel_throughput);
    }

    let mut duckdb_low_curve = Curve::new();
    duckdb_low_curve.set_label("DuckDB Low Intervals");
    duckdb_low_curve.set_line_color("r");
    duckdb_low_curve.set_marker_style("^");
    duckdb_low_curve.draw(&x, &duckdb_low_y);

    let mut duckdb_high_curve = Curve::new();
    duckdb_high_curve.set_label("DuckDB High Intervals");
    duckdb_high_curve.set_line_color("b");
    duckdb_high_curve.set_marker_style("o");
    duckdb_high_curve.draw(&x, &duckdb_high_y);

    let mut wheel_low_curve = Curve::new();
    wheel_low_curve.set_label("Wheel Low Intervals");
    wheel_low_curve.set_line_color("g");
    wheel_low_curve.set_marker_style("x");
    wheel_low_curve.draw(&x, &wheel_low_y);

    let mut wheel_high_curve = Curve::new();
    wheel_high_curve.set_label("Wheel High Intervals");
    wheel_high_curve.set_line_color("m");
    wheel_high_curve.set_marker_style("*");
    wheel_high_curve.draw(&x, &wheel_high_y);

    let mut legend = Legend::new();
    //legend.set_outside(true);
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

    plot.add(&duckdb_low_curve)
        .add(&duckdb_high_curve)
        .add(&wheel_low_curve)
        .add(&wheel_high_curve)
        .add(&legend);

    let path = Path::new("../results/top_n_throughput.png");
    plot.save(&path).unwrap();
}

#[cfg(feature = "plot")]
fn plot_top_n_latency(results: Vec<BenchResult>) {
    use plotpy::{Curve, Legend, Plot};
    use std::path::Path;
    std::fs::create_dir_all("../results").unwrap();

    let x: Vec<f64> = results.iter().map(|e| e.total_entries as f64).collect();
    let mut duckdb_low_y = Vec::new();
    let mut duckdb_high_y = Vec::new();
    let mut wheel_low_y = Vec::new();
    let mut wheel_high_y = Vec::new();

    let p99 = |hist: &Histogram<u64>| hist.value_at_quantile(0.99) as f64;

    for result in results {
        duckdb_low_y.push(p99(&result.duckdb_low.1));
        duckdb_high_y.push(p99(&result.duckdb_high.1));
        wheel_low_y.push(p99(&result.wheel_low.1));
        wheel_high_y.push(p99(&result.wheel_high.1));
    }

    let mut duckdb_low_curve = Curve::new();
    duckdb_low_curve.set_label("DuckDB Low Intervals");
    duckdb_low_curve.set_line_color("r");
    duckdb_low_curve.set_marker_style("^");
    duckdb_low_curve.draw(&x, &duckdb_low_y);

    let mut duckdb_high_curve = Curve::new();
    duckdb_high_curve.set_label("DuckDB High Intervals");
    duckdb_high_curve.set_line_color("b");
    duckdb_high_curve.set_marker_style("o");
    duckdb_high_curve.draw(&x, &duckdb_high_y);

    let mut wheel_low_curve = Curve::new();
    wheel_low_curve.set_label("Wheel Low Intervals");
    wheel_low_curve.set_line_color("g");
    wheel_low_curve.set_marker_style("x");
    wheel_low_curve.draw(&x, &wheel_low_y);

    let mut wheel_high_curve = Curve::new();
    wheel_high_curve.set_label("Wheel High Intervals");
    wheel_high_curve.set_line_color("m");
    wheel_high_curve.set_marker_style("*");
    wheel_high_curve.draw(&x, &wheel_high_y);

    let mut legend = Legend::new();
    //legend.set_outside(true);
    legend.draw();

    // configure plot
    let mut plot = Plot::new();
    plot.set_horizontal_gap(0.5)
        .set_vertical_gap(0.5)
        .set_gaps(0.3, 0.2);

    plot.set_label_y("p99 latency (nanoseconds)");
    plot.set_log_y(true);
    plot.set_log_x(true);
    plot.set_label_x("Total insert records");

    plot.add(&duckdb_low_curve)
        .add(&duckdb_high_curve)
        .add(&wheel_low_curve)
        .add(&wheel_high_curve)
        .add(&legend);

    let path = Path::new("../results/top_n_latency.png");
    plot.save(&path).unwrap();
}
