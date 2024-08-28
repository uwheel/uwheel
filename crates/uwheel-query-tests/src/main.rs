#![allow(warnings)]

use std::process;

use datafusion::{
    arrow::datatypes::{Float64Type, Int64Type},
    prelude::{ParquetReadOptions, SessionContext},
};

use datafusion::arrow::array::{ArrowPrimitiveType, AsArray};

use chrono::{DateTime, NaiveDate};
use clap::Parser;
use datafusion::error::Result;
use pbr::ProgressBar;
use uwheel::{aggregator::sum::F64SumAggregator, wheels::read::ReaderWheel, WheelRange};
use uwheel_query_tests::{
    build_count_wheel,
    build_fare_wheel,
    generate_day_time_ranges,
    generate_hour_time_ranges,
    generate_minute_time_ranges,
    generate_second_time_ranges,
    SumAggregator,
    WheelType,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1000)]
    queries: usize,
}

fn main() {
    let args = Args::parse();
    println!("Running uwheel query integration tests...");

    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            validate_queries(args.queries).await.unwrap();
        })
    });

    match result {
        Ok(_) => {
            println!("All tests passed successfully!");
            process::exit(0);
        }
        Err(_) => {
            process::exit(1);
        }
    }
}

async fn validate_queries(queries: usize) -> Result<(), Box<dyn std::error::Error>> {
    // create local session context
    let ctx = SessionContext::new();

    let filename = "yellow_tripdata_2022-01.parquet";

    // register parquet file with the execution context
    ctx.register_parquet("yellow_tripdata", filename, ParquetReadOptions::default())
        .await?;

    let start_date = NaiveDate::from_ymd_opt(2022, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();

    let end_date = NaiveDate::from_ymd_opt(2022, 1, 31)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();

    println!("===== BUILDING WHEELS =====");
    let mut pb = ProgressBar::new(3);
    pb.format("╢▌▌░╟");
    let count_wheel = build_count_wheel(filename, WheelType::Normal);
    pb.inc();
    let prefix_count_wheel = build_count_wheel(filename, WheelType::Prefix);
    pb.inc();
    let compressed_count_wheel = build_count_wheel(filename, WheelType::Compressed);
    pb.inc();
    let fare_wheel = build_fare_wheel(filename);
    pb.inc();
    pb.finish_print("done");

    println!("===== VALIDATING SECOND RANGES =====");

    let second_ranges = generate_second_time_ranges(start_date, end_date, queries);
    validate(
        &ctx,
        count_wheel.read(),
        prefix_count_wheel.read(),
        compressed_count_wheel.read(),
        fare_wheel.read(),
        &second_ranges,
    )
    .await;

    println!("===== VALIDATING MINUTE RANGES =====");
    let min_ranges = generate_minute_time_ranges(start_date, end_date, queries);
    validate(
        &ctx,
        count_wheel.read(),
        prefix_count_wheel.read(),
        compressed_count_wheel.read(),
        fare_wheel.read(),
        &min_ranges,
    )
    .await;

    println!("===== VALIDATING HOUR RANGES =====");
    let hr_ranges = generate_hour_time_ranges(start_date, end_date, queries);

    validate(
        &ctx,
        count_wheel.read(),
        prefix_count_wheel.read(),
        compressed_count_wheel.read(),
        fare_wheel.read(),
        &hr_ranges,
    )
    .await;

    println!("===== VALIDATING DAY RANGES =====");
    let day_ranges = generate_day_time_ranges(start_date, end_date, queries);
    validate(
        &ctx,
        count_wheel.read(),
        prefix_count_wheel.read(),
        compressed_count_wheel.read(),
        fare_wheel.read(),
        &day_ranges,
    )
    .await;

    Ok(())
}

async fn validate(
    ctx: &SessionContext,
    count_wheel: &ReaderWheel<SumAggregator>,
    prefix_count_wheel: &ReaderWheel<SumAggregator>,
    compressed_count_wheel: &ReaderWheel<SumAggregator>,
    fare_wheel: &ReaderWheel<F64SumAggregator>,
    ranges: &[(u64, u64)],
) {
    validate_combine_range(
        ctx,
        count_wheel,
        prefix_count_wheel,
        compressed_count_wheel,
        fare_wheel,
        ranges,
    )
    .await;
    // TODO: add range, group_by queries
}

async fn validate_combine_range(
    ctx: &SessionContext,
    count_wheel: &ReaderWheel<SumAggregator>,
    prefix_count: &ReaderWheel<SumAggregator>,
    compressed_count: &ReaderWheel<SumAggregator>,
    fare_wheel: &ReaderWheel<F64SumAggregator>,
    ranges: &[(u64, u64)],
) {
    let mut pb = ProgressBar::new(ranges.len() as u64);
    pb.format("╢▌▌░╟");
    for (start, end) in ranges {
        // COUNT(*)
        let df_query = datafusion_count_query(*start, *end);
        let df_count: i64 = exec_single_value::<Int64Type>(ctx, &df_query).await;

        let wheel_range = WheelRange::new_unchecked(*start, *end);
        let uwheel_count = count_wheel
            .combine_range_and_lower(wheel_range)
            .unwrap_or(0) as i64;

        let prefix_uwheel_count = prefix_count
            .combine_range_and_lower(wheel_range)
            .unwrap_or(0) as i64;
        /*
                let compressed_uwheel_count = compressed_count
                    .combine_range_and_lower(wheel_range)
                    .unwrap_or(0) as i64;
        */

        pretty_assertions::assert_eq!(
            df_count,
            uwheel_count,
            "DataFusion query:\n{}",
            df_query.to_string().replace(", ", ",\n    ")
        );

        pretty_assertions::assert_eq!(
            uwheel_count,
            prefix_uwheel_count,
            "Prefix count does not match normal uwheel count"
        );

        /*
                pretty_assertions::assert_eq!(
                    prefix_uwheel_count,
                    compressed_uwheel_count,
                    "Prefix count does not match compressed uwheel count"
                );
        */

        /*
                // SUM(fare_amount)
                let df_query = datafusion_sum_query(*start, *end);
                let df_sum: f64 = exec_single_value::<Float64Type>(ctx, &df_query).await;
                let uwheel_sum = fare_wheel
                    .combine_range_and_lower(wheel_range)
                    .unwrap_or(0.0);

                pretty_assertions::assert_eq!(
                    df_sum,
                    uwheel_sum,
                    "DataFusion query:\n{}",
                    df_query.to_string().replace(", ", ",\n    ")
                );
        */

        pb.inc();
    }
    pb.finish_print("done");
}

fn datafusion_count_query(start: u64, end: u64) -> String {
    let start = DateTime::from_timestamp_millis(start as i64)
        .unwrap()
        .to_rfc3339()
        .to_string();
    let end = DateTime::from_timestamp_millis(end as i64)
        .unwrap()
        .to_rfc3339()
        .to_string();

    format!(
        "SELECT COUNT(*) FROM yellow_tripdata \
         WHERE tpep_dropoff_datetime >= '{}' \
         AND tpep_dropoff_datetime < '{}'",
        start, end
    )
}

fn datafusion_sum_query(start: u64, end: u64) -> String {
    let start = DateTime::from_timestamp_millis(start as i64)
        .unwrap()
        .to_rfc3339()
        .to_string();
    let end = DateTime::from_timestamp_millis(end as i64)
        .unwrap()
        .to_rfc3339()
        .to_string();

    format!(
        "SELECT SUM(fare_amount) FROM yellow_tripdata \
         WHERE tpep_dropoff_datetime >= '{}' \
         AND tpep_dropoff_datetime < '{}'",
        start, end
    )
}

async fn exec_single_value<T: ArrowPrimitiveType + std::fmt::Debug>(
    ctx: &SessionContext,
    query: &str,
) -> T::Native {
    let df = ctx.sql(query).await.unwrap();
    let res = df.collect().await.unwrap();
    let result: T::Native = res[0]
        .project(&[0])
        .unwrap()
        .column(0)
        .as_primitive::<T>()
        .value(0);

    result
}
