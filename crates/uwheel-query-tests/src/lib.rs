#![allow(warnings)]

use aggregator::{Compression, InverseFn};
use std::fs::File;
use uwheel::{
    aggregator::{self, sum::F64SumAggregator},
    Aggregator,
};

use bitpacking::{BitPacker, BitPacker4x};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use datafusion::{
    arrow::array::{Float64Array, TimestampMicrosecondArray},
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
};
use uwheel::{
    wheels::read::aggregation::conf::{DataLayout, WheelMode},
    Conf,
    Entry,
    HawConf,
    NumericalDuration,
    RwWheel,
};

pub enum WheelType {
    Normal,
    Prefix,
    Compressed,
}

// Builds a COUNT(*) wheel
pub fn build_count_wheel(path: &str, wheel_type: WheelType) -> RwWheel<SumAggregator> {
    let file = File::open(path).unwrap();

    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());
    let start_ms = date.timestamp_millis() as u64;

    let mut conf = HawConf::default()
        .with_watermark(start_ms)
        .with_mode(WheelMode::Index);

    match wheel_type {
        WheelType::Compressed => {
            conf.seconds
                .set_data_layout(DataLayout::Compressed(BitPacker4x::BLOCK_LEN));
            conf.minutes
                .set_data_layout(DataLayout::Compressed(BitPacker4x::BLOCK_LEN));
            conf.hours
                .set_data_layout(DataLayout::Compressed(BitPacker4x::BLOCK_LEN));
        }
        WheelType::Prefix => {}
        WheelType::Normal => {}
    }

    conf.seconds
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.minutes
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.hours
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.days
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    let mut wheel: RwWheel<SumAggregator> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap();

    for batch in parquet_reader {
        let b = batch.unwrap();
        let dropoff_array = b
            .column_by_name("tpep_dropoff_datetime")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        for date in dropoff_array.values().iter() {
            let timestamp_ms = DateTime::from_timestamp_micros(*date)
                .unwrap()
                .timestamp_millis() as u64;
            let entry = Entry::new(1, timestamp_ms);
            wheel.insert(entry);
        }
    }
    wheel.advance(31.days());
    // If SIMD is enabled then we make sure the wheels are SIMD compatible after building the index
    wheel.read().to_simd_wheels();

    if let WheelType::Prefix = wheel_type {
        wheel.read().to_prefix_wheels();
    }

    wheel
}

pub fn build_fare_wheel(path: &str) -> RwWheel<F64SumAggregator> {
    let file = File::open(path).unwrap();

    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());
    let start_ms = date.timestamp_millis() as u64;

    let mut conf = HawConf::default()
        .with_watermark(start_ms)
        .with_mode(WheelMode::Index);

    conf.seconds
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.minutes
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.hours
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.days
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    let mut wheel: RwWheel<F64SumAggregator> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap();

    for batch in parquet_reader {
        let b = batch.unwrap();
        let dropoff_array = b
            .column_by_name("tpep_dropoff_datetime")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        let fare_array = b
            .column_by_name("fare_amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for (date, fare) in dropoff_array
            .values()
            .iter()
            .zip(fare_array.values().iter())
        {
            let timestamp_ms = DateTime::from_timestamp_micros(*date)
                .unwrap()
                .timestamp_millis() as u64;
            let entry = Entry::new(*fare, timestamp_ms);
            wheel.insert(entry);
        }
    }
    wheel.advance(31.days());
    // If SIMD is enabled then we make sure the wheels are SIMD compatible after building the index
    wheel.read().to_simd_wheels();

    wheel
}

pub fn generate_second_time_ranges(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    count: usize,
) -> Vec<(u64, u64)> {
    // Calculate total seconds within the date range
    let total_seconds = (end - start).num_seconds() as u64;
    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        // Randomly select start and end seconds
        let start_second = fastrand::u64(0..total_seconds - 1); // exclude last second
        let end_second = fastrand::u64(start_second + 1..total_seconds);
        // Construct DateTime objects with second alignment
        let start_time = start + chrono::Duration::seconds(start_second as i64);
        let end_time = start + chrono::Duration::seconds(end_second as i64);
        ranges.push((
            start_time.timestamp_millis() as u64,
            end_time.timestamp_millis() as u64,
        ));
    }
    ranges
}

pub fn generate_minute_time_ranges(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    count: usize,
) -> Vec<(u64, u64)> {
    // Calculate total minutes within the date range
    let total_minutes = (end - start).num_minutes() as u64;

    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        // Randomly select start and end minutes
        let start_minute = fastrand::u64(0..total_minutes - 1); // exclude last min
        let end_minute = fastrand::u64(start_minute + 1..total_minutes);

        // Construct DateTime objects with minute alignment
        let start_time = start + chrono::Duration::minutes(start_minute as i64);
        let end_time = start + chrono::Duration::minutes(end_minute as i64);

        ranges.push((
            start_time.timestamp_millis() as u64,
            end_time.timestamp_millis() as u64,
        ));
    }
    ranges
}

pub fn generate_hour_time_ranges(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    count: usize,
) -> Vec<(u64, u64)> {
    // Calculate total hours within the date range
    let total_hours = (end - start).num_hours() as u64;

    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        // Randomly select start and end hours
        let start_hour = fastrand::u64(0..total_hours - 1); // exclude last hour
        let end_hour = fastrand::u64(start_hour + 1..total_hours);

        // Construct DateTime objects with minute alignment
        let start_time = start + chrono::Duration::minutes(start_hour as i64);
        let end_time = start + chrono::Duration::minutes(end_hour as i64);

        ranges.push((
            start_time.timestamp_millis() as u64,
            end_time.timestamp_millis() as u64,
        ));
    }
    ranges
}
pub fn generate_day_time_ranges(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    count: usize,
) -> Vec<(u64, u64)> {
    let total_days = (end - start).num_days() as u64;

    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        let start_day = fastrand::u64(0..total_days - 1);
        let end_day = fastrand::u64(start_day + 1..total_days);

        let start_time = start + chrono::Duration::days(start_day as i64);
        let end_time = start + chrono::Duration::days(end_day as i64);

        ranges.push((
            start_time.timestamp_millis() as u64,
            end_time.timestamp_millis() as u64,
        ));
    }
    ranges
}

#[derive(Clone, Debug, Default)]
pub struct SumAggregator;

impl Aggregator for SumAggregator {
    const IDENTITY: Self::PartialAggregate = 0;

    type Input = u32;
    type PartialAggregate = u32;
    type MutablePartialAggregate = u32;
    type Aggregate = u32;

    fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
        input
    }
    fn combine_mutable(mutable: &mut Self::MutablePartialAggregate, input: Self::Input) {
        *mutable += input
    }
    fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
        a
    }

    fn combine(a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a + b
    }
    fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
        a
    }

    fn combine_inverse() -> Option<InverseFn<Self::PartialAggregate>> {
        Some(|a, b| a - b)
    }

    fn compression() -> Option<Compression<Self::PartialAggregate>> {
        let compressor = |slice: &[u32]| {
            let bitpacker = BitPacker4x::new();
            let num_bits = bitpacker.num_bits(slice);
            let mut compressed = vec![0u8; BitPacker4x::BLOCK_LEN * 4];
            let compressed_len = bitpacker.compress(slice, &mut compressed[..], num_bits);

            // 1 bit for metadata + compressed data
            let mut result = Vec::with_capacity(1 + compressed_len);
            // Prepend metadata
            result.push(num_bits);
            // Append compressed data
            result.extend_from_slice(&compressed[..compressed_len]);

            result
        };
        let decompressor = |bytes: &[u8]| {
            let bit_packer = BitPacker4x::new();
            // Extract num bits metadata
            let bits = bytes[0];
            // Decompress data
            let mut decompressed = vec![0u32; BitPacker4x::BLOCK_LEN];
            bit_packer.decompress(&bytes[1..], &mut decompressed, bits);

            decompressed
        };

        Some(Compression::new(compressor, decompressor))
    }
}
