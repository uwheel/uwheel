use std::time::SystemTime;

use aggregator::Compression;
use bitpacking::{BitPacker, BitPacker4x};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use uwheel::{wheels::read::aggregation::conf::RetentionPolicy, *};
use wheels::read::aggregation::conf::DataLayout;

// 2023-11-09 00:00:00
const START_WATERMARK: u64 = 1699488000000;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("combine_range");
    group.bench_function(
        "combine_range_u32_sum",
        combine_range_normal::<SumAggregator>,
    );

    group.bench_function(
        "combine_range_u32_sum_prefix",
        combine_range_prefix::<SumAggregator>,
    );

    group.bench_function(
        "combine_range_u32_sum_bitpacking",
        combine_range_compressed::<SumAggregator>,
    );

    group.bench_function(
        "combined_aggregation_plan",
        combined_aggregation_plan::<SumAggregator>,
    );
    group.finish();
}

enum HawType {
    Compressed,
    Prefix,
    Normal,
}
fn combine_range_normal<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    combine_range::<A>(HawType::Normal, bencher)
}
fn combine_range_prefix<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    combine_range::<A>(HawType::Prefix, bencher)
}
fn combine_range_compressed<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    combine_range::<A>(HawType::Compressed, bencher)
}

fn combine_range<A: Aggregator<PartialAggregate = u32>>(haw_type: HawType, bencher: &mut Bencher) {
    // 2023-11-09 00:00:00
    let haw: Haw<A> = prepare_haw(START_WATERMARK, 3600 * 14, haw_type);
    let watermark = haw.watermark();
    bencher.iter(|| {
        let (start, end) = generate_seconds_range(START_WATERMARK, watermark);
        let (start_date, end_date) = into_offset_date_time_start_end(start, end);
        let range = WheelRange::from(start_date, end_date);
        black_box(haw.combine_range(range))
    });

    #[cfg(feature = "profiler")]
    println!("{:?}", haw.stats());
}

fn combined_aggregation_plan<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    // 2023-11-09 00:00:00
    let haw: Haw<A> = prepare_haw(START_WATERMARK, 3600 * 14, HawType::Normal);
    let watermark = haw.watermark();
    bencher.iter(|| {
        let (start, end) = generate_seconds_range(START_WATERMARK, watermark);
        let (start_date, end_date) = into_offset_date_time_start_end(start, end);
        let range = WheelRange::from(start_date, end_date);
        let ranges = Haw::<A>::split_wheel_ranges(range);
        haw.combined_aggregation_plan(ranges)
    });

    #[cfg(feature = "profiler")]
    println!("{:?}", haw.stats());
}

pub fn into_offset_date_time_start_end(start: u64, end: u64) -> (OffsetDateTime, OffsetDateTime) {
    (
        OffsetDateTime::from_unix_timestamp(start as i64).unwrap(),
        OffsetDateTime::from_unix_timestamp(end as i64).unwrap(),
    )
}

pub fn generate_seconds_range(start_watermark: u64, watermark: u64) -> (u64, u64) {
    // Specify the date range (2023-10-01 to watermark)
    let start_date =
        SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(start_watermark / 1000);
    let end_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(watermark / 1000);

    // Convert dates to Unix timestamps
    let start_timestamp = start_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let end_timestamp = end_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Randomly generate a start time within the specified date range
    let random_start = fastrand::u64(start_timestamp..end_timestamp);

    // Generate a random duration between 1 and (watermark - random_start_seconds) seconds
    let max_duration = end_timestamp - random_start;
    let duration_seconds = fastrand::u64(1..=max_duration);

    (random_start, random_start + duration_seconds)
}

fn prepare_haw<A: Aggregator<PartialAggregate = u32>>(
    start_watermark: u64,
    seconds: u64,
    haw_type: HawType,
) -> Haw<A> {
    let mut conf = HawConf::default()
        .with_watermark(start_watermark)
        .with_retention_policy(RetentionPolicy::Keep);
    match haw_type {
        HawType::Compressed => {
            conf.seconds
                .set_data_layout(DataLayout::Compressed(BitPacker4x::BLOCK_LEN));
            conf.minutes
                .set_data_layout(DataLayout::Compressed(BitPacker4x::BLOCK_LEN));
            conf.hours
                .set_data_layout(DataLayout::Compressed(BitPacker4x::BLOCK_LEN));
        }
        HawType::Prefix => {}
        HawType::Normal => {}
    }

    let mut haw: Haw<A> = Haw::new(conf);
    let deltas: Vec<Option<u32>> = (0..seconds).map(|_| Some(fastrand::u32(1..1000))).collect();
    haw.delta_advance(deltas);

    haw.to_simd_wheels();

    if let HawType::Prefix = haw_type {
        haw.to_prefix_wheels();
    }

    haw
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

    fn combine_inverse() -> Option<aggregator::InverseFn<Self::PartialAggregate>> {
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

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
