mod common;

use bitpacking::{BitPacker, BitPacker4x};
use common::{generate_seconds_range, into_offset_date_time_start_end, SumAggregator};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use uwheel::{wheels::read::aggregation::conf::RetentionPolicy, *};
use wheels::read::aggregation::conf::DataLayout;

// 2023-11-09 00:00:00
const START_WATERMARK: u64 = 1699488000000;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("combine_range");
    group.bench_function(
        "random_range_u32_sum",
        combine_range_random::<SumAggregator>,
    );
    group.bench_function(
        "random_start_u32_sum",
        combine_range_random_start::<SumAggregator>,
    );

    group.bench_function(
        "random_range_u32_sum_prefix",
        combine_range_prefix_random::<SumAggregator>,
    );

    group.bench_function(
        "random_start_u32_sum_prefix",
        combine_range_prefix_random_start::<SumAggregator>,
    );

    group.bench_function(
        "random_range_u32_sum_bitpacking",
        combine_range_compressed_random::<SumAggregator>,
    );

    group.bench_function(
        "random_start_u32_sum_bitpacking",
        combine_range_compressed_random_start::<SumAggregator>,
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
fn combine_range_random<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    combine_range::<A>(HawType::Normal, bencher)
}
fn combine_range_random_start<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    combine_range_watermark::<A>(HawType::Normal, bencher)
}
fn combine_range_prefix_random<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    combine_range::<A>(HawType::Prefix, bencher)
}
fn combine_range_prefix_random_start<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    combine_range_watermark::<A>(HawType::Prefix, bencher)
}
fn combine_range_compressed_random<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    combine_range::<A>(HawType::Compressed, bencher)
}

fn combine_range_compressed_random_start<A: Aggregator<PartialAggregate = u32>>(
    bencher: &mut Bencher,
) {
    combine_range_watermark::<A>(HawType::Compressed, bencher)
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

fn combine_range_watermark<A: Aggregator<PartialAggregate = u32>>(
    haw_type: HawType,
    bencher: &mut Bencher,
) {
    // 2023-11-09 00:00:00
    let haw: Haw<A> = prepare_haw(START_WATERMARK, 3600 * 14, haw_type);
    let watermark = haw.watermark();
    let watermark_offset_date =
        time::OffsetDateTime::from_unix_timestamp(watermark as i64 / 1000).unwrap();
    bencher.iter(|| {
        let (start, end) = common::generate_seconds_range(START_WATERMARK, watermark);
        let (start_date, _) = common::into_offset_date_time_start_end(start, end);
        let range = WheelRange::from(start_date, watermark_offset_date);
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

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
