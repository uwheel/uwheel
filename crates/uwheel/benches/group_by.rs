mod common;

use bitpacking::{BitPacker, BitPacker4x};
use common::SumAggregator;
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use uwheel::{wheels::read::aggregation::conf::RetentionPolicy, *};
use wheels::read::aggregation::conf::DataLayout;

// 2023-11-09 00:00:00
const START_WATERMARK: u64 = 1699488000000;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by");
    group.bench_function("random_range_u32_sum", group_by_random::<SumAggregator>);

    group.bench_function(
        "random_start_u32_sum",
        group_by_random_start::<SumAggregator>,
    );

    group.bench_function(
        "random_range_u32_sum_bitpacking",
        group_by_compressed_random::<SumAggregator>,
    );
    group.bench_function(
        "random_start_u32_sum_bitpacking",
        group_by_compressed_random_start::<SumAggregator>,
    );

    group.finish();
}

enum HawType {
    Compressed,
    Normal,
}

#[inline]
fn random_seconds_interval() -> uwheel::Duration {
    uwheel::Duration::seconds(fastrand::i64(1..1000))
}

fn group_by_random<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    group_by::<A>(HawType::Normal, bencher)
}
fn group_by_random_start<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    group_by_watermark::<A>(HawType::Normal, bencher)
}
fn group_by_compressed_random<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    group_by::<A>(HawType::Compressed, bencher)
}

fn group_by_compressed_random_start<A: Aggregator<PartialAggregate = u32>>(bencher: &mut Bencher) {
    group_by::<A>(HawType::Compressed, bencher)
}

fn group_by<A: Aggregator<PartialAggregate = u32>>(haw_type: HawType, bencher: &mut Bencher) {
    // 2023-11-09 00:00:00
    let haw: Haw<A> = prepare_haw(START_WATERMARK, 3600 * 14, haw_type);
    let watermark = haw.watermark();
    bencher.iter(|| {
        let (start, end) = common::generate_seconds_range(START_WATERMARK, watermark);
        let (start_date, end_date) = common::into_offset_date_time_start_end(start, end);
        let range = WheelRange::from(start_date, end_date);
        black_box(haw.group_by(range, random_seconds_interval()))
    });
}

fn group_by_watermark<A: Aggregator<PartialAggregate = u32>>(
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
        black_box(haw.group_by(range, random_seconds_interval()))
    });
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
        HawType::Normal => {}
    }

    let mut haw: Haw<A> = Haw::new(conf);
    let deltas: Vec<Option<u32>> = (0..seconds).map(|_| Some(fastrand::u32(1..1000))).collect();
    haw.delta_advance(deltas);

    haw.to_simd_wheels();

    haw
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
