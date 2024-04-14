use std::time::SystemTime;

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use uwheel::{
    aggregator::sum::U64SumAggregator,
    rw_wheel::read::{aggregation::conf::RetentionPolicy, hierarchical::WheelRange},
    *,
};

// 2023-11-09 00:00:00
const START_WATERMARK: u64 = 1699488000000;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("combine_range");
    group.bench_function("combine_range_u64_sum", combine_range::<U64SumAggregator>);
    group.bench_function(
        "combined_aggregation_plan",
        combined_aggregation_plan::<U64SumAggregator>,
    );
    group.finish();
}
fn combine_range<A: Aggregator<PartialAggregate = u64>>(bencher: &mut Bencher) {
    // 2023-11-09 00:00:00
    let haw: Haw<A> = prepare_haw(START_WATERMARK, 3600 * 14);
    let watermark = haw.watermark();
    bencher.iter(|| {
        let (start, end) = generate_seconds_range(START_WATERMARK, watermark);
        let range = WheelRange::from(into_offset_date_time_start_end(start, end));
        black_box(haw.combine_range(range))
    });

    #[cfg(feature = "profiler")]
    println!("{:?}", haw.stats());
}

fn combined_aggregation_plan<A: Aggregator<PartialAggregate = u64>>(bencher: &mut Bencher) {
    // 2023-11-09 00:00:00
    let haw: Haw<A> = prepare_haw(START_WATERMARK, 3600 * 14);
    let watermark = haw.watermark();
    bencher.iter(|| {
        let (start, end) = generate_seconds_range(START_WATERMARK, watermark);
        let range = WheelRange::from(into_offset_date_time_start_end(start, end));
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

fn prepare_haw<A: Aggregator<PartialAggregate = u64>>(
    start_watermark: u64,
    seconds: u64,
) -> Haw<A> {
    let conf = HawConf::default()
        .with_watermark(start_watermark)
        .with_retention_policy(RetentionPolicy::Keep);

    let mut haw: Haw<A> = Haw::new(conf);
    let deltas: Vec<Option<u64>> = (0..seconds).map(|_| Some(fastrand::u64(1..1000))).collect();
    haw.delta_advance(deltas);
    haw
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
