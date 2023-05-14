use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use haw::{aggregator::AllAggregator, *};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wheel-serde");
    group.bench_function("ser-small-wheel-native", ser_small_wheel_native);
    group.bench_function("ser-small-wheel", ser_small_wheel);
    group.bench_function("ser-small-wheel-lz4", ser_small_wheel_lz4);
    group.bench_function("ser-large-wheel", ser_large_wheel);
    group.bench_function("ser-large-wheel-native", ser_large_wheel_native);
    group.bench_function("ser-large-wheel-lz4", ser_large_wheel_lz4);
    group.bench_function("deser-small-wheel", deser_small_wheel);
    group.bench_function("deser-small-wheel_lz4", deser_small_wheel_lz4);
    group.bench_function("deser-large-wheel", deser_large_wheel);
    group.bench_function("deser-large-wheel_lz4", deser_large_wheel_lz4);

    // serialise seconds wheel
    group.bench_function("ser-seconds-wheel", ser_seconds_wheel);

    // deserialise indivdual AggregationWheels
    group.bench_function("deser-seconds-wheel", deser_seconds_wheel);
    group.bench_function("deser-minutes-wheel", deser_minutes_wheel);
    group.bench_function("deser-hours-wheel", deser_hours_wheel);
    group.bench_function("deser-days-wheel", deser_days_wheel);

    group.finish();
}

fn large_wheel() -> Wheel<AllAggregator> {
    let mut time = 0;
    let mut wheel = Wheel::new(time);
    let ticks = 604800;
    for _ in 0..ticks {
        wheel.advance_to(time);
        wheel.insert(Entry::new(1.0, time)).unwrap();
        time += 1000;
    }
    wheel
}

fn small_wheel() -> Wheel<AllAggregator> {
    let mut time = 0;
    let mut wheel = Wheel::new(time);
    for _ in 0..60 {
        wheel.advance_to(time);
        wheel.insert(Entry::new(1.0, time)).unwrap();
        time += 1000;
    }
    wheel
}

fn ser_small_wheel(bencher: &mut Bencher) {
    ser_wheel(small_wheel(), bencher)
}
fn ser_small_wheel_native(bencher: &mut Bencher) {
    ser_wheel_native(small_wheel(), bencher)
}
fn ser_small_wheel_lz4(bencher: &mut Bencher) {
    ser_wheel_lz4(small_wheel(), bencher)
}
fn ser_large_wheel(bencher: &mut Bencher) {
    ser_wheel(large_wheel(), bencher)
}
fn ser_large_wheel_native(bencher: &mut Bencher) {
    ser_wheel_native(large_wheel(), bencher)
}
fn ser_large_wheel_lz4(bencher: &mut Bencher) {
    ser_wheel_lz4(large_wheel(), bencher)
}

#[inline]
fn ser_wheel(wheel: Wheel<AllAggregator>, bencher: &mut Bencher) {
    bencher.iter(|| black_box(wheel.as_bytes()));
}
#[inline]
fn ser_wheel_native(wheel: Wheel<AllAggregator>, bencher: &mut Bencher) {
    bencher.iter(|| black_box(wheel.to_be_bytes()));
}

#[inline]
fn ser_wheel_lz4(wheel: Wheel<AllAggregator>, bencher: &mut Bencher) {
    bencher.iter(|| black_box(lz4_flex::compress_prepend_size(&wheel.as_bytes())));
}

fn deser_small_wheel(bencher: &mut Bencher) {
    deser_wheel(small_wheel(), bencher)
}
fn deser_small_wheel_lz4(bencher: &mut Bencher) {
    deser_wheel_lz4(small_wheel(), bencher)
}

fn deser_large_wheel(bencher: &mut Bencher) {
    deser_wheel(large_wheel(), bencher)
}
fn deser_large_wheel_lz4(bencher: &mut Bencher) {
    deser_wheel_lz4(large_wheel(), bencher)
}

#[inline]
fn deser_wheel(wheel: Wheel<AllAggregator>, bencher: &mut Bencher) {
    let raw_wheel = wheel.as_bytes();
    bencher.iter(|| black_box(Wheel::<AllAggregator>::from_bytes(&raw_wheel)));
}

#[inline]
fn deser_wheel_lz4(wheel: Wheel<AllAggregator>, bencher: &mut Bencher) {
    let raw_wheel = wheel.as_bytes();
    let compressed_wheel = lz4_flex::compress_prepend_size(&raw_wheel);
    bencher.iter(|| {
        black_box(Wheel::<AllAggregator>::from_bytes(
            &lz4_flex::decompress_size_prepended(&compressed_wheel).unwrap(),
        ))
    });
}

#[inline]
fn ser_seconds_wheel(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| black_box(wheel.seconds_unchecked().as_bytes()));
}

#[inline]
fn deser_seconds_wheel(bencher: &mut Bencher) {
    let wheel = large_wheel();
    let raw_wheel = wheel.as_bytes();
    bencher.iter(|| black_box(Wheel::<AllAggregator>::seconds_wheel_from_bytes(&raw_wheel)));
}

#[inline]
fn deser_minutes_wheel(bencher: &mut Bencher) {
    let wheel = large_wheel();
    let raw_wheel = wheel.as_bytes();
    bencher.iter(|| black_box(Wheel::<AllAggregator>::minutes_wheel_from_bytes(&raw_wheel)));
}

#[inline]
fn deser_hours_wheel(bencher: &mut Bencher) {
    let wheel = large_wheel();
    let raw_wheel = wheel.as_bytes();
    bencher.iter(|| black_box(Wheel::<AllAggregator>::hours_wheel_from_bytes(&raw_wheel)));
}

#[inline]
fn deser_days_wheel(bencher: &mut Bencher) {
    let wheel = large_wheel();
    let raw_wheel = wheel.as_bytes();
    bencher.iter(|| black_box(Wheel::<AllAggregator>::days_wheel_from_bytes(&raw_wheel)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
