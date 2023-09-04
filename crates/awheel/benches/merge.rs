use awheel_core::{aggregator::sum::U32SumAggregator, *};
use criterion::{criterion_group, criterion_main, Bencher, Criterion};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge-wheels");
    group.bench_function("merge-large-to-fresh-wheel", merge_large_to_fresh_wheel);
    group.bench_function("merge-small-to-fresh-wheel", merge_small_to_fresh_wheel);
    group.bench_function("merge-large-same-size", merge_same_size_large);
    group.bench_function("merge-small-same-size", merge_same_size_small);

    group.finish();
}
fn large_wheel() -> RwWheel<U32SumAggregator> {
    let mut time = 0;
    let mut wheel = RwWheel::new(time);
    for _ in 0..wheel.read().remaining_ticks() {
        wheel.advance_to(time);
        wheel.insert(Entry::new(1u32, time));
        time += 1000;
    }
    wheel
}

fn small_wheel() -> RwWheel<U32SumAggregator> {
    let mut time = 0;
    let mut wheel = RwWheel::new(time);
    for _ in 0..60 {
        wheel.advance_to(time);
        wheel.insert(Entry::new(1u32, time));
        time += 1000;
    }
    wheel
}

fn merge_large_to_fresh_wheel(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| {
        let fresh_wheel = RwWheel::new(0);
        fresh_wheel.read().merge(wheel.read());
        fresh_wheel
    });
}

fn merge_small_to_fresh_wheel(bencher: &mut Bencher) {
    let wheel = small_wheel();
    bencher.iter(|| {
        let fresh_wheel = RwWheel::new(0);
        fresh_wheel.merge_read_wheel(wheel.read());
        fresh_wheel
    });
}

fn merge_same_size_small(bencher: &mut Bencher) {
    let wheel = small_wheel();
    let other_wheel = small_wheel();
    bencher.iter(|| wheel.merge_read_wheel(other_wheel.read()));
}

fn merge_same_size_large(bencher: &mut Bencher) {
    let wheel = large_wheel();
    let other_wheel = large_wheel();
    bencher.iter(|| wheel.merge_read_wheel(other_wheel.read()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
