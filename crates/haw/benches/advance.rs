use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion};
use haw::{aggregator::U32SumAggregator, *};

pub fn advance_time_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wheel-time-advance");
    for ticks in [1, 20, 50, 5000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(ticks), ticks, |b, &ticks| {
            advance_time(ticks as usize, b);
        });
    }
    group.finish();
}

fn advance_time(ticks: usize, bencher: &mut Bencher) {
    let mut time = 0;
    let ticks_as_ms = (ticks * 1000) as u64;
    let mut wheel = Wheel::<U32SumAggregator>::new(time);
    bencher.iter(|| {
        time += ticks_as_ms;
        wheel.advance_to(time);
    });
}

criterion_group!(benches, advance_time_benchmark);
criterion_main!(benches);
