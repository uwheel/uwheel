use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion};
use haw::{aggregator::U64SumAggregator, *};

pub fn advance_time_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("advance");
    for ticks in [1, 4, 8, 16, 32, 64, 128, 256, 512, 1024].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}-seconds", ticks)),
            ticks,
            |b, &ticks| {
                advance_time(ticks as usize, b);
            },
        );
    }
    group.finish();
}

fn advance_time(ticks: usize, bencher: &mut Bencher) {
    let mut time = 0;
    let ticks_as_ms = (ticks * 1000) as u64;
    let mut wheel = RwWheel::<U64SumAggregator>::new(time);
    bencher.iter(|| {
        time += ticks_as_ms;
        wheel.advance_to(time);
    });
}

criterion_group!(benches, advance_time_benchmark);
criterion_main!(benches);
