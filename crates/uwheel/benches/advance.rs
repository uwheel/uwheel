use criterion::{BatchSize, Bencher, BenchmarkId, Criterion, criterion_group, criterion_main};
use uwheel::{aggregator::sum::U64SumAggregator, wheels::read::ReaderWheel, *};

pub fn advance_time_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("advance");
    for ticks in [1, 4, 8, 16, 32, 64, 128, 256, 512, 1024].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("advance-{}-seconds", ticks)),
            ticks,
            |b, &ticks| {
                advance_time(ticks as usize, b);
            },
        );
    }
    for deltas in [1, 100, 1000, 10000, 100000, 1000000, 10000000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("delta-advance-{}", deltas)),
            deltas,
            |b, &deltas| {
                delta_advance(deltas as u64, b);
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
        wheel.advance_to(time)
    });
}

fn delta_advance(deltas: u64, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let time = 0;
            let wheel: ReaderWheel<U64SumAggregator> = ReaderWheel::new(time);
            let deltas: Vec<_> = (0..deltas)
                .map(|_| Some(fastrand::u64(1..100000u64)))
                .collect();
            (wheel, deltas)
        },
        |(wheel, deltas)| {
            wheel.delta_advance(deltas);
            wheel
        },
        BatchSize::PerIteration,
    );
}

criterion_group!(benches, advance_time_benchmark);
criterion_main!(benches);
