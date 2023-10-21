use awheel_core::{
    aggregator::{max::U64MaxAggregator, min::U64MinAggregator, sum::U64SumAggregator},
    *,
};
use criterion::{criterion_group, criterion_main, BatchSize, Bencher, BenchmarkId, Criterion};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("combine");
    for combines in [1, 32, 64, 512, 1024, 4096].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("combine-{}-sum-u64", combines)),
            combines,
            |b, &combines| {
                combine_partials::<U64SumAggregator>(combines as u64, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("combine-{}-min-u64", combines)),
            combines,
            |b, &combines| {
                combine_partials::<U64MinAggregator>(combines as u64, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("combine-{}-max-u64", combines)),
            combines,
            |b, &combines| {
                combine_partials::<U64MaxAggregator>(combines as u64, b);
            },
        );
    }

    group.finish();
}

fn combine_partials<A: Aggregator<PartialAggregate = u64>>(combines: u64, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let partials: Vec<_> = (0..combines).map(|_| fastrand::u64(1..100000u64)).collect();
            partials
        },
        |partials: Vec<A::PartialAggregate>| A::combine_slice(&partials),
        BatchSize::PerIteration,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
