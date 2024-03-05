use awheel_core::{aggregator::sum::U64SumAggregator, *};
use criterion::{
    black_box,
    criterion_group,
    criterion_main,
    BatchSize,
    Bencher,
    BenchmarkId,
    Criterion,
};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix-sum");
    for partials in [100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("build-prefix-{}-sum-u64", partials)),
            partials,
            |b, &partials| {
                build_prefix_sum::<U64SumAggregator>(partials as u64, b);
            },
        );
        // TODO: add SIMD variant
    }

    group.finish();
}

fn build_prefix_sum<A: Aggregator<PartialAggregate = u64>>(partials: u64, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let partials: Vec<_> = (0..partials).map(|_| fastrand::u64(1..100000u64)).collect();
            partials
        },
        |partials: Vec<A::PartialAggregate>| black_box(A::build_prefix(&partials)),
        BatchSize::PerIteration,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
