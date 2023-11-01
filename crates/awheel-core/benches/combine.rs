use awheel_core::{
    aggregator::{max::U64MaxAggregator, min::U64MinAggregator, sum::U64SumAggregator},
    rw_wheel::read::aggregation::PartialArray,
    *,
};
use criterion::{
    black_box,
    criterion_group,
    criterion_main,
    BatchSize,
    Bencher,
    BenchmarkId,
    Criterion,
};
use zerocopy::{self, AsBytes, Ref};

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
            BenchmarkId::from_parameter(format!("combine-raw-{}-sum-u64", combines)),
            combines,
            |b, &combines| {
                combine_raw_partials::<U64SumAggregator>(combines as u64, b);
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
            BenchmarkId::from_parameter(format!("combine-raw-{}-min-u64", combines)),
            combines,
            |b, &combines| {
                combine_raw_partials::<U64MinAggregator>(combines as u64, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("combine-{}-max-u64", combines)),
            combines,
            |b, &combines| {
                combine_partials::<U64MaxAggregator>(combines as u64, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("combine-raw-{}-max-u64", combines)),
            combines,
            |b, &combines| {
                combine_raw_partials::<U64MaxAggregator>(combines as u64, b);
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
        |partials: Vec<A::PartialAggregate>| black_box(A::combine_slice(&partials)),
        BatchSize::PerIteration,
    );
}

fn combine_raw_partials<A: Aggregator<PartialAggregate = u64>>(
    combines: u64,
    bencher: &mut Bencher,
) {
    bencher.iter_batched(
        || {
            let partials: Vec<_> = (0..combines).map(|_| fastrand::u64(1..100000u64)).collect();
            partials.as_bytes().to_vec()
        },
        |bytes: Vec<u8>| {
            let partial_arr: PartialArray<'_, A> = PartialArray::from_bytes(&bytes);
            black_box(partial_arr.combine_range(..))
        },
        BatchSize::PerIteration,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
