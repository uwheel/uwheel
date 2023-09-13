use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use wheeldb::{aggregator::sum::I32SumAggregator, WheelDB};

fn insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("wheeldb");
    group.throughput(Throughput::Elements(1));

    let mut db: WheelDB<I32SumAggregator> = WheelDB::open_default("bench");

    group.bench_function("insert 1 (memory)", |b| {
        b.iter(|| db.insert((10, 1000)));
    });
}

criterion_group!(benches, insert);
criterion_main!(benches);
