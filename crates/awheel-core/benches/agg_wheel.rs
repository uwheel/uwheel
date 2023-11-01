use awheel_core::{
    aggregator::sum::U64SumAggregator,
    rw_wheel::read::{
        aggregation::{
            conf::{CompressionPolicy, RetentionPolicy, WheelConf},
            AggregationWheel,
            WheelSlot,
        },
        hierarchical::HOUR_TICK_MS,
    },
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

pub fn encode_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");
    for partials in [100, 1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("encode-no-compress-{}", partials)),
            partials,
            |b, &partials| {
                encode_wheel(partials as u64, CompressionPolicy::Never, b);
            },
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("encode-array-only-no-compress-{}", partials)),
            partials,
            |b, &partials| {
                encode_wheel_array(partials as u64, b);
            },
        );
        #[cfg(feature = "pco")]
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("encode-pco-{}", partials)),
            partials,
            |b, &partials| {
                encode_wheel(partials as u64, CompressionPolicy::Always, b);
            },
        );
    }
    group.finish();
}

fn encode_wheel(partials: u64, policy: CompressionPolicy, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let conf = WheelConf::new(HOUR_TICK_MS, 24)
                .with_retention_policy(RetentionPolicy::Keep)
                .with_compression_policy(policy);
            let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);

            for _i in 0..partials {
                wheel.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                wheel.tick();
            }
            wheel
        },
        |wheel| black_box(wheel.as_bytes()),
        BatchSize::PerIteration,
    );
}

fn encode_wheel_array(partials: u64, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let conf =
                WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
            let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);

            for _i in 0..partials {
                wheel.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                wheel.tick();
            }
            wheel
        },
        |wheel| black_box(wheel.slots().as_bytes().to_vec()),
        BatchSize::PerIteration,
    );
}

criterion_group!(benches, encode_bench);
criterion_main!(benches);
