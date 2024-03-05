use awheel_core::{
    aggregator::{sum::U64SumAggregator, Aggregator},
    rw_wheel::read::{
        aggregation::{
            conf::{RetentionPolicy, WheelConf},
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

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge-many");
    let slots = 10000;
    for wheels in [1, 32, 64, 512, 1024].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}-wheels-sum-u64", wheels)),
            wheels,
            |b, &wheels| {
                merge_many::<U64SumAggregator>(slots, wheels as u64, b);
            },
        );
        #[cfg(feature = "rayon")]
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}-wheels-par-sum-u64", wheels)),
            wheels,
            |b, &wheels| {
                merge_many_par::<U64SumAggregator>(slots, wheels as u64, b);
            },
        );

        // group.bench_with_input(
        //     BenchmarkId::from_parameter(format!("{}-partials-min-u64", wheels)),
        //     wheels,
        //     |b, &wheels| {
        //         merge_many::<U64MinAggregator>(60, wheels as u64, b);
        //     },
        // );

        // group.bench_with_input(
        //     BenchmarkId::from_parameter(format!("{}-partials--max-u64", wheels)),
        //     wheels,
        //     |b, &wheels| {
        //         merge_many::<U64MaxAggregator>(60, wheels as u64, b);
        //     },
        // );
    }

    group.finish();
}

fn merge_many<A: Aggregator<PartialAggregate = u64>>(
    partials: u64,
    total_wheels: u64,
    bencher: &mut Bencher,
) {
    bencher.iter_batched(
        || {
            let conf =
                WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
            let mut wheels = Vec::with_capacity(total_wheels as usize);
            for _i in 0..total_wheels {
                let mut w = AggregationWheel::<A>::new(conf);
                for _i in 0..partials {
                    w.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                    w.tick();
                }
                wheels.push(w);
            }
            wheels
        },
        |wheels| black_box(AggregationWheel::merge_many(wheels)),
        BatchSize::PerIteration,
    );
}

#[cfg(feature = "rayon")]
fn merge_many_par<A: Aggregator<PartialAggregate = u64>>(
    partials: u64,
    total_wheels: u64,
    bencher: &mut Bencher,
) {
    bencher.iter_batched(
        || {
            let conf =
                WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
            let mut wheels = Vec::with_capacity(total_wheels as usize);
            for _i in 0..total_wheels {
                let mut w = AggregationWheel::<A>::new(conf);
                for _i in 0..partials {
                    w.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                    w.tick();
                }
                wheels.push(w);
            }
            wheels
        },
        |wheels| black_box(AggregationWheel::merge_parallel(wheels)),
        BatchSize::PerIteration,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
