use awheel_core::{
    aggregator::{max::U64MaxAggregator, min::U64MinAggregator, sum::U64SumAggregator, Aggregator},
    rw_wheel::read::{
        aggregation::{
            array::PartialArray,
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
    Throughput,
};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge");
    group.throughput(Throughput::Elements(1));

    for partials in [1, 32, 64, 512, 1024, 4096].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("typed-{}-partials-sum-u64", partials)),
            partials,
            |b, &partials| {
                merge_wheel::<U64SumAggregator>(partials as u64, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("raw-{}-partials-sum-u64", partials)),
            partials,
            |b, &partials| {
                raw_merge_wheel::<U64SumAggregator>(partials as u64, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("typed-{}-partials-min-u64", partials)),
            partials,
            |b, &partials| {
                merge_wheel::<U64MinAggregator>(partials as u64, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("raw-{}-partials-min-u64", partials)),
            partials,
            |b, &partials| {
                raw_merge_wheel::<U64MinAggregator>(partials as u64, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("typed-{}-partials--max-u64", partials)),
            partials,
            |b, &partials| {
                merge_wheel::<U64MaxAggregator>(partials as u64, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("raw-{}-partials--max-u64", partials)),
            partials,
            |b, &partials| {
                raw_merge_wheel::<U64MaxAggregator>(partials as u64, b);
            },
        );
    }

    group.finish();

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

fn merge_wheel<A: Aggregator<PartialAggregate = u64>>(partials: u64, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let conf =
                WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
            let mut w1 = AggregationWheel::<A>::new(conf);
            let mut w2 = AggregationWheel::<A>::new(conf);

            for _i in 0..partials {
                w1.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                w1.tick();

                w2.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                w2.tick();
            }
            (w1, w2)
        },
        |(mut w1, w2)| black_box(w1.merge(&w2)),
        BatchSize::PerIteration,
    );
}

fn raw_merge_wheel<A: Aggregator<PartialAggregate = u64>>(partials: u64, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let conf =
                WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
            let mut w1 = AggregationWheel::<A>::new(conf);
            let mut w2 = AggregationWheel::<A>::new(conf);

            for _i in 0..partials {
                w1.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                w1.tick();

                w2.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                w2.tick();
            }
            (w1, w2.slots().as_bytes().to_vec())
        },
        |(mut w1, raw_wheel)| black_box(w1.merge_from_array(PartialArray::from_bytes(&raw_wheel))),
        BatchSize::PerIteration,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
