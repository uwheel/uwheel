use awheel_core::{
    aggregator::{
        sum::{U32SumAggregator, U64SumAggregator},
        Aggregator,
    },
    rw_wheel::read::{
        aggregation::{
            array::{MutablePartialArray, PartialArray},
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

const TOTAL_WHEELS: usize = 10000;
const SLOT_VALUE: usize = 10;

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

    let mut group = c.benchmark_group("merge");
    group.throughput(Throughput::Elements(TOTAL_WHEELS as u64));

    for partials in [1, 10, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("alloc-{}-partials-sum-u32", partials)),
            partials,
            |b, &partials| {
                alloc_merge_wheel::<U32SumAggregator>(partials as u64, SLOT_VALUE as u32, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("alloc-{}-partials-sum-u64", partials)),
            partials,
            |b, &partials| {
                alloc_merge_wheel::<U64SumAggregator>(partials as u64, SLOT_VALUE as u64, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("zero-copy-{}-partials-sum-u32", partials)),
            partials,
            |b, &partials| {
                zero_copy_merge_wheel::<U32SumAggregator>(partials as u64, SLOT_VALUE as u32, b);
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("zero-copy-{}-partials-sum-u64", partials)),
            partials,
            |b, &partials| {
                zero_copy_merge_wheel::<U64SumAggregator>(partials as u64, SLOT_VALUE as u64, b);
            },
        );

        // group.bench_with_input(
        //     BenchmarkId::from_parameter(format!("typed-{}-partials-min-u64", partials)),
        //     partials,
        //     |b, &partials| {
        //         merge_wheel::<U64MinAggregator>(partials as u64, b);
        //     },
        // );

        // group.bench_with_input(
        //     BenchmarkId::from_parameter(format!("raw-{}-partials-min-u64", partials)),
        //     partials,
        //     |b, &partials| {
        //         raw_merge_wheel::<U64MinAggregator>(partials as u64, b);
        //     },
        // );

        // group.bench_with_input(
        //     BenchmarkId::from_parameter(format!("typed-{}-partials--max-u64", partials)),
        //     partials,
        //     |b, &partials| {
        //         merge_wheel::<U64MaxAggregator>(partials as u64, b);
        //     },
        // );

        // group.bench_with_input(
        //     BenchmarkId::from_parameter(format!("raw-{}-partials--max-u64", partials)),
        //     partials,
        //     |b, &partials| {
        //         raw_merge_wheel::<U64MaxAggregator>(partials as u64, b);
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

fn zero_copy_merge_wheel<A: Aggregator>(
    partials: u64,
    value: A::PartialAggregate,
    bencher: &mut Bencher,
) {
    bencher.iter_batched(
        || {
            let conf =
                WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
            let mut w1 = AggregationWheel::<A>::new(conf);

            for _i in 0..partials {
                // w1.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                w1.insert_slot(WheelSlot::new(Some(value), None));
                w1.tick();
            }

            let mut wheels = Vec::with_capacity(TOTAL_WHEELS);
            for _x in 0..TOTAL_WHEELS {
                let mut w2 = AggregationWheel::<A>::new(conf);

                // w2.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                w2.insert_slot(WheelSlot::new(Some(value), None));
                w2.tick();
                wheels.push(w2.slots().as_bytes().to_vec());
            }

            (w1, wheels)
        },
        |(mut w1, wheels)| {
            for wheel in wheels {
                w1.merge_from_ref(PartialArray::<'_, A>::from_bytes(&wheel));
            }
            w1
        },
        BatchSize::PerIteration,
    );
}

fn alloc_merge_wheel<A: Aggregator>(
    partials: u64,
    value: A::PartialAggregate,
    bencher: &mut Bencher,
) {
    bencher.iter_batched(
        || {
            let conf =
                WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
            let mut w1 = AggregationWheel::<A>::new(conf);

            for _i in 0..partials {
                // w1.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                w1.insert_slot(WheelSlot::new(Some(value), None));
                w1.tick();
            }

            let mut wheels = Vec::with_capacity(TOTAL_WHEELS);
            for _x in 0..TOTAL_WHEELS {
                let mut w2 = AggregationWheel::<A>::new(conf);

                // w2.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
                w2.insert_slot(WheelSlot::new(Some(value), None));
                w2.tick();
                wheels.push(w2.slots().as_bytes().to_vec());
            }

            (w1, wheels)
        },
        |(mut w1, wheels)| {
            for wheel in wheels {
                w1.merge_from_ref(MutablePartialArray::<A>::from_bytes(&wheel));
            }
            w1
        },
        BatchSize::PerIteration,
    );
}

// fn raw_merge_wheel<A: Aggregator<PartialAggregate = u64>>(partials: u64, bencher: &mut Bencher) {
//     bencher.iter_batched(
//         || {
//             let conf =
//                 WheelConf::new(HOUR_TICK_MS, 24).with_retention_policy(RetentionPolicy::Keep);
//             let mut w1 = AggregationWheel::<A>::new(conf);
//             let mut w2 = AggregationWheel::<A>::new(conf);

//             for _i in 0..partials {
//                 w1.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
//                 w1.tick();

//                 w2.insert_slot(WheelSlot::new(Some(fastrand::u64(100..1000)), None));
//                 w2.tick();
//             }
//             (w1, w2.slots().as_bytes().to_vec())
//         },
//         |(mut w1, raw_wheel)| black_box(w1.merge_from_ref(PartialArray::from_bytes(&raw_wheel))),
//         BatchSize::PerIteration,
//     );
// }

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
