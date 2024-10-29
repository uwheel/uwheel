use std::num::NonZeroUsize;

use criterion::{
    criterion_group,
    criterion_main,
    BatchSize,
    Bencher,
    BenchmarkId,
    Criterion,
    Throughput,
};
use rand::prelude::*;
use uwheel::{aggregator::sum::U64SumAggregator, *};

const NUM_ELEMENTS: usize = 10000;

pub fn insert_benchmark(c: &mut Criterion) {
    {
        let mut group = c.benchmark_group("latency");
        group.bench_function("insert-wheel-same-timestamp", insert_same_timestamp_wheel);
        group.bench_function("insert-wheel-to-timer", insert_to_timer);

        for seconds in [1u64, 10, 20, 30, 40, 50, 60].iter() {
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("insert-out-of-order-interval-{}", seconds)),
                seconds,
                |b, &seconds| {
                    insert_wheel_random(seconds, b);
                },
            );
        }
    }
    let mut group = c.benchmark_group("throughput");

    group.throughput(Throughput::Elements(NUM_ELEMENTS as u64));

    for out_of_order in [
        0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0,
    ]
    .iter()
    {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("insert-out-of-order_{}", out_of_order)),
            out_of_order,
            |b, &out_of_order| {
                insert_out_of_order(out_of_order as f32, b);
            },
        );
    }
    group.finish();
}

fn generate_out_of_order_timestamps(size: usize, percent: f32) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    let timestamps_per_second = 1000;
    let num_seconds = 60;
    let timestamps: Vec<u64> = (0..=num_seconds)
        .flat_map(|second| {
            let start_timestamp = second * 1000;
            let end_timestamp = start_timestamp + 999;
            (start_timestamp..=end_timestamp)
                .cycle()
                .map(align_to_closest_thousand)
                .take(timestamps_per_second)
        })
        .collect();

    let num_swaps = (timestamps.len() as f32 * percent / 100.0).round() as usize;

    let mut shuffled_timestamps = timestamps.clone();
    shuffled_timestamps.shuffle(&mut rng);

    for i in 0..num_swaps {
        let j = (i + 1..timestamps.len())
            .filter(|&x| shuffled_timestamps[x] > shuffled_timestamps[i])
            .max_by_key(|&x| shuffled_timestamps[x])
            .unwrap_or(i);
        shuffled_timestamps.swap(i, j);
    }

    shuffled_timestamps.truncate(size);
    shuffled_timestamps
}

fn insert_out_of_order(percentage: f32, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let time = 0;
            let wheel = RwWheel::<U64SumAggregator>::new(time);
            let timestamps = generate_out_of_order_timestamps(NUM_ELEMENTS, percentage);
            (wheel, timestamps)
        },
        |(mut wheel, timestamps)| {
            for timestamp in timestamps {
                wheel.insert(Entry::new(1, timestamp));
            }
            wheel
        },
        BatchSize::PerIteration,
    );
}

fn insert_wheel_random(seconds: u64, bencher: &mut Bencher) {
    let conf = uwheel::wheels::Conf::default().with_write_ahead(NonZeroUsize::new(16).unwrap());
    let mut wheel = RwWheel::<U64SumAggregator>::with_conf(conf);
    bencher.iter(|| {
        let ts = fastrand::u64(1..=seconds) * 1000;
        wheel.insert(Entry::new(1, ts));
    });
}

fn insert_to_timer(bencher: &mut Bencher) {
    let mut wheel = RwWheel::<U64SumAggregator>::new(0);
    bencher.iter(|| {
        let ts = fastrand::u64(65..=128) * 1000;
        wheel.insert(Entry::new(1, ts));
    });
}
fn insert_same_timestamp_wheel(bencher: &mut Bencher) {
    let mut wheel = RwWheel::<U64SumAggregator>::new(0);
    bencher.iter(|| {
        wheel.insert(Entry::new(1, 1000));
    });
}

fn align_to_closest_thousand(timestamp: u64) -> u64 {
    let remainder = timestamp % 1000;
    if remainder < 500 {
        timestamp - remainder
    } else {
        timestamp + (1000 - remainder)
    }
}
criterion_group!(benches, insert_benchmark);
criterion_main!(benches);
