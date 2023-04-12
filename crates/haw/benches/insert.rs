use criterion::{
    criterion_group,
    criterion_main,
    BatchSize,
    Bencher,
    BenchmarkId,
    Criterion,
    Throughput,
};
use haw::{
    aggregator::{Aggregator, AllAggregator, U32SumAggregator},
    *,
};
use rand::prelude::*;

const NUM_ELEMENTS: usize = 100000;

pub fn insert_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wheel-throughput");
    group.throughput(Throughput::Elements(NUM_ELEMENTS as u64));
    group.bench_function("insert-no-wheel", insert_no_wheel);

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

fn insert_no_wheel(bencher: &mut Bencher) {
    bencher.iter_batched(
        || U32SumAggregator,
        |aggregator| {
            let mut current = aggregator.lift(0u32);
            for _ in 0..NUM_ELEMENTS {
                let entry = aggregator.lift(1u32);
                current = aggregator.combine(entry, current);
            }
            aggregator
        },
        BatchSize::PerIteration,
    );
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
                .into_iter()
                .cycle()
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
            let wheel = Wheel::<AllAggregator>::new(time);
            let timestamps = generate_out_of_order_timestamps(NUM_ELEMENTS, percentage);
            (wheel, timestamps)
        },
        |(mut wheel, timestamps)| {
            for timestamp in timestamps {
                wheel.insert(Entry::new(1.0, timestamp)).unwrap();
            }
            wheel
        },
        BatchSize::PerIteration,
    );
}

criterion_group!(benches, insert_benchmark);
criterion_main!(benches);
