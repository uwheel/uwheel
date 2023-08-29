use awheel::{aggregator::sum::U64SumAggregator, Entry, RwWheel};
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

const NUM_ELEMENTS: usize = 10000;

#[inline]
fn random_timestamp(seconds: u64) -> u64 {
    fastrand::u64(1..=seconds * 1000)
}
#[inline]
fn random_timestamp_aligned(seconds: u64) -> u64 {
    let ts = fastrand::u64(1..=seconds * 1000);
    align_to_closest_thousand(ts)
}

pub fn insert_benchmark(c: &mut Criterion) {
    {
        let mut group = c.benchmark_group("latency");
        group.bench_function("insert-fiba-same-timestamp", insert_same_timestamp_fiba);
        group.bench_function("insert-wheel-same-timestamp", insert_same_timestamp_wheel);

        for seconds in [1u64, 10, 20, 30, 40, 50, 60].iter() {
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("insert-out-of-order-interval-{}", seconds)),
                seconds,
                |b, &seconds| {
                    insert_wheel_random(seconds, b);
                },
            );
            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "insert-fiba-bfinger2-out-of-order-interval-{}",
                    seconds
                )),
                seconds,
                |b, &seconds| {
                    insert_fiba_bfinger2_random(seconds, b);
                },
            );
            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "insert-fiba-cg-bfinger2-out-of-order-interval-{}",
                    seconds
                )),
                seconds,
                |b, &seconds| {
                    insert_fiba_cg_bfinger2_random(seconds, b);
                },
            );
            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "insert-fiba-bfinger4-out-of-order-interval-{}",
                    seconds
                )),
                seconds,
                |b, &seconds| {
                    insert_fiba_bfinger4_random(seconds, b);
                },
            );
            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "insert-fiba-cg-bfinger4-out-of-order-interval-{}",
                    seconds
                )),
                seconds,
                |b, &seconds| {
                    insert_fiba_cg_bfinger4_random(seconds, b);
                },
            );
            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "insert-fiba-bfinger8-out-of-order-interval-{}",
                    seconds
                )),
                seconds,
                |b, &seconds| {
                    insert_fiba_bfinger8_random(seconds, b);
                },
            );
            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "insert-fiba-cg-bfinger8-out-of-order-interval-{}",
                    seconds
                )),
                seconds,
                |b, &seconds| {
                    insert_fiba_cg_bfinger8_random(seconds, b);
                },
            );
        }
    }
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(NUM_ELEMENTS as u64));

    for seconds in [1u64, 10, 20, 30, 40, 50, 60].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("insert-out-of-order-wheel-{}", seconds)),
            seconds,
            |b, &seconds| {
                insert_batch_wheel(seconds, b);
            },
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("insert-random-cg-bfinger2-{}", seconds)),
            seconds,
            |b, &seconds| {
                insert_batch_random_fiba_cg_bfinger2(seconds, b);
            },
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("insert-random-cg-bfinger4-{}", seconds)),
            seconds,
            |b, &seconds| {
                insert_batch_random_fiba_cg_bfinger4(seconds, b);
            },
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("insert-random-cg-bfinger8-{}", seconds)),
            seconds,
            |b, &seconds| {
                insert_batch_random_fiba_cg_bfinger8(seconds, b);
            },
        );
    }

    group.finish();
}

fn _generate_out_of_order_timestamps(size: usize, percent: f32) -> Vec<u64> {
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
fn insert_batch_wheel(seconds: u64, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let time = 0;
            RwWheel::<U64SumAggregator>::new(time)
        },
        |mut wheel| {
            for _i in 0..NUM_ELEMENTS {
                let timestamp = random_timestamp(seconds);
                wheel.insert(Entry::new(1, timestamp));
            }
            wheel
        },
        BatchSize::PerIteration,
    );
}

fn _insert_out_of_order(percentage: f32, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let time = 0;
            let wheel = RwWheel::<U64SumAggregator>::new(time);
            let timestamps = _generate_out_of_order_timestamps(NUM_ELEMENTS, percentage);
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

fn insert_batch_random_fiba_cg_bfinger2(seconds: u64, bencher: &mut Bencher) {
    bencher.iter_batched(
        window::bfinger_two::create_fiba_with_sum,
        |mut fiba| {
            for _i in 0..NUM_ELEMENTS {
                let timestamp = random_timestamp_aligned(seconds);
                fiba.pin_mut().insert(&timestamp, &1u64);
            }
            fiba
        },
        BatchSize::PerIteration,
    );
}

fn insert_batch_random_fiba_cg_bfinger4(seconds: u64, bencher: &mut Bencher) {
    bencher.iter_batched(
        window::bfinger_four::create_fiba_4_with_sum,
        |mut fiba| {
            for _i in 0..NUM_ELEMENTS {
                let timestamp = random_timestamp_aligned(seconds);
                fiba.pin_mut().insert(&timestamp, &1u64);
            }
            fiba
        },
        BatchSize::PerIteration,
    );
}

fn insert_batch_random_fiba_cg_bfinger8(seconds: u64, bencher: &mut Bencher) {
    bencher.iter_batched(
        window::bfinger_eight::create_fiba_8_with_sum,
        |mut fiba| {
            for _i in 0..NUM_ELEMENTS {
                let timestamp = random_timestamp_aligned(seconds);
                fiba.pin_mut().insert(&timestamp, &1u64);
            }
            fiba
        },
        BatchSize::PerIteration,
    );
}

fn _insert_out_of_order_fiba(percentage: f32, bencher: &mut Bencher) {
    bencher.iter_batched(
        || {
            let fiba = window::bfinger_two::create_fiba_with_sum();
            let timestamps = _generate_out_of_order_timestamps(NUM_ELEMENTS, percentage);
            (fiba, timestamps)
        },
        |(mut fiba, timestamps)| {
            for timestamp in timestamps {
                fiba.pin_mut().insert(&timestamp, &1u64);
            }
            fiba
        },
        BatchSize::PerIteration,
    );
}

fn insert_same_timestamp_fiba(bencher: &mut Bencher) {
    let mut fiba = window::bfinger_two::create_fiba_with_sum();
    bencher.iter(|| {
        fiba.pin_mut().insert(&1000, &1u64);
    });
}
fn insert_fiba_bfinger2_random(seconds: u64, bencher: &mut Bencher) {
    let mut fiba = window::bfinger_two::create_fiba_with_sum();
    bencher.iter(|| {
        let ts = random_timestamp(seconds);
        fiba.pin_mut().insert(&ts, &1u64);
    });
}
fn insert_fiba_cg_bfinger2_random(seconds: u64, bencher: &mut Bencher) {
    let mut fiba = window::bfinger_two::create_fiba_with_sum();
    bencher.iter(|| {
        let ts = random_timestamp_aligned(seconds);
        fiba.pin_mut().insert(&ts, &1u64);
    });
}
fn insert_fiba_bfinger4_random(seconds: u64, bencher: &mut Bencher) {
    let mut fiba = window::bfinger_four::create_fiba_4_with_sum();
    bencher.iter(|| {
        let ts = random_timestamp(seconds);
        fiba.pin_mut().insert(&ts, &1u64);
    });
}
fn insert_fiba_cg_bfinger4_random(seconds: u64, bencher: &mut Bencher) {
    let mut fiba = window::bfinger_four::create_fiba_4_with_sum();
    bencher.iter(|| {
        let ts = random_timestamp_aligned(seconds);
        fiba.pin_mut().insert(&ts, &1u64);
    });
}
fn insert_fiba_bfinger8_random(seconds: u64, bencher: &mut Bencher) {
    let mut fiba = window::bfinger_eight::create_fiba_8_with_sum();
    bencher.iter(|| {
        let ts = random_timestamp(seconds);
        fiba.pin_mut().insert(&ts, &1u64);
    });
}
fn insert_fiba_cg_bfinger8_random(seconds: u64, bencher: &mut Bencher) {
    let mut fiba = window::bfinger_eight::create_fiba_8_with_sum();
    bencher.iter(|| {
        let ts = random_timestamp_aligned(seconds);
        fiba.pin_mut().insert(&ts, &1u64);
    });
}

fn insert_wheel_random(seconds: u64, bencher: &mut Bencher) {
    let mut wheel = RwWheel::<U64SumAggregator>::new(0);
    bencher.iter(|| {
        let ts = random_timestamp(seconds);
        wheel.insert(Entry::new(1, ts));
    });
}

fn insert_same_timestamp_wheel(bencher: &mut Bencher) {
    let mut wheel = RwWheel::<U64SumAggregator>::new(0);
    bencher.iter(|| {
        wheel.insert(Entry::new(1, 1000));
    });
}

#[inline]
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
