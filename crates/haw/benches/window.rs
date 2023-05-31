use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion};
use haw::{
    aggregator::{Aggregator, U32SumAggregator},
    *,
};

const TICK_COUNT: usize = 10000;

pub fn tumbling_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("tumbling");
    for window_size in [5, 10, 30, 60].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("wheel_{}", window_size)),
            window_size,
            |b, &window_size| {
                tumbling(window_size as usize, b);
            },
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("no_wheel_{}", window_size)),
            window_size,
            |b, &window_size| {
                tumbling_no_wheel(window_size as usize, b);
            },
        );
    }
    group.finish();
}

fn tumbling(window_size: usize, bencher: &mut Bencher) {
    let mut time = 0;
    let mut window_counter = 0;
    let mut counter = 0;
    let mut wheel = Wheel::<U32SumAggregator>::new(time);
    bencher.iter(|| {
        while window_counter < window_size {
            wheel.insert(Entry::new(1u32, time)).unwrap();
            counter += 1;
            if counter == TICK_COUNT {
                time += 1000;
                wheel.advance_to(time);
                window_counter += 1;
                counter = 0;
            }
        }
        wheel.advance_to(time);
        wheel.seconds_unchecked().interval(window_size)
    });
}

fn tumbling_no_wheel(window_size: usize, bencher: &mut Bencher) {
    let aggregator = U32SumAggregator;
    let mut window_counter = 0;
    let mut counter = 0;
    bencher.iter(|| {
        let mut current = aggregator.lift(0u32);
        while window_counter < window_size {
            let entry = aggregator.lift(1u32);
            current = aggregator.combine(entry, current);
            counter += 1;
            if counter == TICK_COUNT {
                window_counter += 1;
                counter = 0;
            }
        }
        aggregator.lower(current)
    });
}

criterion_group!(benches, tumbling_benchmark);
criterion_main!(benches);
