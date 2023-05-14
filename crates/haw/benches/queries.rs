use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use haw::{aggregator::AllAggregator, *};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wheel-queries");
    group.bench_function("random-interval", random_interval_bench);
    group.bench_function("random-seconds-interval", random_seconds_interval_bench);
    group.bench_function("random-minutes-interval", random_minutes_interval_bench);
    group.bench_function("random-hours-interval", random_hour_interval_bench);
    group.bench_function("random-days-interval", random_days_interval_bench);
    group.bench_function("landmark-window", landmark_window_bench);

    group.finish();
}

#[inline]
fn random_interval(wheel: &Wheel<AllAggregator>) {
    let pick = fastrand::usize(0..4);
    if pick == 0usize {
        assert!(wheel
            .seconds_unchecked()
            .interval(random_seconds())
            .is_some());
    } else if pick == 1usize {
        assert!(wheel
            .minutes_unchecked()
            .interval(random_minute())
            .is_some());
    } else if pick == 2usize {
        assert!(wheel.hours_unchecked().interval(random_hours()).is_some());
    } else {
        assert!(wheel.days_unchecked().interval(random_days()).is_some());
    }
}
fn random_hours() -> usize {
    fastrand::usize(1..24)
}
fn random_minute() -> usize {
    fastrand::usize(1..59)
}
fn random_days() -> usize {
    fastrand::usize(1..7)
}
fn random_seconds() -> usize {
    fastrand::usize(1..59)
}

fn large_wheel() -> (Wheel<AllAggregator>, AllAggregator) {
    let aggregator = AllAggregator;
    let mut time = 0;
    let mut wheel = Wheel::new(time);
    let ticks = 604800; // 7-days as seconds
    for _ in 0..ticks {
        wheel.advance_to(time);
        wheel.insert(Entry::new(1.0, time)).unwrap();
        time += 1000;
    }
    (wheel, aggregator)
}

fn random_interval_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| random_interval(&wheel));
}
fn landmark_window_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel.landmark().is_some());
    });
}

fn random_seconds_interval_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel
            .seconds_unchecked()
            .interval(random_seconds())
            .is_some());
    });
}
fn random_minutes_interval_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel
            .minutes_unchecked()
            .interval(random_minute())
            .is_some());
    });
}

fn random_hour_interval_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel.hours_unchecked().interval(random_hours()).is_some());
    });
}
fn random_days_interval_bench(bencher: &mut Bencher) {
    let (wheel, _) = large_wheel();
    bencher.iter(|| {
        assert!(wheel.days_unchecked().interval(random_days()).is_some());
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
