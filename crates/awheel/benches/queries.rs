use awheel_core::{aggregator::all::AllAggregator, Options, RwWheel, *};
use criterion::{criterion_group, criterion_main, Bencher, Criterion};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wheel-queries");
    group.bench_function("random-interval", random_interval_bench);
    group.bench_function("random-seconds-interval", random_seconds_interval_bench);
    group.bench_function("random-minutes-interval", random_minutes_interval_bench);
    group.bench_function("random-hours-interval", random_hour_interval_bench);
    group.bench_function("random-days-interval", random_days_interval_bench);

    // drill-down
    group.bench_function(
        "random-drill-down-interval",
        random_drill_down_interval_bench,
    );

    group.bench_function(
        "random-minutes-drill-down-interval",
        random_minutes_drill_down_interval_bench,
    );
    group.bench_function(
        "random-hours-drill-down-interval",
        random_hours_drill_down_interval_bench,
    );
    group.bench_function(
        "random-days-drill-down-interval",
        random_days_drill_down_interval_bench,
    );

    // landmark
    group.bench_function("landmark-window", landmark_window_bench);

    group.finish();
}

#[inline]
fn random_interval(wheel: &RwWheel<AllAggregator>) {
    let pick = fastrand::usize(0..4);
    if pick == 0usize {
        assert!(wheel
            .read()
            .interval(time::Duration::seconds(random_seconds() as i64))
            .is_some());
    } else if pick == 1usize {
        assert!(wheel
            .read()
            .interval(time::Duration::minutes(random_minute() as i64))
            .is_some());
    } else if pick == 2usize {
        assert!(wheel
            .read()
            .interval(time::Duration::hours(random_hours() as i64))
            .is_some());
    } else {
        assert!(wheel
            .read()
            .interval(time::Duration::days(random_days() as i64))
            .is_some());
    }
}
#[inline]
fn random_drill_down_interval(wheel: &RwWheel<AllAggregator>) {
    let pick = fastrand::usize(0..3);
    if pick == 0usize {
        assert!(wheel
            .read()
            .as_ref()
            .minutes_unchecked()
            .drill_down_interval(random_minute())
            .is_some());
    } else if pick == 1usize {
        assert!(wheel
            .read()
            .as_ref()
            .hours_unchecked()
            .drill_down_interval(random_hours())
            .is_some());
    } else {
        assert!(wheel
            .read()
            .as_ref()
            .days_unchecked()
            .drill_down_interval(random_days())
            .is_some());
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

fn large_wheel() -> RwWheel<AllAggregator> {
    let mut time = 0;
    let mut wheel: RwWheel<AllAggregator> =
        RwWheel::with_options(time, Options::default().with_drill_down());
    let ticks = 604800; // 7-days as seconds
    for _ in 0..ticks {
        wheel.advance_to(time);
        wheel.insert(Entry::new(1.0, time));
        time += 1000;
    }
    wheel
}
fn random_drill_down_interval_bench(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| random_drill_down_interval(&wheel));
}

fn random_interval_bench(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| random_interval(&wheel));
}
fn landmark_window_bench(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| {
        assert!(wheel.read().landmark().is_some());
    });
}

fn random_seconds_interval_bench(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| {
        assert!(wheel
            .read()
            .interval(time::Duration::seconds(random_seconds() as i64))
            .is_some());
    });
}
fn random_minutes_interval_bench(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| {
        assert!(wheel
            .read()
            .interval(time::Duration::minutes(random_minute() as i64))
            .is_some());
    });
}

fn random_hour_interval_bench(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| {
        assert!(wheel
            .read()
            .interval(time::Duration::hours(random_hours() as i64))
            .is_some());
    });
}
fn random_days_interval_bench(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| {
        assert!(wheel
            .read()
            .interval(time::Duration::days(random_days() as i64))
            .is_some());
    });
}

fn random_minutes_drill_down_interval_bench(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| {
        assert!(wheel
            .read()
            .as_ref()
            .minutes_unchecked()
            .drill_down_interval(random_minute())
            .is_some());
    });
}

fn random_hours_drill_down_interval_bench(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| {
        assert!(wheel
            .read()
            .as_ref()
            .hours_unchecked()
            .drill_down_interval(random_hours())
            .is_some());
    });
}
fn random_days_drill_down_interval_bench(bencher: &mut Bencher) {
    let wheel = large_wheel();
    bencher.iter(|| {
        assert!(wheel
            .read()
            .as_ref()
            .days_unchecked()
            .drill_down_interval(random_days())
            .is_some());
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
