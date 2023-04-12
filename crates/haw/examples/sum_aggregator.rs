use haw::{aggregator::U32SumAggregator, time::NumericalDuration, *};

fn main() {
    let aggregator = U32SumAggregator;
    // Initial start time
    let mut time = 0;
    let mut wheel = Wheel::new(time);

    // Fill the seconds wheel (60 slots)
    for _ in 0..60 {
        wheel.insert(Entry::new(1u32, time)).unwrap();
        wheel.advance(1.seconds());
        time += 1000;
    }

    // force a rotation of the seconds wheel
    wheel.advance(1.seconds());

    assert_eq!(wheel.seconds_wheel().len(), 60);

    // MINUTES_WHEEL: 60 seconds of partial aggregates should have been ticked over to the minute wheel
    assert_eq!(wheel.minutes_wheel().lower(1, &aggregator), Some(60));

    // WHOLE WHEEL: full range of data
    assert_eq!(wheel.landmark(), Some(60));

    // WHOLE WHEEL: interval of last 15 seconds
    assert_eq!(wheel.interval(15.seconds()), Some(14));
}
