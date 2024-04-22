use uwheel::{aggregator::sum::U32SumAggregator, Entry, NumericalDuration, RwWheel, WheelRange};

fn main() {
    // Initial start watermark 2023-11-09 00:00:00 (represented as milliseconds)
    let mut watermark = 1699488000000;
    // Create a Reader-Writer Wheel with U32 Sum Aggregation using the default configuration
    let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(watermark);

    // Fill the wheel 3600 worth of seconds
    for _ in 0..3600 {
        // Insert entry into the wheel
        wheel.insert(Entry::new(1u32, watermark));
        // bump the watermark by 1 second and also advanced the wheel
        watermark += 1000;
        wheel.advance_to(watermark);
    }
    // The low watermark is now 2023-11-09 01:00:00

    // query the wheel using different intervals
    assert_eq!(wheel.read().interval(15.seconds()), Some(15));
    assert_eq!(wheel.read().interval(1.minutes()), Some(60));

    // combine range of 2023-11-09 00:00:00 and 2023-11-09 01:00:00
    let start = 1699488000000;
    let end = 1699491600000;
    let range = WheelRange::new_unchecked(start, end);
    assert_eq!(wheel.read().combine_range(range), Some(3600));
    // The following runs the the same combine range query as above.
    assert_eq!(wheel.read().interval(1.hours()), Some(3600));
}
