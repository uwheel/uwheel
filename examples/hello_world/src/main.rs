use uwheel::{
    aggregator::sum::U32SumAggregator,
    Entry,
    NumericalDuration,
    RwWheel,
    WheelRange,
    Window,
};

fn main() {
    // Initial start watermark 2023-11-09 00:00:00 (represented as milliseconds)
    let mut watermark = 1699488000000;
    // Create a Reader-Writer Wheel with U32 Sum Aggregation using the default configuration
    let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(watermark);

    // Install a Sliding Window Aggregation Query (results are produced when we advance the wheel).
    wheel.window(Window::sliding(30.minutes(), 10.minutes()));

    // Simulate ingestion and fill the wheel with 1 hour of aggregates (3600 seconds).
    for _ in 0..3600 {
        // Insert entry with data 1 to the wheel
        wheel.insert(Entry::new(1u32, watermark));
        // bump the watermark by 1 second and also advanced the wheel
        watermark += 1000;

        // Print the result if any window is triggered
        for window in wheel.advance_to(watermark) {
            println!("Window fired {:#?}", window);
        }
    }
    // Explore historical data - The low watermark is now 2023-11-09 01:00:00

    // query the wheel using different intervals
    assert_eq!(wheel.read().interval(15.seconds()), Some(15));
    assert_eq!(wheel.read().interval(1.minutes()), Some(60));

    // combine range of 2023-11-09 00:00:00 and 2023-11-09 01:00:00
    let range = WheelRange::new_unchecked(1699488000000, 1699491600000);
    assert_eq!(wheel.read().combine_range(range), Some(3600));
    // The following runs the the same combine range query as above.
    assert_eq!(wheel.read().interval(1.hours()), Some(3600));
}
