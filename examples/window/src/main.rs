use uwheel::{aggregator::sum::U64SumAggregator, Entry, NumericalDuration, RwWheel, Window};

fn main() {
    let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(0);

    // Install window
    wheel.window(
        Window::default()
            .with_range(10.seconds())
            .with_slide(3.seconds()),
    );

    // insert an entry per second
    for i in 1..=22 {
        wheel.insert(Entry::new(i, i * 1000 - 1));
    }
    // advance the wheel by 22 seconds and see which window aggregates are produced
    let results = wheel.advance(22.seconds());
    println!("{:#?}", results);
}
