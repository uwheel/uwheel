use uwheel::{aggregator::sum::U64SumAggregator, Entry, NumericalDuration, RwWheel};

fn main() {
    let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(0);

    // Install window
    wheel.window(10.seconds(), 3.seconds());

    for i in 1..=22 {
        wheel.insert(Entry::new(i, i * 1000 - 1));
    }
    let results = wheel.advance(22.seconds());
    println!("{:#?}", results);
}
