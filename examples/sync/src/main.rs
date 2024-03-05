use uwheel::{aggregator::sum::U32SumAggregator, Entry, NumericalDuration, RwWheel};

fn main() {
    let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
    rw_wheel.insert(Entry::new(1, 999));
    rw_wheel.advance(1.seconds());

    // share the ReadWheel across threads
    let read = rw_wheel.read().clone();

    let handle = std::thread::spawn(move || {
        println!(
            "Read result from another thread {:#?}",
            read.interval(1.seconds())
        );
    });

    handle.join().expect("Failed to join the thread.");
}
