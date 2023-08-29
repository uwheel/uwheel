use std::{cell::RefCell, rc::Rc};

use awheel::{
    aggregator::sum::U32SumAggregator,
    time::NumericalDuration,
    Entry,
    ReadWheel,
    RwWheel,
};

fn main() {
    let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
    let sum = Rc::new(RefCell::new(0));
    let inner_sum = sum.clone();

    // schedule a repeat action
    let _ = wheel
        .timer()
        .schdule_repeat(5000, 5.seconds(), move |read: &ReadWheel<_>| {
            if let Some(last_five) = read.interval(5.seconds()) {
                println!("Last five {}", last_five);
                *inner_sum.borrow_mut() += last_five;
            }
        });

    for i in 1..5u64 {
        wheel.insert(Entry::new(250, i * 1000));
    }

    // trigger first timer to add sum of last 5 seconds
    wheel.advance(5.seconds());
    assert_eq!(*sum.borrow(), 1000);

    for i in 5..8u64 {
        wheel.insert(Entry::new(250, i * 1000));
    }

    // trigger second timer to add sum of last 5 seconds
    wheel.advance(5.seconds());
    assert_eq!(*sum.borrow(), 1750);
}
