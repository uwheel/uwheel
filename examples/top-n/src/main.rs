use awheel::{
    aggregator::{
        sum::U64SumAggregator,
        top_n::{Descending, TopNAggregator, TopNState},
    },
    time::NumericalDuration,
    Entry,
    RwWheel,
};
use tinystr::TinyAsciiStr;

const N: usize = 5;
pub type TinyStr = TinyAsciiStr<8>;

fn tiny_str(name: &str) -> TinyStr {
    TinyStr::from_str(name).unwrap()
}

/// A Top N aggregator in descending order with TinyStr as key and U64SumAggregator as inner aggregator
fn main() {
    let mut wheel: RwWheel<TopNAggregator<TinyStr, N, U64SumAggregator, Descending>> =
        RwWheel::new(0);
    wheel
        .write()
        .insert(Entry::new((tiny_str("test1"), 10), 1000))
        .unwrap();
    wheel
        .write()
        .insert(Entry::new((tiny_str("test2"), 50), 1000))
        .unwrap();
    wheel
        .write()
        .insert(Entry::new((tiny_str("test3"), 30), 1000))
        .unwrap();

    wheel
        .write()
        .insert(Entry::new((tiny_str("test4"), 20), 1000))
        .unwrap();
    wheel
        .write()
        .insert(Entry::new((tiny_str("test5"), 100), 1000))
        .unwrap();
    wheel
        .write()
        .insert(Entry::new((tiny_str("test6"), 130), 1000))
        .unwrap();

    wheel.advance_to(2000);

    // print the result from the last second
    let state: TopNState<TinyStr, N, U64SumAggregator> =
        wheel.read().interval(1.seconds()).unwrap();
    let arr = state.as_ref();
    println!("{:#?}", arr);

    // insert same keys with different values at different timestamp
    wheel
        .write()
        .insert(Entry::new((tiny_str("test1"), 100), 2000))
        .unwrap();
    wheel
        .write()
        .insert(Entry::new((tiny_str("test2"), 10), 2000))
        .unwrap();
    wheel
        .write()
        .insert(Entry::new((tiny_str("test3"), 20), 2000))
        .unwrap();

    // insert some new keys
    wheel
        .write()
        .insert(Entry::new((tiny_str("test8"), 15), 2000))
        .unwrap();
    wheel
        .write()
        .insert(Entry::new((tiny_str("test9"), 1), 2000))
        .unwrap();
    wheel
        .write()
        .insert(Entry::new((tiny_str("test10"), 1050), 2000))
        .unwrap();

    // advance again
    wheel.advance_to(3000);

    // interval of last 2 seconds should be two states combined
    let state: TopNState<TinyStr, N, U64SumAggregator> =
        wheel.read().interval(2.seconds()).unwrap();
    let arr = state.as_ref();
    println!("{:#?}", arr);
}
