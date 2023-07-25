use awheel::{
    aggregator::{
        top_n::{TopNAggregator, TopNState},
        U64SumAggregator,
    },
    time::NumericalDuration,
    Entry,
    RwWheel,
};

const N: usize = 10;

/// A Top 10 aggregator with u32 as key and U64SumAggregator as inner aggregator
fn main() {
    let mut wheel: RwWheel<TopNAggregator<u32, N, U64SumAggregator>> = RwWheel::new(0);

    wheel.write().insert(Entry::new((1u32, 10), 1000)).unwrap();
    wheel.write().insert(Entry::new((2u32, 50), 1000)).unwrap();
    wheel.write().insert(Entry::new((3u32, 30), 1000)).unwrap();
    wheel.write().insert(Entry::new((4u32, 20), 1000)).unwrap();
    wheel.write().insert(Entry::new((5u32, 100), 1000)).unwrap();
    wheel.write().insert(Entry::new((6u32, 60), 1000)).unwrap();
    wheel.write().insert(Entry::new((7u32, 55), 1000)).unwrap();
    wheel.write().insert(Entry::new((8u32, 45), 1000)).unwrap();
    wheel.write().insert(Entry::new((9u32, 23), 1000)).unwrap();
    wheel
        .write()
        .insert(Entry::new((10u32, 150), 1000))
        .unwrap();
    wheel
        .write()
        .insert(Entry::new((11u32, 1000), 1000))
        .unwrap();

    wheel.advance_to(2000);

    // print the result from the last second
    let state: TopNState<u32, N, U64SumAggregator> = wheel.read().interval(1.seconds()).unwrap();
    let arr = state.iter();
    println!("{:#?}", arr);

    // insert same keys with different values at different timestamp
    wheel.write().insert(Entry::new((1u32, 100), 2000)).unwrap();
    wheel.write().insert(Entry::new((2u32, 10), 2000)).unwrap();
    wheel.write().insert(Entry::new((3u32, 20), 2000)).unwrap();

    // insert some new keys
    wheel.write().insert(Entry::new((4u32, 15), 2000)).unwrap();
    wheel.write().insert(Entry::new((5u32, 1), 2000)).unwrap();
    wheel
        .write()
        .insert(Entry::new((6u32, 5000), 2000))
        .unwrap();

    // advance again
    wheel.advance_to(3000);

    // interval of last 2 seconds should be two TopKState's combined
    let state: TopNState<u32, N, U64SumAggregator> = wheel.read().interval(2.seconds()).unwrap();
    let arr = state.iter();
    println!("{:#?}", arr);
}
