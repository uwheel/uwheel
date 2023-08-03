#![no_main]

use arbitrary::Arbitrary;
use awheel::{aggregator::all::AllAggregator, RwWheel, *};
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Arbitrary)]
enum Op {
    Insert(f64, u64),
    Advance(u64),
}

fuzz_target!(|ops: Vec<Op>| {
    let time = 0u64;
    let mut wheel: RwWheel<AllAggregator> = RwWheel::new(time);

    for op in ops {
        match op {
            Op::Insert(data, timestamp) => {
                let _ = wheel.write().insert(Entry::new(data, timestamp));
            }
            Op::Advance(watermark) => {
                wheel.advance_to(watermark);
            }
        }
    }
});
