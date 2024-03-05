#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use uwheel::{aggregator::sum::U32SumAggregator, RwWheel, *};

#[derive(Debug, Arbitrary)]
enum Op {
    Insert(u32, u64),
    Advance(u64),
}

fuzz_target!(|ops: Vec<Op>| {
    let time = 0u64;
    let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(time);

    for op in ops {
        match op {
            Op::Insert(data, timestamp) => {
                wheel.insert(Entry::new(data, timestamp));
            }
            Op::Advance(watermark) => {
                wheel.advance_to(watermark);
            }
        }
    }
});
