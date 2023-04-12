#![no_main]

use libfuzzer_sys::fuzz_target;
use haw::*;
use arbitrary::Arbitrary;
use haw::aggregator::U32SumAggregator;
use haw::Wheel;

#[derive(Debug, Arbitrary)]
enum Op {
    Insert(u32, u64),
    Advance(u64),
    Range(u64, u64),
}

fuzz_target!(|ops: Vec<Op>| {
    let time = 0u64;
    let mut wheel: Wheel<U32SumAggregator> = Wheel::new(time);

    for op in ops {
        match op {
            Op::Insert(data, timestamp) => {
                let _ = wheel.insert(Entry::new(data, timestamp));
            }
            Op::Advance(watermark) => {
                wheel.advance_to(watermark);
            }
            Op::Range(start, end) => {
                //let _ = wheel.range(start..end);
            }
        }
    }
});
