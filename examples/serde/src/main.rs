use awheel::{
    aggregator::U32SumAggregator,
    rw_wheel::RwWheel,
    time::NumericalDuration,
    Entry,
    ReadWheel,
};
use postcard::to_allocvec;
use std::time::Instant;

fn main() {
    let mut time = 0;
    let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
    let ticks = wheel.read().remaining_ticks();
    println!("====EMPTY WHEEL====");
    serialize_wheel(wheel.read());

    // Fill the wheel
    for _ in 0..ticks {
        wheel.write().insert(Entry::new(1u32, time)).unwrap();
        time += 1000;
        wheel.advance(1.seconds());
    }
    println!("====FULL WHEEL====");
    serialize_wheel(wheel.read());
}

fn serialize_wheel(wheel: &ReadWheel<U32SumAggregator>) {
    let now = Instant::now();
    let bytes = to_allocvec(wheel).unwrap();
    println!(
        "Serialised wheel size {} bytes in {:?}",
        bytes.len(),
        now.elapsed()
    );
    let now = Instant::now();
    let _out: ReadWheel<U32SumAggregator> = postcard::from_bytes(&bytes).unwrap();
    println!("Deserialized wheel in {:?}", now.elapsed());

    let now = Instant::now();
    let lz4_compressed = lz4_flex::compress_prepend_size(&bytes);
    println!(
        "lz4 serialised wheel size {} bytes in {:?}",
        lz4_compressed.len(),
        now.elapsed(),
    );
}
