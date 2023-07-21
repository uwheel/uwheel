use awheel::{
    aggregator::U32SumAggregator,
    rw_wheel::RwWheel,
    time::{Duration, NumericalDuration},
    Entry,
    ReadWheel,
    SECONDS,
};
use postcard::to_allocvec;
use std::time::Instant;

fn main() {
    let mut time = 0;
    let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
    let seconds_ticks = SECONDS - 1;
    let minutes_ticks = (3600 - seconds_ticks) - 1;
    let hours_ticks = (86400 - (minutes_ticks + seconds_ticks)) - 1;
    let days_ticks = (604800 - (hours_ticks + minutes_ticks + seconds_ticks)) - 1;
    let weeks_ticks = (Duration::weeks(52).whole_seconds() as usize
        - (days_ticks + hours_ticks + minutes_ticks + seconds_ticks))
        - 1;
    let years_ticks = (Duration::years(10).whole_seconds() as usize
        - (weeks_ticks + days_ticks + hours_ticks + minutes_ticks + seconds_ticks))
        - 1;

    println!("====EMPTY WHEEL====");
    serialize_wheel(wheel.read());

    for _ in 0..seconds_ticks {
        wheel.write().insert(Entry::new(1u32, time)).unwrap();
        time += 1000;
        wheel.advance(1.seconds());
    }
    println!("====Levels: l0====");
    serialize_wheel(wheel.read());

    for _ in 0..minutes_ticks {
        wheel.write().insert(Entry::new(1u32, time)).unwrap();
        time += 1000;
        wheel.advance(1.seconds());
    }
    println!("====Levels: l0-l1====");
    serialize_wheel(wheel.read());

    for _ in 0..hours_ticks {
        wheel.write().insert(Entry::new(1u32, time)).unwrap();
        time += 1000;
        wheel.advance(1.seconds());
    }
    println!("====Levels: l0-l2====");
    serialize_wheel(wheel.read());

    for _ in 0..days_ticks {
        wheel.write().insert(Entry::new(1u32, time)).unwrap();
        time += 1000;
        wheel.advance(1.seconds());
    }
    println!("====Levels: l0-l3====");
    serialize_wheel(wheel.read());

    for _ in 0..weeks_ticks {
        wheel.write().insert(Entry::new(1u32, time)).unwrap();
        time += 1000;
        wheel.advance(1.seconds());
    }
    println!("====Levels: l0-l4====");
    serialize_wheel(wheel.read());

    for _ in 0..years_ticks {
        wheel.write().insert(Entry::new(1u32, time)).unwrap();
        time += 1000;
        wheel.advance(1.seconds());
    }
    println!("====Levels: l0-l5====");
    serialize_wheel(wheel.read());
}

fn serialize_wheel(wheel: &ReadWheel<U32SumAggregator>) {
    println!("Memory wheel size {} bytes", wheel.size());
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
    let now = Instant::now();
    let zstd_compressed = zstd::bulk::compress(&bytes, 3).unwrap();
    println!(
        "zstd serialised wheel size {} bytes in {:?}",
        zstd_compressed.len(),
        now.elapsed(),
    );
}
