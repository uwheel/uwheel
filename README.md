<p align="center">
  <img width="300" height="300" src="assets/logo.png">
</p>

![ci](https://github.com/Max-Meldrum/uwheel/actions/workflows/rust.yml/badge.svg)
[![Cargo](https://img.shields.io/badge/crates.io-v0.1.0-orange)](https://crates.io/crates/uwheel)
[![Documentation](https://docs.rs/uwheel/badge.svg)](https://docs.rs/uwheel)
[![unsafe forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](https://github.com/rust-secure-code/safety-dance/)
[![Apache](https://img.shields.io/badge/license-Apache-blue.svg)](https://github.com/Max-Meldrum/uwheel/blob/main/LICENSE-APACHE)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/Max-Meldrum/uwheel/blob/main/LICENSE-MIT)

# µWheel

µWheel is an Embeddable Aggregate Management System for Streams and Queries.

See more about its design [here](DESIGN.md) and try it out directly on the [web](https://maxmeldrum.com/uwheel).

## Features

- Streaming window aggregation
- Built-in warehousing capabilities
- Wheel-based query optimizer + vectorized execution.
- Out-of-order support using ``low watermarking``.
- High-throughput stream ingestion.
- User-defined aggregation.
- Low space footprint.
- Incremental checkpointing support.
- Fully mergeable.
- Compatible with ``#[no_std]`` (requires ``alloc``).

## When should I use µWheel?

µWheel unifies the aggregate management for online streaming and offline analytical queries in a single system.
µWheel is not a general purpose solution but a specialized system tailored for a pre-defined aggregation function.

µWheel is an execellent choice when:

- You know the aggregation function apriori.
- You need high-throughput ingestion of out-of-order streams.
- You need support for streaming window queries (e.g., Sliding/Tumbling).
- You need support for exploratory analysis of historical data.
- You need a lightweight and highly embeddable solution.

**Example use cases:**

- A mini stream processor ([see example](examples/window))
- A compact and mergeable system for analytics at the edge ([see example](examples/aggregator)).

## Pre-defined Aggregators

| Function | Description | Types | SIMD |
| ---- | ------| ----- |----- |
| SUM |  Sum of all inputs | u16, u32, u64, i16, i32, i64, f32, f64 | &check; |
| MIN |  Minimum value of all inputs |  u16, u32, u64, i32, i16, i64, f32, f64 | &check;|
| MAX |  Maximum value of all inputs | u16, u32, u64, i16, i32, i64, f32, f64 | &check;|
| AVG |  Arithmetic mean of all inputs | u16, u32, u64, i16, i32, i64, f32, f64 | &cross; |
| ALL |  Pre-computed SUM, AVG, MIN, MAX, COUNT | f64 | &cross;|
| TOP N  |  Top N of all inputs | ``Aggregator`` with aggregate data that implements ``Ord`` | &cross;|

See a user-defined aggregator example [here](examples/aggregator/).

## Feature Flags
- `std` (_enabled by default_)
    - Enables features that rely on the standard library
- `sum` (_enabled by default_)
    - Enables sum aggregation
- `avg` (_enabled by default_)
    - Enables avg aggregation
- `min` (_enabled by default_)
    - Enables min aggregation
- `max` (_enabled by default_)
    - Enables max aggregation
- `all` (_enabled by default_)
    - Enables all aggregation
- `top_n`
    - Enables Top-N aggregation
- `simd` (_requires `nightly`_)
    - Enables support to speed up aggregation functions with SIMD operations
- `sync` (_implicitly enables `std`_)
    - Enables a sync version of ``ReaderWheel`` that can be shared and queried across threads
- `profiler` (_implicitly enables `std`_)
    - Enables recording of latencies for various operations
- `serde`
    - Enables serde support
- `timer`
    - Enables scheduling user-defined functions

## Usage

For ``std`` support and compilation of built-in aggregators:

```toml
uwheel  = "0.1.0"
```
For ``no_std`` support and minimal compile time:

```toml
uwheel = { version = "0.1.0", default-features = false }
```

## Examples

The following code is from the [hello_world](examples/hello_world) example.

```rust
use uwheel::{aggregator::sum::U32SumAggregator, WheelRange, NumericalDuration, Entry, RwWheel};

// Initial start watermark 2023-11-09 00:00:00 (represented as milliseconds)
let mut watermark = 1699488000000;
// Create a Reader-Writer Wheel with U32 Sum Aggregation using the default configuration
let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(watermark);

// Install a Sliding Window Aggregation Query (results are produced when we advance the wheel).
wheel.window(
    Window::default()
        .with_range(30.minutes())
        .with_slide(10.minutes()),
);

// Simulate ingestion and fill the wheel with 1 hour of aggregates (3600 seconds).
for _ in 0..3600 {
    // Insert entry with data 1 to the wheel
    wheel.insert(Entry::new(1u32, watermark));
    // bump the watermark by 1 second and also advanced the wheel
    watermark += 1000;

    // Print the result if any window is triggered
    for (ts, window) in wheel.advance_to(watermark) {
        println!("Window fired at {} with result {}", ts, window);
    }
}
// Explore historical data - The low watermark is now 2023-11-09 01:00:00

// query the wheel using different intervals
assert_eq!(wheel.read().interval(15.seconds()), Some(15));
assert_eq!(wheel.read().interval(1.minutes()), Some(60));

// combine range of 2023-11-09 00:00:00 and 2023-11-09 01:00:00
let range = WheelRange::new_unchecked(1699488000000, 1699491600000);
assert_eq!(wheel.read().combine_range(range), Some(3600));
// The following runs the the same combine range query as above.
assert_eq!(wheel.read().interval(1.hours()), Some(3600));
```

See more examples [here](examples).

## Demo

<img src="crates/uwheel-demo/assets/uwheel_demo.gif">

Give µWheel a try directly on the [web](https://maxmeldrum.com/uwheel) or build the demo application [locally](crates/uwheel-demo/) 

## Acknowledgements

- µWheel borrows scripts from the [egui](https://github.com/emilk/egui) crate.
- µWheel uses a modified Duration from the [time](https://github.com/time-rs/time) crate.
- µWheel soft forks a [Hierarchical Timing Wheel](https://github.com/Bathtor/rust-hash-wheel-timer) made by @Bathtor.

## License

Licensed under either of

  * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
  * MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
