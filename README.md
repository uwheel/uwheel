<p align="center">
  <img width="300" height="300" src="assets/logo.png">
</p>

![ci](https://github.com/uwheel/uwheel/actions/workflows/rust.yml/badge.svg)
[![Cargo](https://img.shields.io/badge/crates.io-v0.3.0-orange)](https://crates.io/crates/uwheel)
[![Documentation](https://docs.rs/uwheel/badge.svg)](https://docs.rs/uwheel)
[![unsafe forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](https://github.com/rust-secure-code/safety-dance/)
[![Discord](https://img.shields.io/discord/1245309039940993085?label=µWheel%20discord)](https://discord.gg/dhRxfck9jN)
[![Apache](https://img.shields.io/badge/license-Apache-blue.svg)](https://github.com/uwheel/uwheel/blob/main/LICENSE-APACHE)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/uwheel/uwheel/blob/main/LICENSE-MIT)

# µWheel

µWheel is an Embeddable Aggregate Management System for Streams and Queries.

See more about its design [here](DESIGN.md) and try it out directly on the [web](https://uwheel.github.io/uwheel/).

## Features

- Streaming window aggregation
- Built-in warehousing capabilities
- Wheel-based query optimizer + vectorized execution.
- Out-of-order support using ``low watermarking``.
- High-throughput stream ingestion.
- User-defined aggregation.
- Low space footprint.
- Incremental checkpointing support.
- Compatible with ``#[no_std]`` (requires ``alloc``).

## When should I use µWheel?

µWheel unifies the aggregate management for online streaming and offline analytical queries in a single system.
µWheel is not a general purpose solution but a specialized system tailored for a pre-defined aggregation function.

µWheel is an excellent choice when:

- You know the aggregation function apriori.
- You need high-throughput ingestion of out-of-order streams.
- You need support for streaming window queries (e.g., Sliding/Tumbling).
- You need support for exploratory analysis of historical data.
- You need a lightweight and highly embeddable solution.

**Example use cases:**

- A mini stream processor ([see example](examples/window))
- A real-time OLAP index (e.g., Top-N) ([see example](examples/top-n))
- A compact and mergeable system for analytics at the edge ([see example](examples/aggregator)).

## Pre-defined Aggregators

| Function | Description | Types | SIMD |
| ---- | ------| ----- |----- |
| SUM |  Sum of all inputs | u16, u32, u64, i16, i32, i64, f32, f64 | &check; |
| MIN |  Minimum value of all inputs |  u16, u32, u64, i32, i16, i64, f32, f64 | &check;|
| MAX |  Maximum value of all inputs | u16, u32, u64, i16, i32, i64, f32, f64 | &check;|
| MINMAX |  Minimum and Maximum value of all inputs | u16, u32, u64, i16, i32, i64, f32, f64 | &cross;|
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
- `min_max` (_enabled by default_)
  - Enables min-max aggregation
- `all` (_enabled by default_)
  - Enables all aggregation
- `top_n`
  - Enables Top-N aggregation
- `roaring`
  - Enables roaring bitmap aggregators
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
uwheel  = "0.3.0"
```

For ``no_std`` support and minimal compile time:

```toml
uwheel = { version = "0.3.0", default-features = false }
```

## Examples

The following code is from the [hello world](examples/hello_world) example.

```rust
use uwheel::{aggregator::sum::U32SumAggregator, WheelRange, NumericalDuration, Entry, RwWheel};

// Initial start watermark 2023-11-09 00:00:00 (represented as milliseconds)
let mut watermark = 1699488000000;
// Create a Reader-Writer Wheel with U32 Sum Aggregation using the default configuration
let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(watermark);

// Install a Sliding Window Aggregation Query (results are produced when we advance the wheel).
wheel.window(Window::sliding(30.minutes(), 10.minutes()));

// Simulate ingestion and fill the wheel with 1 hour of aggregates (3600 seconds).
for _ in 0..3600 {
    // Insert entry with data 1 to the wheel
    wheel.insert(Entry::new(1u32, watermark));
    // bump the watermark by 1 second and also advanced the wheel
    watermark += 1000;

    // Print the result if any window is triggered
    for window in wheel.advance_to(watermark) {
        println!("Window fired {:#?}", window);
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

## Acknowledgements

- µWheel borrows scripts from the [egui](https://github.com/emilk/egui) crate.
- µWheel uses a modified Duration from the [time](https://github.com/time-rs/time) crate.
- µWheel soft forks a [Hierarchical Timing Wheel](https://github.com/Bathtor/rust-hash-wheel-timer) made by @Bathtor.

## Contributing

See [Contributing](CONTRIBUTING.md).

## Community

If you find µWheel interesting and want to learn more, then join the [Discord](https://discord.gg/dhRxfck9jN) community!

## Publications

- Max Meldrum, Paris Carbone (2024). µWheel: Aggregate Management for Streams and Queries (**Best Paper Award**). In DEBS '24. [[PDF]](https://dl.acm.org/doi/pdf/10.1145/3629104.3666031).

## Blog Posts

- [Introducing datafusion-uwheel, A Native DataFusion Optimizer for Time-based Analytics](https://uwheel.rs/post/datafusion_uwheel/) - August 2024
- [Best Paper Award + 0.2.0 Release](https://uwheel.rs/post/best-paper-award-020-release/) - July 2024
- [Speeding up Temporal Aggregation in DataFusion by 60-60000x using µWheel](https://uwheel.rs/post/datafusion/) - May 2024

## Citing µWheel

```
@inproceedings{meldrum2024uwheel,
  author = {Meldrum, Max and Carbone, Paris},
  title = {μWheel: Aggregate Management for Streams and Queries},
  booktitle = {Proceedings of the 18th ACM International Conference on Distributed and Event-Based Systems},
  year = {2024},
  pages = {54--65},
  doi = {10.1145/3629104.3666031}
}
```

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
