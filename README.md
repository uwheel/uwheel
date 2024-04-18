<p align="center">
  <img width="300" height="300" src="assets/logo.png">
</p>

![ci](https://github.com/Max-Meldrum/uwheel/actions/workflows/rust.yml/badge.svg)
[![unsafe forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](https://github.com/rust-secure-code/safety-dance/)
[![Apache](https://img.shields.io/badge/license-Apache-blue.svg)](https://github.com/Max-Meldrum/uwheel/blob/main/LICENSE-APACHE)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/Max-Meldrum/uwheel/blob/main/LICENSE-MIT)

# µWheel

µWheel is an Embeddable Aggregate Management System for Hybrid Stream and Analytical Processing.

See more about its design [here](DESIGN.md) and try it out directly on the [web](https://maxmeldrum.com/uwheel).

## Features

- Wheel-based query optimizer
- Vectorized query execution.
- Out-of-order support using ``low watermarking``.
- User-defined aggregation.
- High-throughput stream ingestion.
- Low space footprint.
- Incremental checkpointing support.
- Fully mergeable.
- Compatible with ``#[no_std]`` (requires ``alloc``).

## Use cases

- A mini stream processor ([see example](examples/window))
- A tiny but powerful materialized view for streaming databases.
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

```rust
use uwheel::{aggregator::sum::U32SumAggregator, WheelRange, NumericalDuration, Entry, RwWheel};

// Initial start watermark 2023-11-09 00:00:00 (represented as milliseconds)
let mut watermark = 1699488000000;
// Create a Reader-Writer Wheel with U32 Sum Aggregation using the default configuration
let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(watermark);

// Fill the wheel 3600 worth of seconds
for _ in 0..3600 {
    // Insert entry into the wheel
    wheel.write().insert(Entry::new(1u32, watermark)).unwrap();
    // bump the watermark by 1 second and also advanced the wheel
    watermark += 1000;
    wheel.advance_to(watermark);
}
// The low watermark is now 2023-11-09 01:00:00

// query the wheel using different intervals
assert_eq!(wheel.read().interval(15.seconds()), Some(15));
assert_eq!(wheel.read().interval(1.minutes()), Some(60));

// Combine range of 2023-11-09 00:00:00 and 2023-11-09 01:00:00
let start = 1699488000000;
let end = 1699491600000;
let range = WheelRange::from_unix_timestamps(start, end);
assert_eq!(wheel.read().combine_range(range), Some(3600));
// The following runs the the same combine range query as above.
assert_eq!(wheel.read().interval(1.hours()), Some(3600));
```

See more examples [here](examples).

## Demo

<img src="crates/uwheel-demo/assets/uwheel_demo.gif">

Give µWheel a try directly on the [web](https://maxmeldrum.com/uwheel) or build the demo application [locally](crates/uwheel-demo/) 


## License

Licensed under either of

  * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
  * MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
