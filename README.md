<p align="center">
  <img width="300" height="300" src="assets/logo.png">
</p>

# µWheel

µWheel is an Event-driven Aggregate Management System.

Features:

- Versatile
    - Event-driven Stream Analytics
    - Temporal Warehousing
    - Time-Series Analysis
- Lightweight
    - Pre-aggregation
    - Exploits hierarchical nature of time
    - Implicit timestamps (event-time indexed wheels)
- Performance
    - Decoupled write and read paths
    - High-throughput ingestion
    - Low-latency queries
- Embeddable
    - ``#[no_std]`` compatible (requires ``alloc``)
    - WASM friendly

## How it works

µWheel is designed around event-time (Low Watermarking) indexed aggregate wheels.
Writes are handled by a writer wheel, which supports in-place aggregation and is
optimized for single-threaded ingestion. Reads, on the other hand, are managed through a hierarchically event-time-indexed
reader wheel which uses a wheel-centric query optimizer.


## Aggregation Framework


* ``lift(input) -> MutablePartialAggregate``
    * Lifts input data into a mutable aggregate
* ``combine_mutable(mutable, input)``
    * Combines the input data into a mutable aggregate
* ``freeze(mutable) -> PartialAggregate``
    * Freezes the mutable aggregate into an immutable one
* ``combine(a, a) -> a``
    * Combines ⊕ two partial aggregates into a new one
* ``lower(a) -> Aggregate``
    * Lowers a partial aggregate to a final aggregate (e.g., sum/count -> avg)


**Pre-defined Aggregators:**

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
- `window`
    - Enables optimizations for streaming window aggregation queries
- `simd` (_requires `nightly`_)
    - Enables support to speed up aggregation functions with SIMD operations
- `sync` (_implicitly enables `std`_)
    - Enables a sync version of ``ReadWheel`` that can be shared and queried across threads
- `stats` (_implicitly enables `std`_)
    - Enables recording of latencies for various operations
- `serde`
    - Enables serde support
- `timer`
    - Enables scheduling user-defined functions

## Usage

For ``std`` support and compilation of built-in aggregators:

```toml
awheel  = "0.1.0"
```
For ``no_std`` support and minimal compile time:

```toml
awheel = { version = "0.1.0", default-features = false }
```

## Examples

```rust
use awheel::{aggregator::U32SumAggregator, time::NumericalDuration, Entry, RwWheel};

// Initial start time (represented as milliseconds)
let mut time = 0;
let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(time);

// Fill the seconds wheel (60 slots)
for _ in 0..60 {
    wheel.write().insert(Entry::new(1u32, time)).unwrap();
    time += 1000;
}

// force a rotation of the seconds wheel
wheel.advance(60.seconds());

// interval of last 1 minute
assert_eq!(wheel.read().interval(1.minutes()), Some(60));

// full range of data
assert_eq!(wheel.read().landmark(), Some(60));

// interval of last 15 seconds
assert_eq!(wheel.read().interval(15.seconds()), Some(15));
```

See more examples [here](examples).

## Demo

<img src="crates/awheel-demo/assets/awheel_demo.gif">

An interactive demo [application](crates/awheel-demo/) that works both natively and on the web.


## License

Licensed under either of

  * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
  * MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
