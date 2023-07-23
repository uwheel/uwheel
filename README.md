# awheel

## What it is

awheel (aggregation wheel) is a lightweight index for unified stream and temporal warehousing.

Features:

- Versatile
    - OLAP (Roll-ups, Drill-downs)
    - Stream Analytics
- Lightweight
    - Exploits hierarchical nature of time
    - Implicit timestamps (event-time indexed wheels)
- Performance
    - Decoupled write and read paths
    - High-throughput ingestion
    - Low-latency queries

## Use cases

- Unified OLAP and Stream Analytics
    - Streaming Window Aggregation
    - Index for speeding up temporal OLAP queries
    - Materialized view with embedded time dimension
- Analytics at the edge
    - WASM + ``#[no_std]`` compatible
    - Low memory footprint
    - Serializable and highly compressible


## Aggregation Framework

The Aggregation Interface is inspired by the work of [Tangwongsan et al.](http://www.vldb.org/pvldb/vol8/p702-tangwongsan.pdf). 

**Pre-defined Aggregators:**

| Function | Description | Types |
| ---- | ------| ----- |
| SUM |  Sum of all inputs | u16, u32, u64, u128, i16, i32, i64, i128, f32, f64 | 
| AVG |  Arithmetic mean of all inputs | u16, u32, u64, u128, i16, i32, i64, i128, f32, f64 | 
| MIN |  Minimum value of all inputs |  u16, u32, u64, u128, i16, i32, i64, i128, f32, f64 | 
| MAX |  Maximum value of all inputs | u16, u32, u64, u128, i16, i32, i64, i128, f32, f64 | 
| ALL |  Pre-computed SUM, AVG, MIN, MAX, COUNT | f64 |
| TOP N  |  Top N of all inputs | ``Aggregator`` with aggregate data that implements ``Ord`` |

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
    - Enables top_n aggregation
- `window`
    - Enables wheels for streaming window aggregation
- `sync` (_implicitly enables `std`_)
    - Enables a sync version of ``ReadWheel`` that can be queried across threads
- `stats` (_implicitly enables `std`_)
    - Enables recording of latencies for various operations
- `serde`
    - Enables serde support
- `tree`
    - Enables the multi-key ``RwTreeWheel``


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
