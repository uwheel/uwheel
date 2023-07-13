# Hierarchical Aggregation Wheel (HAW)

## What it is
HAW is a lightweight index for unified stream and temporal warehousing.

Features:

- Streaming Window Aggregation (Sliding, Tumbling, Landmark)
- OLAP (Roll-ups, Drill-downs)
- Event-time driven using low watermarking
- Low memory footprint
- Serializable and highly compressible
- High-throughput ingestion
- Low-latency query execution
- Compatible with `#[no_std]` with the ``alloc`` crate

## How it works

Similarly to Hierarchical Wheel Timers, HAW exploits the hierarchical nature of time and utilise several aggregation wheels,
each with a different time granularity. This enables a compact representation of aggregates across time
with a low memory footprint and makes it highly compressible and efficient to store on disk. HAWs are event-time driven and uses the notion of a Watermark which means that no timestamps are stored as they are implicit in the wheel slots. It is up to the user of the wheel to advance the watermark and thus roll up aggregates continously up the time hierarchy.

For instance, to store aggregates with second granularity up to 10 years, we would need the following aggregation wheels:

* Seconds wheel with 60 slots
* Minutes wheel with 60 slots
* Hours wheel with 24 slots
* Days wheel with 7 slots
* Weeks wheel with 52 slots
* Years wheel with 10 slots

The above scheme results in a total of 213 wheel slots. This is the minimum number of slots
required to support rolling up aggregates across 10 years with second granularity.

## Aggregation Framework

HAWs Aggregation Interface is inspired by the work of [Tangwongsan et al.](http://www.vldb.org/pvldb/vol8/p702-tangwongsan.pdf). 

To implement your own custom aggregator, you have to implement the [Aggregator](https://github.com/Max-Meldrum/haw/blob/42d413c54845ab4370115bf5afb9a3e383eecaaa/crates/haw/src/aggregator/mod.rs#L26C11-L26C21) trait for your type.

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
- `window` (_enabled by default_)
    - Enables wheels for streaming window aggregation
- `sum` (_enabled by default_)
    - Enables sum aggregation
- `avg` (_enabled by default_)
    - Enables avg aggregation
- `min` (_enabled by default_)
    - Enables min aggregation
- `max` (_enabled by default_)
    - Enables max aggregation
- `top_n`
    - Enables top_n aggregation
- `sync` (_implicitly enables `std`_)
    - Enables a sync version of ``ReadWheel`` that can be queried across threads
- `stats` (_implicitly enables `std`_)
    - Enables recording of latencies for various operations
- `tree`
    - Enables the multi-key ``RwTreeWheel``
- `rkyv`
    - Enables serialisation & deserialisation using the [rkyv](https://docs.rs/rkyv/latest/rkyv/) framework.


## Usage

By default the library relies on ``std`` and compiles pre-defined aggregators:

```toml
haw = "0.1.0"
```
For ``no_std`` support and no default aggregators:

```toml
haw = { version = "0.1.0", default-features = false }
```

## Examples

```rust
use haw::{aggregator::U32SumAggregator, time::NumericalDuration, Entry, RwWheel};

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

## License

Licensed under either of

  * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
  * MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
