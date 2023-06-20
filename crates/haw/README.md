# Hierarchical Aggregation Wheel (HAW)

## What it is
HAW is a lightweight index that pre-computes and maintains aggregates across stream event time.

Features:

- Fast insertions
- Compact and highly compressible
- Event-time driven using low watermarking
- Bounded query latency
- Roll-ups & drill-downs
- Compatible with `#[no_std]` with the ``alloc`` crate

## How it works

Similarly to Hierarchical Wheel Timers, HAW exploits the hierarchical nature of time and utilise several aggregation wheels,
each with a different time granularity. This enables a compact representation of aggregates across time
with a low memory footprint and makes it highly compressible and efficient to store on disk. HAWs are event-time driven and uses the notion of a Watermark which means that no timestamps are stored as they are implicit in the wheel slots. It is up to the user of the wheel to advance the watermark and thus roll up aggregates continously up the time hierarchy.

- `Aggregator` 
    - Interface defining the aggregation on top of HAW
    - HAW comes with pre-defined aggregators (e.g., SUM, AVG, ALL, TOP_K)
- `Watermark`
    - A u64 integer representing the current time
    - Insertions below the watermark will be rejected
- `Write-ahead Wheel`
    - Mutable wheel slots ahead of the watermark
    - Supports distributive, algebraic, and holistic aggregations.
- `Aggregation Wheels`
    - A hierarchy of wheels with immutable slots
    - Supports distributive and algebraic aggregations


For instance, to store aggregates with second granularity up to 10 years, we would need the following aggregation wheels:

* Seconds wheel with 60 slots
* Minutes wheel with 60 slots
* Hours wheel with 24 slots
* Days wheel with 7 slots
* Weeks wheel with 52 slots
* Years wheel with 10 slots

The above scheme results in a total of 213 wheel slots. This is the minimum number of slots
required to support rolling up aggregates across 10 years with second granularity.




## Feature Flags
- `std` (_enabled by default_)
    - Enables features that rely on the standard library
- `years_size_10` (_enabled by default_)
    - Enables rolling up aggregates across 10 years
- `years_size_100`
    - Enables rolling up aggregates across 100 years
- `rkyv`
    - Enables serialisation & deserialisation using the [rkyv](https://docs.rs/rkyv/latest/rkyv/) framework.

## Examples

```rust
use haw::{aggregator::U32SumAggregator, time::NumericalDuration, Entry, Wheel};

// Initial start time (represented as milliseconds)
let mut time = 0;
let mut wheel: Wheel<U32SumAggregator> = Wheel::new(time);

// Fill the seconds wheel (60 slots)
for _ in 0..60 {
    wheel.insert(Entry::new(1u32, time)).unwrap();
    time += 1000;
}

// force a rotation of the seconds wheel
wheel.advance(60.seconds());

// interval of last 1 minute
assert_eq!(wheel.interval(1.minutes()), Some(60));

// full range of data
assert_eq!(wheel.landmark(), Some(60));

// interval of last 15 seconds
assert_eq!(wheel.interval(15.seconds()), Some(15));
```

## License

Licensed under either of

  * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
  * MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
