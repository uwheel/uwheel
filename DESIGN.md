# µWheel Design Doc

This doc describes the design of µWheel.

The figures used in doc are borrowed from the [paper](https://maxmeldrum.com/assets/files/uwheel_debs24.pdf) "µWheel: Aggregate Management for Streams and Queries", which has been
accepted for publication in the proceedings of ACM, DEBS 2024. (Licensed under CC-BY 4.0)

# Motivation

Both streaming aggregation and temporal ad-hoc queries share the fundamental requirement of the materialization of aggregates over time.
Streaming window aggregation queries typically operate within timeframes measured in seconds or minutes, whereas offline historical queries demand aggregates calculated across time spans extending to hours, days, or even whole months.
Many streaming systems today support the former but are forced to offload data to specialized OLAP systems (e.g., Clickhouse, Apache Pinot) to support more advanced ad-hoc queries.

µWheel unifies the aggregate management of both workloads into a single system.

# Overview

µWheel is an event-driven aggregate management system for the ingestion, indexing, and querying of stream aggregates. µWheel
has built-in support for pre-materialization of aggregates across multiple event-time dimensions.

The design of µWheel is centered around [low-watermarking](http://www.vldb.org/pvldb/vol14/p3135-begoli.pdf), a concept found in modern streaming systems (e.g., Apache Flink, RisingWave, Arroyo).
Wheels in µWheel are low-watermark indexed which enables fast writes and lookups. Additionally, it lets us avoid storing explicit timestamps since they are implicit in the wheels.

A low watermark `w` indicates that all records with timestamps `t` where `t <= w` have been ingested.
µWheel exploits this property and seperates the write and read paths. Writes (timestamps above `w`) are handled by a [Writer Wheel](#Writer-Wheel) which is optimized for single-threaded ingestion of out-of-order aggregates.
Reads (queries with `time < w`) are managed by a hierarchically indexed [Reader Wheel](#Reader-Wheel) that employs a wheel-based query optimizer whose cost function
minimizes the number of aggregate operations for a query.

µWheel adopts a [Lazy Synchronization](#Advancing-Time) aggregation approach. Aggregates are only shifted over from the `WriterWheel` to the `ReaderWheel`
once the internal low watermark has been advanced.

<p align="center">
  <img width="400" height="400" src="assets/overview.svg">
</p>

## Aggregation Framework

µWheels aggregation framework consists of 5 core functions that users must implement:

* ``lift(input) -> MutablePartialAggregate``
    * Lifts input data into a mutable aggregate
* ``combine_mutable(mutable, input)``
    * Combines ⊙ the input data into a mutable aggregate
* ``freeze(mutable) -> PartialAggregate``
    * Freezes the mutable aggregate into an immutable one
* ``combine(a, a) -> PartialAggregate``
    * Combines ⊕ two partial aggregates into a new one
* ``lower(a) -> Aggregate``
    * Lowers a partial aggregate to a final aggregate (e.g., sum/count -> avg)

Why two different combine functions, mutable and immutable? µWheel treats aggregation above the low watermark
differently than below it. Mutable Partial aggregate state above the low watermark can take any form whereas immutable partial aggregates
are of fixed size. For instance, in a Top-N aggregator the mutable state may be a `HashMap<Key, Aggregate>` whereas the immutable state
is a Top-N array of that time unit `[Option<(Key, PartialAggregate)>; N]`.

Down below are some additonal optional functions that users may implement:

* ``combine_simd(slice) -> PartialAggregate``
    * Combines a slice of partial aggregates using explicit SIMD instructions
* ``combine_inverse(a, b) -> PartialAggregate``
    * Deducts a partial aggregate from another
* ``compression()``
    * Defines how partial aggregates can be compressed and decompressed.

µWheel's query optimizer uses these framework provided hints during query planning and execution.

## Writer Wheel

The `WriterWheel` is designed for single-threaded use and high-throughput stream ingestion.
Internally the wheel consists of two wheels: write-ahead and overflow.

### Write-ahead Wheel

The write-ahead wheel is a pre-allocated fixed-sized circular buffer that enables pre-aggregation of
N time units above the low watermark. For instance, assuming the lowest granularity of seconds and a write-ahead capacity of 64
then the wheel supports mutable aggregations up to 64 seconds above the watermark.

µWheel users may configure the write-ahead capacity.

### Overflow Wheel

Events with timestamps that exceed the capacity of the ``write-ahead`` wheel are scheduled into a Overflow wheel (Hierarchical Timing Wheel)
and are inserted once the low watermark has advanced far enough.

## Reader Wheel

The ``ReaderWheel`` indexes complete aggregates hierarchically across multiple time dimensions in a data structure called Hierarchical Aggregate Wheel (HAW).
Internally the Hierarchical Aggregate Wheel consists of multiple event-time indexed wheels,
each maintaining aggregates across a different time granularity. Each wheel may be configured with different data layouts and different retention policies.
HAW is equipped with a wheel-based query optimizer that uses a hybrid cost and heursistics based approach.

### Aggregate Scheme

The default aggregate scheme used is the following:

* Seconds: 60
* Minutes: 60
* Hours: 24
* Days: 7
* Weeks: 52
* Years: 10

Note that each wheel (time dimension) may be configured to hold more than just the latest N slots.

### Data Layout

An aggregate wheel supports three possible data layouts that are configurable per wheel granularity:

* ``Deque``: A regular deque of partial aggregates.
* ``PrefixDeque``: A prefix-enabled deque which requires double the space but runs queries in O(1).
* ``CompressedDeque``: A compressed deque which compresses wheel slots into chunks.

### Queries

µWheel supports the following queries:

* ``window(window)``
    * Installs a streaming window aggregation query.
    * µWheel supports Tumbling, Sliding, and Session Windows.
* ``combine_range(start, end) -> Aggregate``
    * Combines the time range into a final aggregate.
* ``interval(duration) -> Aggregate``
    * Combines the range ``start: watermark() - duration, end: watermark()``
* ``range(start, end) -> Vec<(TimeStamp, Aggregate)>``
    * Returns aggregates and their timestamps in the given time range.
* ``group_by(range, interval) -> Vec<(TimeStamp, Aggregate)>``
    * Groups aggregates between the range by the interval.
* ``landmark() -> Aggregate``
    * Executes a Landmark Window to get a aggregate result across the full hierarchical wheel.
    * Runs in O(1) since it combines the total rotation aggregate of each wheel (dimension).

### Query Optimizer

µWheel's query optimizer leverages framework-provided hints such as SIMD compatibility and the invertibility property.
If explicit SIMD support is available then the query planner will favour plans where it can be fully exploited.
On the other hand if vectorized execution is not possible then the optimizer will prioritize reducing the number of aggregate operations instead.

The image below illustrates a flow chart of the query optimizer which aims to select the optimal execution plan.

<p align="center">
  <img src="assets/query_optimizer_flow_chart.svg">
</p>

## Streaming Window Aggregation

µWheel supports ``Tumbling``, ``Sliding``, and ``Session`` windows. For Tumbling and Sliding, µWheel uses the Pairs stream slicing technique internally and if a window is installed it continously build pairs. Pairs are managed internally by a circular-based window aggregator that implements the following functions:

- ``push(p: Pair)`` Pushes a pair into the back.
- ``compute()`` Computes the current window.
- ``pop()`` Removes the oldest pair from the front.

Window results are generated when [time is advanced](#Advancing-Time) in µWheel.

## Advancing Time

Aggregate Synchronization in µWheel refers to the advancement of time, a process which shifts aggregates from the writer wheel to the reader.
µWheel only performs synchronization lazily once its low watermark is advanced. This design choice has a twofold purpose.
First, it enables µWheel to avoid costly index maintenance (e.g., tree rebalance) in the hot path of writes.
Secondly, it creates a clear seperation between writes and reads, enabling concurrent ingestion and querying.

Internally, the advancement of time causes wheels in µWheel to `tick`. This includes both ticking the [WriterWheel](#Writer-Wheel) and the [ReaderWheel](#Reader-Wheel).
For the ReaderWheel, a tick may at best cause a single tick in the lowest granularity wheel (seconds). However, in worst case a tick may cause a full roll-up rotation across all wheels
in the Hierarchical Aggregate Wheel.
