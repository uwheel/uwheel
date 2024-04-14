# µWheel Design Doc

This doc describes the design of µWheel.

# Overview

µWheel is an event-driven aggregate management system for the ingestion, querying, and indexing of stream aggregates.

The design of µWheel is centered around low watermarking, a concept found in modern streaming systems (e.g., Apache Flink, RisingWave).
µWheel takes advantage of the low watermark and seperates the write and read paths.

Writes are handled by a `WriterWheel` which is optimized for single-threaded ingestion of out-of-order aggregates.
Reads are managed by a hierarchically indexed `ReaderWheel` that employs a wheel-based query optimizer whose cost function
tries to minimize the number of aggregate operations for a query.

µWheel adopts a lazy synchronization approach. Aggregates are only shifted over from the `WriterWheel` to the `ReaderWheel`
once the internal low watermark has been advanced.

<img src="assets/overview.svg">

## Aggregation Framework

µWheels aggregation framework consists of 5 core functions that users must implement:

* ``lift(input) -> MutablePartialAggregate``
    * Lifts input data into a mutable aggregate
* ``combine_mutable(mutable, input)``
    * Combines ⊙ the input data into a mutable aggregate
* ``freeze(mutable) -> PartialAggregate``
    * Freezes the mutable aggregate into an immutable one
* ``combine(a, a) -> a``
    * Combines ⊕ two partial aggregates into a new one
* ``lower(a) -> Aggregate``
    * Lowers a partial aggregate to a final aggregate (e.g., sum/count -> avg)

`lift` and `combine_mutable` are used at the Writer Wheel for mutable pre-aggregation whereas the remaining functions focus
on immutable operations at the Reader Wheel.

## Writer Wheel

The `WriterWheel` consists of two wheels internally, a write-ahead and overflow.

## Reader Wheel


### Query Optimizer

<img src="assets/query_optimizer_flow_chart.svg">
