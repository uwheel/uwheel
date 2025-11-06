#![cfg(feature = "roaring")]

use std::sync::Arc;

use criterion::{
    BatchSize,
    Bencher,
    Criterion,
    Throughput,
    black_box,
    criterion_group,
    criterion_main,
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use uwheel::{Entry, RwWheel, WheelRange, aggregator::roaring::RoaringAggregator32};

const INSERT_EVENT_COUNT: usize = 10_000;
const FILTER_EVENT_COUNT: usize = 60_000;
const QUERY_COUNT: usize = 1_024;
const VALUE_DOMAIN: u32 = 4_096;
const TIMESTAMP_STEP_MS: u64 = 1_000;
const RANGE_WIDTH_MS: u64 = 30_000;

#[derive(Clone, Copy)]
struct Event {
    value: u32,
    timestamp_ms: u64,
}

#[derive(Clone, Copy)]
struct Query {
    start_ms: u64,
    end_ms: u64,
    value: u32,
    expected: bool,
}

fn roaring_benchmarks(c: &mut Criterion) {
    let mut insert_group = c.benchmark_group("roaring-insert");
    insert_group.throughput(Throughput::Elements(INSERT_EVENT_COUNT as u64));
    insert_group.bench_function("sequential-timestamps", roaring_insert);
    insert_group.finish();

    let mut filter_group = c.benchmark_group("roaring-temporal-filter");
    filter_group.throughput(Throughput::Elements(QUERY_COUNT as u64));
    filter_group.bench_function("contains-id", roaring_temporal_filter);
    filter_group.finish();
}

fn roaring_insert(bencher: &mut Bencher) {
    let events = Arc::new(generate_events(INSERT_EVENT_COUNT));
    bencher.iter_batched(
        || (RwWheel::<RoaringAggregator32>::new(0), events.clone()),
        |(mut wheel, events)| {
            for event in events.iter() {
                wheel.insert(Entry::new(
                    black_box(event.value),
                    black_box(event.timestamp_ms),
                ));
            }
            wheel
        },
        BatchSize::SmallInput,
    );
}

fn roaring_temporal_filter(bencher: &mut Bencher) {
    let events = generate_events(FILTER_EVENT_COUNT);
    let wheel = populate_wheel(&events, RANGE_WIDTH_MS);
    let queries = generate_queries(&wheel, &events, QUERY_COUNT, RANGE_WIDTH_MS);

    let mut index = 0usize;
    bencher.iter(|| {
        let query = &queries[index];
        index += 1;
        if index == queries.len() {
            index = 0;
        }
        let result = check_id_in_range(&wheel, query.start_ms, query.end_ms, query.value);
        assert_eq!(result, query.expected);
        black_box(result);
    });
}

fn generate_events(count: usize) -> Vec<Event> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..count)
        .map(|idx| Event {
            value: rng.gen_range(0..VALUE_DOMAIN),
            timestamp_ms: idx as u64 * TIMESTAMP_STEP_MS,
        })
        .collect()
}

fn populate_wheel(events: &[Event], padding_ms: u64) -> RwWheel<RoaringAggregator32> {
    let mut wheel = RwWheel::<RoaringAggregator32>::new(0);
    for event in events {
        wheel.insert(Entry::new(event.value, event.timestamp_ms));
    }
    if let Some(last) = events.last() {
        let _ = wheel.advance_to(last.timestamp_ms + padding_ms);
    }
    wheel
}

fn generate_queries(
    wheel: &RwWheel<RoaringAggregator32>,
    events: &[Event],
    count: usize,
    range_width_ms: u64,
) -> Vec<Query> {
    let mut rng = StdRng::seed_from_u64(1_337);
    let mut queries = Vec::with_capacity(count);

    for _ in 0..count {
        let event = &events[rng.gen_range(0..events.len())];
        let start = event.timestamp_ms;
        let end = start + range_width_ms;
        let value = if rng.gen_bool(0.5) {
            event.value
        } else {
            rng.gen_range(0..VALUE_DOMAIN)
        };
        let expected = check_id_in_range(wheel, start, end, value);
        queries.push(Query {
            start_ms: start,
            end_ms: end,
            value,
            expected,
        });
    }

    queries
}

fn check_id_in_range(
    wheel: &RwWheel<RoaringAggregator32>,
    start_ms: u64,
    end_ms: u64,
    value: u32,
) -> bool {
    wheel
        .read()
        .combine_range(WheelRange::new_unchecked(start_ms, end_ms))
        .map_or(false, |partial| partial.contains(value))
}

criterion_group!(benches, roaring_benchmarks);
criterion_main!(benches);
