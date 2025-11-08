#[cfg(feature = "bloom")]
use criterion::{criterion_group, criterion_main};

#[cfg(feature = "bloom")]
mod bloom_impl {
    use std::sync::Arc;

    use criterion::{BatchSize, Bencher, Criterion, Throughput, black_box};
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use uwheel::{Entry, RwWheel, WheelRange, aggregator::bloom::BloomAggregator};

    type BenchmarkBloomAggregator = BloomAggregator<u32>;

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

    pub(super) fn bloom_benchmarks(c: &mut Criterion) {
        let mut insert_group = c.benchmark_group("bloom-insert");
        insert_group.throughput(Throughput::Elements(INSERT_EVENT_COUNT as u64));
        insert_group.bench_function("sequential-timestamps", bloom_insert);
        insert_group.finish();

        let mut filter_group = c.benchmark_group("bloom-temporal-filter");
        filter_group.throughput(Throughput::Elements(QUERY_COUNT as u64));
        filter_group.bench_function("contains-id", bloom_temporal_filter);
        filter_group.finish();
    }

    fn bloom_insert(bencher: &mut Bencher) {
        let events = Arc::new(generate_events(INSERT_EVENT_COUNT));
        bencher.iter_batched(
            || (RwWheel::<BenchmarkBloomAggregator>::new(0), events.clone()),
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

    fn bloom_temporal_filter(bencher: &mut Bencher) {
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

    fn populate_wheel(events: &[Event], padding_ms: u64) -> RwWheel<BenchmarkBloomAggregator> {
        let mut wheel = RwWheel::<BenchmarkBloomAggregator>::new(0);
        for event in events {
            wheel.insert(Entry::new(event.value, event.timestamp_ms));
        }
        if let Some(last) = events.last() {
            let _ = wheel.advance_to(last.timestamp_ms + padding_ms);
        }
        wheel
    }

    fn generate_queries(
        wheel: &RwWheel<BenchmarkBloomAggregator>,
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
        wheel: &RwWheel<BenchmarkBloomAggregator>,
        start_ms: u64,
        end_ms: u64,
        value: u32,
    ) -> bool {
        wheel
            .read()
            .combine_range(WheelRange::new_unchecked(start_ms, end_ms))
            .is_some_and(|partial| partial.contains(&value))
    }
}

#[cfg(feature = "bloom")]
use bloom_impl::bloom_benchmarks;

#[cfg(feature = "bloom")]
criterion_group!(benches, bloom_benchmarks);

#[cfg(feature = "bloom")]
criterion_main!(benches);

#[cfg(not(feature = "bloom"))]
fn main() {
    panic!("Enable the `bloom` feature to run this benchmark.");
}
