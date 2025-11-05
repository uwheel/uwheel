use uwheel::{
    Aggregator,
    Entry,
    NumericalDuration,
    RwWheel,
    Window,
    aggregator::roaring::RoaringAggregator32,
};

fn main() {
    #[derive(Clone, Debug)]
    struct SensorEvent {
        timestamp_ms: u64,
        value: u32,
    }

    const THRESHOLD: u32 = 70;

    let events = vec![
        SensorEvent {
            timestamp_ms: 1_000,
            value: 65,
        },
        SensorEvent {
            timestamp_ms: 2_000,
            value: 72,
        },
        SensorEvent {
            timestamp_ms: 3_000,
            value: 88,
        },
        SensorEvent {
            timestamp_ms: 6_000,
            value: 63,
        },
        SensorEvent {
            timestamp_ms: 7_000,
            value: 95,
        },
        SensorEvent {
            timestamp_ms: 9_000,
            value: 77,
        },
    ];

    let mut wheel: RwWheel<RoaringAggregator32> = RwWheel::new(0);
    wheel.window(Window::tumbling(4.seconds()));

    // Insert buckets for events that cross the threshold.
    for event in &events {
        if event.value >= THRESHOLD {
            let bucket = time_bucket(event.timestamp_ms);
            wheel.insert(Entry::new(bucket, event.timestamp_ms));
        }
    }

    let aggregates = wheel.advance_to(12_000);

    for window in aggregates {
        let bitmap = RoaringAggregator32::lower(window.aggregate.clone());
        let matches: Vec<_> = events
            .iter()
            .filter(|event| {
                let ts = event.timestamp_ms;
                ts >= window.window_start_ms
                    && ts < window.window_end_ms
                    && bitmap.contains(time_bucket(ts))
            })
            .collect();

        println!(
            "Window [{}, {}) high-value buckets: {:?}",
            window.window_start_ms,
            window.window_end_ms,
            bitmap.iter().collect::<Vec<_>>()
        );

        println!("  Matches:");
        for event in matches {
            println!("    ts={}ms value={}", event.timestamp_ms, event.value);
        }
    }
}

fn time_bucket(timestamp_ms: u64) -> u32 {
    (timestamp_ms / 1_000) as u32
}
