use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use uwheel::{aggregator::bloom::BloomAggregator, Entry, RwWheel, WheelRange};

type UserKey = u64;

/// Bloom filter aggregator specialized for hashed user identifiers.
type BloomFilterAggregator = BloomAggregator<UserKey>;

#[derive(Clone, Copy)]
struct BadgeEvent {
    user_id: &'static str,
    timestamp_ms: u64,
}

const WATCH_LIST: [&str; 4] = ["alice", "bob", "carol", "mallory"];

const TIME_WINDOWS: [(&str, u64, u64); 3] = [
    ("Early shift", 0, 10_000),
    ("Lunch rush", 10_000, 20_000),
    ("Closing shift", 20_000, 30_000),
];

const BADGE_EVENTS: [BadgeEvent; 10] = [
    BadgeEvent {
        user_id: "alice",
        timestamp_ms: 1_000,
    },
    BadgeEvent {
        user_id: "bob",
        timestamp_ms: 2_500,
    },
    BadgeEvent {
        user_id: "trent",
        timestamp_ms: 6_000,
    },
    BadgeEvent {
        user_id: "carol",
        timestamp_ms: 11_000,
    },
    BadgeEvent {
        user_id: "bob",
        timestamp_ms: 12_000,
    },
    BadgeEvent {
        user_id: "eve",
        timestamp_ms: 13_500,
    },
    BadgeEvent {
        user_id: "carol",
        timestamp_ms: 18_000,
    },
    BadgeEvent {
        user_id: "alice",
        timestamp_ms: 21_000,
    },
    BadgeEvent {
        user_id: "mallory",
        timestamp_ms: 23_000,
    },
    BadgeEvent {
        user_id: "trent",
        timestamp_ms: 25_000,
    },
];

fn main() {
    println!("\n== Temporal filtering with Bloom aggregators ==\n");

    let mut wheel: RwWheel<BloomFilterAggregator> = RwWheel::new(0);
    for event in BADGE_EVENTS {
        wheel.insert(Entry::new(user_key(event.user_id), event.timestamp_ms));
    }

    if let Some(last) = BADGE_EVENTS.last() {
        let _ = wheel.advance_to(last.timestamp_ms + 1_000);
    }

    for &(label, start, end) in &TIME_WINDOWS {
        report_window(&wheel, label, start, end);
    }

    let suspicious = "mallory";
    let span = (7_500, 22_500);
    let seen = seen_in_range(&wheel, span.0, span.1, suspicious);
    println!(
        "Did {suspicious} badge in the hallway between {}ms and {}ms? {}",
        span.0, span.1, seen
    );
}

fn report_window(wheel: &RwWheel<BloomFilterAggregator>, label: &str, start_ms: u64, end_ms: u64) {
    println!("{label} [{start_ms}ms, {end_ms}ms):");
    match wheel
        .read()
        .combine_range(WheelRange::new_unchecked(start_ms, end_ms))
    {
        Some(partial) => {
            for candidate in WATCH_LIST {
                let seen = partial.contains(&user_key(candidate));
                println!("  contains {candidate:<7}: {seen}");
            }
        }
        None => println!("  no badge activity recorded"),
    }
    println!();
}

// Checks whether this user has been spotted in the time range using the bloom filter
fn seen_in_range(
    wheel: &RwWheel<BloomFilterAggregator>,
    start_ms: u64,
    end_ms: u64,
    user_id: &'static str,
) -> bool {
    wheel
        .read()
        .combine_range(WheelRange::new_unchecked(start_ms, end_ms))
        .is_some_and(|partial| partial.contains(&user_key(user_id)))
}

fn user_key(user_id: &str) -> UserKey {
    let mut hasher = DefaultHasher::new();
    user_id.hash(&mut hasher);
    hasher.finish()
}
