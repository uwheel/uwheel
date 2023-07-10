use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use clap::{ArgEnum, Parser};
use duckdb::Result;
use haw::{aggregator::AllAggregator, time, Entry, RwWheel};
use minstant::Instant;
use olap::*;
use sketches_ddsketch::{Config, DDSketch};

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Workload {
    ReadOnly,
    ReadWrite,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1)]
    threads: usize,
    #[clap(short, long, value_parser, default_value_t = 10_000)]
    batch_size: usize,
    #[clap(short, long, value_parser, default_value_t = 100)]
    events_per_min: usize,
    #[clap(short, long, value_parser, default_value_t = 1000000)]
    queries: usize,
    #[clap(short, long, value_parser, default_value_t = 100000)]
    advances: usize,
    #[clap(short, long, value_parser, default_value_t = 1)]
    advance_step: usize,
    #[clap(short, long, value_parser, default_value_t = 1)]
    advance_freq_seconds: usize,
    #[clap(arg_enum, value_parser, default_value_t = Workload::ReadOnly)]
    workload: Workload,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let events_per_min = args.events_per_min;

    println!("Running with {:#?}", args);
    let (_watermark, batches) = DataGenerator::generate_query_data(events_per_min);

    let mut wheel: RwWheel<AllAggregator> = RwWheel::new(0);
    for batch in batches {
        for record in batch {
            wheel
                .write()
                .insert(Entry::new(record.fare_amount, record.do_time))
                .unwrap();
        }
        use haw::time::NumericalDuration;
        wheel.advance(60.seconds());
    }
    println!("Finished preparing HAW");

    let read_wheel = wheel.read().clone();

    let mut handles = vec![];
    let now = Instant::now();
    let gate = Arc::new(AtomicBool::new(true));

    for _ in 0..args.threads {
        let read_wheel = read_wheel.clone();
        let inner_gate = gate.clone();
        let handle = std::thread::spawn(move || {
            let mut sketch = DDSketch::new(Config::new(0.01, 2048, 1.0e-9));
            while inner_gate.load(Ordering::Relaxed) {
                let query = QueryInterval::generate_random();
                let now = Instant::now();
                let _res = match query {
                    QueryInterval::Seconds(secs) => {
                        read_wheel.interval(time::Duration::seconds(secs as i64))
                    }
                    QueryInterval::Minutes(mins) => {
                        read_wheel.interval(time::Duration::minutes(mins as i64))
                    }
                    QueryInterval::Hours(hours) => {
                        read_wheel.interval(time::Duration::hours(hours as i64))
                    }
                    QueryInterval::Days(days) => {
                        read_wheel.interval(time::Duration::days(days as i64))
                    }
                    QueryInterval::Landmark => read_wheel.landmark(),
                };
                sketch.add(now.elapsed().as_nanos() as f64);
            }
            sketch
        });

        handles.push(handle);
    }

    let workload = args.workload;
    match &workload {
        Workload::ReadOnly => {
            // just sleep a bit
            std::thread::sleep(Duration::from_secs(20));
        }
        Workload::ReadWrite => {
            let write_now = Instant::now();
            for _i in 0..args.advances {
                let wm = wheel.watermark();
                wheel.write().insert(Entry::new(1.0, wm + 1)).unwrap();
                wheel.advance(time::Duration::seconds(args.advance_step as i64));
            }
            let runtime = write_now.elapsed();
            println!(
                "ran with {} advance Mops/s (took {:.2}s)",
                (args.advances as f64 / runtime.as_secs_f64()) as u64 / 1_000_000,
                runtime.as_secs_f64(),
            );
        }
    }

    gate.store(false, Ordering::Relaxed);

    let skecthes: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    let runtime = now.elapsed();

    let mut sketch = DDSketch::new(Config::new(0.01, 2048, 1.0e-9));
    for s in skecthes {
        sketch.merge(&s).unwrap();
    }
    let percentiles = haw::stats::sketch_percentiles(&sketch);
    let total_queries = sketch.count();
    println!("{:#?}", percentiles);

    println!(
        "Executed {} queries and ran with {} Mops/s (took {:.2}s)",
        total_queries,
        (total_queries as f64 / runtime.as_secs_f64()) as u64 / 1_000_000,
        runtime.as_secs_f64(),
    );
    Ok(())
}
