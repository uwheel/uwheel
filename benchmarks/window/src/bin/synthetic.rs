use awheel::{
    aggregator::U64SumAggregator,
    time::Duration,
    window::{eager, eager_window_query_cost, lazy, lazy_window_query_cost, WindowWheel},
    Entry,
};
use clap::Parser;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};
use window::{fiba_wheel, TimestampGenerator};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1000)]
    windows: u64,
    #[clap(short, long, value_parser, default_value_t = 10000)]
    events_per_sec: u64,
    #[clap(short, long, value_parser, default_value_t = 5)]
    max_distance: u64,
    #[clap(short, long, value_parser, default_value_t = 30)]
    range: u64,
    #[clap(short, long, value_parser, default_value_t = 10)]
    slide: u64,
    #[clap(short, long, value_parser, default_value_t = 10)]
    ooo_degree: u64,
}

// calculate how many seconds are required to trigger N number of windows with a RANGE and SLIDE.
fn number_of_seconds(windows: u64, range: u64, slide: u64) -> u64 {
    (windows - 1) * slide + range
}

fn main() {
    let args = Args::parse();
    println!("Running with {:#?}", args);
    let range = Duration::seconds(args.range as i64);
    let slide = Duration::seconds(args.slide as i64);
    let seconds = number_of_seconds(
        args.windows,
        range.whole_seconds() as u64,
        slide.whole_seconds() as u64,
    );
    let inserts = seconds * args.events_per_sec;
    dbg!((seconds, inserts));
    dbg!(lazy_window_query_cost(range, slide));
    dbg!(eager_window_query_cost(range, slide));

    let lazy_wheel: lazy::LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
        .with_range(range)
        .with_slide(slide)
        .build();

    run("Lazy Wheel SUM", seconds, lazy_wheel, &args);

    let eager_wheel: eager::EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
        .with_range(range)
        .with_slide(slide)
        .build();

    let gate = Arc::new(AtomicBool::new(true));
    let read_wheel = eager_wheel.wheel().clone();
    let inner_gate = gate.clone();
    let handle = std::thread::spawn(move || {
        let now = Instant::now();
        let mut counter = 0;
        while inner_gate.load(Ordering::Relaxed) {
            // Execute queries on random granularities
            let pick = fastrand::usize(0..3);
            if pick == 0 {
                let _res = std::hint::black_box(
                    read_wheel.interval(Duration::seconds(fastrand::i64(1..60))),
                );
            } else if pick == 1 {
                let _res = std::hint::black_box(
                    read_wheel.interval(Duration::minutes(fastrand::i64(1..60))),
                );
            } else {
                let _res = std::hint::black_box(
                    read_wheel.interval(Duration::hours(fastrand::i64(1..24))),
                );
            }
            counter += 1;
        }
        println!(
            "Concurrent Read task ran at {} Mops/s",
            (counter as f64 / now.elapsed().as_secs_f64()) as u64 / 1_000_000
        );
    });
    run("Eager Wheel SUM", seconds, eager_wheel, &args);
    gate.store(false, Ordering::Relaxed);
    handle.join().unwrap();

    /*
    let cg_bfinger_two_wheel = fiba_wheel::BFingerTwoWheel::new(0, range, slide);
    run(
        "FiBA CG Bfinger2 Wheel SUM",
        seconds,
        cg_bfinger_two_wheel,
        &args,
    );

    let cg_bfinger_four_wheel = fiba_wheel::BFingerFourWheel::new(0, range, slide);
    run(
        "FiBA CG Bfinger4 Wheel SUM",
        seconds,
        cg_bfinger_four_wheel,
        &args,
    );
    */

    let cg_bfinger_eight_wheel = fiba_wheel::BFingerEightWheel::new(0, range, slide);
    run(
        "FiBA CG Bfinger8 Wheel SUM",
        seconds,
        cg_bfinger_eight_wheel,
        &args,
    );
    //let fiba_pairs_wheel = fiba_wheel::PairsFiBA::new(0, range, slide);
    //run("FiBA Pairs Wheel SUM", seconds, fiba_pairs_wheel, &args);
}

fn run(id: &str, seconds: u64, mut window: impl WindowWheel<U64SumAggregator>, args: &Args) {
    let Args {
        events_per_sec,
        windows: _,
        max_distance,
        range: _,
        slide: _,
        ooo_degree,
    } = *args;
    let mut ts_generator =
        TimestampGenerator::new(0, Duration::seconds(max_distance as i64), ooo_degree as f32);
    let full = Instant::now();
    for _i in 0..seconds {
        for _i in 0..events_per_sec {
            window
                .insert(Entry::new(1, ts_generator.timestamp()))
                .unwrap();
        }
        ts_generator.update_watermark(ts_generator.watermark() + 1000);

        // advance window wheel
        for (_timestamp, _result) in window.advance_to(ts_generator.watermark()) {
            //println!("Window at {} with data {:?}", _timestamp, _result);
        }
    }
    let runtime = full.elapsed();
    println!("{} (took {:.2}s)", id, runtime.as_secs_f64(),);
    window.print_stats();
}
