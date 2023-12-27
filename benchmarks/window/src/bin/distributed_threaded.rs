#![allow(unused_imports)]
#![allow(dead_code)]

use awheel::{
    aggregator::sum::U64SumAggregator,
    delta::DeltaState,
    time_internal::{Duration, NumericalDuration},
    window::distributed::{DistributedWindow, DistributedWindowExt},
    Entry,
    RwWheel,
};
use clap::Parser;
use minstant::Instant;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 1)]
    threads: usize,
    #[clap(short, long, value_parser, default_value_t = 1000)]
    windows: u64,
    #[clap(short, long, value_parser, default_value_t = 10000)]
    events_per_sec: usize,
}

// calculate how many seconds are required to trigger N number of windows with a RANGE and SLIDE.
fn number_of_seconds(windows: u64, range: u64, slide: u64) -> u64 {
    (windows - 1) * slide + range
}

fn main() {
    let args = Args::parse();
    println!("Running with {:#?}", args);
    let events_per_sec = args.events_per_sec;
    let workers = args.threads;
    let range = Duration::seconds(30);
    let slide = Duration::seconds(10);
    let seconds = number_of_seconds(
        args.windows,
        range.whole_seconds() as u64,
        slide.whole_seconds() as u64,
    ) as usize;
    let (tx, _rx) = flume::unbounded();
    let total_events = (events_per_sec * seconds) * workers;

    let merger_handle = std::thread::spawn(move || {
        let merger: Merger<DistributedWindow<U64SumAggregator>> = Merger::new(
            total_events,
            _rx,
            DistributedWindow::new(
                0,
                range.whole_milliseconds() as usize,
                slide.whole_milliseconds() as usize,
                workers,
            ),
        );
        merger.run();
    });

    let mut worker_handles = Vec::with_capacity(workers);
    for id in 0..workers {
        let tx_c = tx.clone();
        let handle = std::thread::spawn(move || {
            let worker = Worker::new(id, seconds, events_per_sec, tx_c);
            worker.run()
        });
        worker_handles.push(handle);
    }

    // drop the tx in our main function not the one(s) in workers
    drop(tx);

    let _collected_handles: Vec<_> = worker_handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();

    merger_handle.join().unwrap();
}

#[allow(dead_code)]
pub struct Worker {
    id: usize,
    seconds: usize,
    events_per_sec: usize,
    wheel: RwWheel<U64SumAggregator>,
    sender: flume::Sender<(usize, DeltaState<u64>)>,
}

impl Worker {
    pub fn new(
        id: usize,
        seconds: usize,
        events_per_sec: usize,
        sender: flume::Sender<(usize, DeltaState<u64>)>,
    ) -> Self {
        Self {
            id,
            seconds,
            events_per_sec,
            wheel: RwWheel::new(0),
            sender,
        }
    }

    pub fn run(mut self) {
        for _i in 0..self.seconds {
            let watermark = self.wheel.watermark();
            for _ in 0..self.events_per_sec {
                // TODO: fastrand value
                self.wheel
                    .insert(Entry::new(fastrand::u64(1..10000), watermark));
            }
            let delta = self.wheel.advance_and_emit_deltas(1.seconds());
            self.sender.send((self.id, delta)).unwrap();
        }
        println!("Finished worker {}", self.id);
    }
}

// Window Merger
pub struct Merger<D: DistributedWindowExt<U64SumAggregator>> {
    total_events: usize,
    start: Option<Instant>,
    receiver: flume::Receiver<(usize, DeltaState<u64>)>,
    dw: D,
}

impl<D: DistributedWindowExt<U64SumAggregator>> Merger<D> {
    pub fn new(
        total_events: usize,
        receiver: flume::Receiver<(usize, DeltaState<u64>)>,
        dw: D,
    ) -> Self {
        Self {
            total_events,
            start: None,
            receiver,
            dw,
        }
    }
    pub fn run(mut self) {
        self.start = Some(Instant::now());
        // For each worker merge deltas into DistributedWindow
        for (worker, delta) in self.receiver.iter() {
            for window in self.dw.merge_deltas(worker as u64, delta) {
                dbg!(window);
            }
        }
        let elapsed = self.start.unwrap().elapsed();
        println!(
            "ran with {} Mevent/s (took {:.2}s)",
            (self.total_events as f64 / elapsed.as_secs_f64()) as u64 / 1_000_000,
            elapsed.as_secs_f64(),
        );
        let stats = self.dw.stats();
        println!("{:#?}", stats);
    }
}
