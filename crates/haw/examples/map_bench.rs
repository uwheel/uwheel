use clap::{ArgEnum, Parser};
use rand::distributions::Distribution;
use std::{
    alloc::{Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
};
use zipf::ZipfDistribution;

use haw::{aggregator::U64SumAggregator, map::WheelMap, Entry};

#[global_allocator]
static ALLOCATOR: Alloc = Alloc;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static FREED: AtomicUsize = AtomicUsize::new(0);
static RESIDENT: AtomicUsize = AtomicUsize::new(0);

fn allocated() -> usize {
    ALLOCATED.swap(0, Ordering::Relaxed) / 1_000_000
}

fn freed() -> usize {
    FREED.swap(0, Ordering::Relaxed) / 1_000_000
}

fn resident() -> usize {
    RESIDENT.load(Ordering::Relaxed) / 1_000_000
}

#[derive(Default, Debug, Clone, Copy)]
struct Alloc;

unsafe impl std::alloc::GlobalAlloc for Alloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        assert_ne!(ret, std::ptr::null_mut());
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        RESIDENT.fetch_add(layout.size(), Ordering::Relaxed);
        std::ptr::write_bytes(ret, 0xa1, layout.size());
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        std::ptr::write_bytes(ptr, 0xde, layout.size());
        FREED.fetch_add(layout.size(), Ordering::Relaxed);
        RESIDENT.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 10000)]
    keys: usize,
    #[clap(short, long, value_parser, default_value_t = 100000)]
    runs: usize,
    #[clap(arg_enum, value_parser, default_value_t = DistributionMode::Zipf)]
    mode: DistributionMode,
}

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
enum DistributionMode {
    Zipf,
    Sequential,
    Random,
}

fn main() {
    let args = Args::parse();
    let total_random_keys = args.keys;
    let runs = args.runs;
    use rand::Rng;
    let mut rng = rand::thread_rng();

    println!("Running with {:#?}", args);
    println!(
        "Total Keys: {} with {} bytes per key",
        total_random_keys,
        std::mem::size_of::<u64>()
    );
    println!(
        "Wheel Size (No Alloc): {}",
        std::mem::size_of::<haw::Wheel<U64SumAggregator>>()
    );

    let keys: Vec<u64> = match args.mode {
        DistributionMode::Zipf => {
            let zipf = ZipfDistribution::new(total_random_keys, 0.99).unwrap();
            (0..runs).map(|_| zipf.sample(&mut rng) as u64).collect()
        }
        DistributionMode::Random => (0..runs)
            .map(|_| rng.gen_range(0..total_random_keys) as u64)
            .collect(),
        DistributionMode::Sequential => (0..runs as u64).collect(),
    };

    const VALUE: u64 = 1;
    const TIMESTAMP: u64 = 1000;

    let mut map: WheelMap<u64, U64SumAggregator> = WheelMap::new(0);

    let before_writes = std::time::Instant::now();
    for k in keys.iter() {
        assert!(map.insert(*k, Entry::new(VALUE, TIMESTAMP)).is_ok());
        if (k + 1) % (total_random_keys as u64 / 10) == 0 {
            println!(
                "{:.2} million wps {} mb allocated {} mb freed {} mb resident",
                *k as f64 / (before_writes.elapsed().as_micros().max(1)) as f64,
                allocated(),
                freed(),
                resident(),
            )
        }
    }
    let elapsed = before_writes.elapsed();
    dbg!(elapsed);
    println!(
        "ingestion ran at {} ops/s (took {:?})",
        (runs as f64 / elapsed.as_secs_f64()),
        elapsed,
    );

    let before_reads = std::time::Instant::now();
    for k in keys {
        assert_eq!(map.get(&k).unwrap().seconds_unchecked().interval(30), None);
        if (k + 1) % (total_random_keys as u64 / 10) == 0 {
            println!(
                "{:.2} million rps {} mb allocated {} mb freed {} mb resident",
                k as f64 / (before_reads.elapsed().as_micros().max(1)) as f64,
                allocated(),
                freed(),
                resident(),
            )
        }
    }
    dbg!(before_reads.elapsed());
}
