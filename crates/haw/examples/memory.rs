use clap::Parser;
use std::{
    alloc::{Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
};

use haw::{aggregator::U64SumAggregator, time::NumericalDuration, Entry, RwWheel};

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
    #[clap(short, long, value_parser, default_value_t = false)]
    drill_down: bool,
}

fn main() {
    let args = Args::parse();
    println!("Running with {:#?}", args);

    let mut wheel = if args.drill_down {
        RwWheel::<U64SumAggregator>::with_drill_down(0)
    } else {
        RwWheel::<U64SumAggregator>::new(0)
    };

    let ticks = wheel.read().remaining_ticks();
    for _i in 0..ticks {
        let wm = wheel.watermark();
        wheel.write().insert(Entry::new(1u64, wm + 1000)).unwrap();
        wheel.advance(1.seconds());
    }
    assert!(wheel.read().is_full());
    println!(
        "size_of RwWheel<U64SumAggregator> {}",
        std::mem::size_of::<RwWheel<U64SumAggregator>>()
    );
    println!(
        "{} mb allocated {} mb freed {} mb resident",
        allocated(),
        freed(),
        resident(),
    );
}
