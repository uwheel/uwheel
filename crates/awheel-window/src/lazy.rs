use crate::{state::State, WindowExt};

use super::util::{pairs_capacity, pairs_space, PairType};
use awheel_core::{
    aggregator::{Aggregator, InverseExt},
    rw_wheel::{
        read::{aggregation::combine_or_insert, ReadWheel},
        WheelExt,
    },
    time::{Duration, NumericalDuration},
    Entry,
    Error,
    RwWheel,
};

use awheel_core::rw_wheel::read::aggregation::iter::Iter;
use core::{iter::Iterator, mem};

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

#[cfg(feature = "stats")]
use awheel_stats::Measure;

#[repr(C)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Clone)]
pub struct PairsWheel<A: Aggregator> {
    capacity: usize,
    slots: Box<[Option<A::PartialAggregate>]>,
    tail: usize,
    head: usize,
}

impl<A: Aggregator> PairsWheel<A> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::assert_capacity(capacity);
        Self {
            capacity,
            slots: (0..capacity)
                .map(|_| None)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            head: 0,
            tail: 0,
        }
    }
    #[inline]
    pub fn tick(&mut self) -> Option<A::PartialAggregate> {
        if !self.is_empty() {
            let tail = self.tail;
            self.tail = self.wrap_add(self.tail, 1);
            self.slot(tail).take()
        } else {
            None
        }
    }

    #[inline]
    pub fn push(&mut self, data: A::PartialAggregate) {
        combine_or_insert::<A>(self.slot(self.head), data);
        self.head = self.wrap_add(self.head, 1);
    }

    #[inline]
    fn slot(&mut self, idx: usize) -> &mut Option<A::PartialAggregate> {
        &mut self.slots[idx]
    }
    /// Combines partial aggregates of the last `subtrahend` slots
    ///
    /// - If given a interval, returns the combined partial aggregate based on that interval,
    ///   or `None` if out of bounds
    #[inline]
    pub fn interval(&self, subtrahend: usize) -> Option<A::PartialAggregate> {
        let tail = self.slot_idx_backward_from_head(subtrahend);
        let iter = Iter::<A>::new(&self.slots, tail, self.head);
        iter.combine()
    }
}

impl<A: Aggregator> WheelExt for PairsWheel<A> {
    fn capacity(&self) -> usize {
        self.capacity
    }
    fn head(&self) -> usize {
        self.head
    }
    fn tail(&self) -> usize {
        self.tail
    }
    fn size_bytes(&self) -> Option<usize> {
        let inner_slots = mem::size_of::<Option<A::PartialAggregate>>() * self.capacity;
        Some(mem::size_of::<Self>() + inner_slots)
    }
}

#[derive(Default, Copy, Clone)]
pub struct Builder {
    range: usize,
    slide: usize,
    time: u64,
}

impl Builder {
    pub fn with_watermark(mut self, watermark: u64) -> Self {
        self.time = watermark;
        self
    }
    pub fn with_range(mut self, range: Duration) -> Self {
        self.range = range.whole_milliseconds() as usize;
        self
    }
    pub fn with_slide(mut self, slide: Duration) -> Self {
        self.slide = slide.whole_milliseconds() as usize;
        self
    }
    pub fn build<A: Aggregator + InverseExt>(self) -> LazyWindowWheel<A> {
        assert!(
            self.range >= self.slide,
            "Range must be larger or equal to slide"
        );
        LazyWindowWheel::new(self.time, self.range, self.slide)
    }
}

/// A window wheel that uses the Pairs technique to store partial aggregates
/// for a RANGE r and SLIDE s. It utilises a regular HAW for insertions and populating
/// window slices.
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
pub struct LazyWindowWheel<A: Aggregator> {
    range: usize,
    slide: usize,
    pairs_wheel: PairsWheel<A>,
    wheel: RwWheel<A>,
    state: State,
    #[cfg(feature = "stats")]
    stats: super::stats::Stats,
}

impl<A: Aggregator> LazyWindowWheel<A> {
    pub fn new(time: u64, range: usize, slide: usize) -> Self {
        let state = State::new(time, range, slide);
        Self {
            range,
            slide,
            pairs_wheel: PairsWheel::with_capacity(pairs_capacity(range, slide)),
            wheel: RwWheel::new(time),
            state,
            #[cfg(feature = "stats")]
            stats: Default::default(),
        }
    }
    // Combines aggregates from the Pairs Wheel in worst-case [2r/s] or best-case [r/s]
    #[inline]
    fn compute_window(&self) -> A::PartialAggregate {
        let pair_slots = pairs_space(self.range, self.slide);
        #[cfg(feature = "stats")]
        let _measure = Measure::new(&self.stats.window_computation_ns);
        self.pairs_wheel.interval(pair_slots).unwrap_or_default()
    }
}

impl<A: Aggregator> WindowExt<A> for LazyWindowWheel<A> {
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)> {
        let ticks = duration.whole_seconds();
        let mut window_results = Vec::new();
        for _tick in 0..ticks {
            self.wheel.advance(1.seconds());
            self.state.pair_ticks_remaining -= 1;

            if self.state.pair_ticks_remaining == 0 {
                // pair ended
                let partial = self
                    .wheel
                    .read()
                    .interval(self.state.current_pair_duration())
                    .unwrap_or_default();

                self.pairs_wheel.push(partial);

                // Update pair metadata
                self.state.update_pair_len();

                self.state.next_pair_end =
                    self.wheel.read().watermark() + self.state.current_pair_len as u64;
                self.state.pair_ticks_remaining =
                    self.state.current_pair_duration().whole_seconds() as usize;

                if self.wheel.read().watermark() == self.state.next_window_end {
                    // Window computation:
                    let window = self.compute_window();

                    window_results.push((self.wheel.read().watermark(), Some(A::lower(window))));

                    #[cfg(feature = "stats")]
                    let _measure = Measure::new(&self.stats.cleanup_ns);

                    // how many "pairs" we need to pop off from the Pairs wheel
                    let removals = match self.state.pair_type {
                        PairType::Even(_) => 1,
                        PairType::Uneven(_, _) => 2,
                    };
                    for _i in 0..removals {
                        let _ = self.pairs_wheel.tick();
                    }

                    // next window ends at next slide (p1+p2)
                    self.state.next_window_end += self.slide as u64;
                }
            }
        }
        window_results
    }
    fn advance_to(&mut self, watermark: u64) -> Vec<(u64, Option<A::Aggregate>)> {
        let diff = watermark.saturating_sub(self.wheel.read().watermark());
        #[cfg(feature = "stats")]
        let _measure = Measure::new(&self.stats.advance_ns);
        self.advance(Duration::milliseconds(diff as i64))
    }
    #[inline]
    fn insert(&mut self, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        #[cfg(feature = "stats")]
        let _measure = Measure::new(&self.stats.insert_ns);
        self.wheel.write().insert(entry)
    }
    /// Returns a reference to the underlying HAW
    fn wheel(&self) -> &ReadWheel<A> {
        self.wheel.read()
    }
    #[cfg(feature = "stats")]
    fn print_stats(&self) {
        let rw_wheel = self.wheel.size_bytes();
        let pairs = self.pairs_wheel.size_bytes().unwrap();
        self.stats.size_bytes.set(rw_wheel + pairs);
        println!("{:#?}", self.stats);
    }
}
