use super::{
    util::{create_pair_type, pairs_capacity, pairs_space, PairType},
    WindowWheel,
};
use crate::{
    aggregator::{Aggregator, InverseExt},
    time::{Duration, NumericalDuration},
    wheels::{aggregation::combine_or_insert, WheelExt},
    Entry,
    Error,
    Wheel,
};

use crate::wheels::aggregation::iter::Iter;
use core::iter::Iterator;

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

#[cfg(feature = "stats")]
use crate::stats::Measure;

#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Clone)]
pub struct PairsWheel<A: Aggregator> {
    capacity: usize,
    aggregator: A,
    slots: Box<[Option<A::PartialAggregate>]>,
    tail: usize,
    head: usize,
}

impl<A: Aggregator> PairsWheel<A> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert_capacity!(capacity);
        Self {
            capacity,
            aggregator: Default::default(),
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
        // TODO: sanity check of range and slide
        LazyWindowWheel::new(self.time, self.range, self.slide)
    }
}

/// A window wheel that uses the Pairs technique to store partial aggregates
/// for a RANGE r and SLIDE s. It utilises a regular HAW for insertions and populating
/// window slices.
#[allow(dead_code)]
pub struct LazyWindowWheel<A: Aggregator> {
    range: usize,
    slide: usize,
    pair_ticks_remaining: usize,
    current_pair_len: usize,
    pair_type: PairType,
    pairs_wheel: PairsWheel<A>,
    wheel: Wheel<A>,
    // When the next window starts
    next_window_start: u64,
    // When the next window ends
    next_window_end: u64,
    next_pair_start: u64,
    next_pair_end: u64,
    in_p1: bool,
    #[cfg(feature = "stats")]
    stats: super::stats::Stats,
}

impl<A: Aggregator> LazyWindowWheel<A> {
    pub fn new(time: u64, range: usize, slide: usize) -> Self {
        let pair_type = create_pair_type(range, slide);
        let next_window_start = time + slide as u64;
        let current_pair_len = match pair_type {
            PairType::Even(slide) => slide,
            PairType::Uneven(_, p2) => p2,
        };
        let next_pair_start = 0;
        let next_pair_end = time + current_pair_len as u64;
        Self {
            range,
            slide,
            current_pair_len,
            pair_ticks_remaining: current_pair_len / 1000,
            pair_type,
            pairs_wheel: PairsWheel::with_capacity(pairs_capacity(range, slide)),
            wheel: Wheel::new(time),
            next_window_start,
            next_window_end: time + range as u64,
            next_pair_start,
            next_pair_end,
            in_p1: false,
            #[cfg(feature = "stats")]
            stats: Default::default(),
        }
    }
    fn current_pair_duration(&self) -> Duration {
        Duration::milliseconds(self.current_pair_len as i64)
    }
    fn update_pair_len(&mut self) {
        if let PairType::Uneven(p1, p2) = self.pair_type {
            if self.in_p1 {
                self.current_pair_len = p2;
                self.in_p1 = false;
            } else {
                self.current_pair_len = p1;
                self.in_p1 = true;
            }
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

impl<A: Aggregator> WindowWheel<A> for LazyWindowWheel<A> {
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)> {
        let ticks = duration.whole_seconds();
        let mut window_results = Vec::new();
        for _tick in 0..ticks {
            self.wheel.advance(1.seconds());
            self.pair_ticks_remaining -= 1;

            if self.pair_ticks_remaining == 0 {
                // pair ended
                let partial = self
                    .wheel
                    .interval(self.current_pair_duration())
                    .unwrap_or_default();

                self.pairs_wheel.push(partial);

                // Update pair metadata
                self.update_pair_len();

                self.next_pair_end = self.wheel.watermark() + self.current_pair_len as u64;
                self.pair_ticks_remaining = self.current_pair_duration().whole_seconds() as usize;

                if self.wheel.watermark() == self.next_window_end {
                    // Window computation:
                    let window = self.compute_window();

                    window_results.push((self.wheel.watermark(), Some(A::lower(window))));

                    #[cfg(feature = "stats")]
                    let _measure = Measure::new(&self.stats.cleanup_ns);

                    // how many "pairs" we need to pop off from the Pairs wheel
                    let removals = match self.pair_type {
                        PairType::Even(_) => 1,
                        PairType::Uneven(_, _) => 2,
                    };
                    for _i in 0..removals {
                        let _ = self.pairs_wheel.tick();
                    }

                    // next window ends at next slide (p1+p2)
                    self.next_window_end += self.slide as u64;
                }
            }
        }
        window_results
    }
    fn advance_to(&mut self, watermark: u64) -> Vec<(u64, Option<A::Aggregate>)> {
        let diff = watermark.saturating_sub(self.wheel.watermark());
        #[cfg(feature = "stats")]
        let _measure = Measure::new(&self.stats.advance_ns);
        self.advance(Duration::milliseconds(diff as i64))
    }
    #[inline]
    fn insert(&mut self, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        #[cfg(feature = "stats")]
        let _measure = Measure::new(&self.stats.insert_ns);
        self.wheel.insert(entry)
    }
    /// Returns a reference to the underlying HAW
    fn wheel(&self) -> &Wheel<A> {
        &self.wheel
    }
    #[cfg(feature = "stats")]
    fn print_stats(&self) {
        println!("{:#?}", self.stats);
    }
}
