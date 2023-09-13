use crate::{state::State, WindowExt};

use super::util::{pairs_capacity, pairs_space, PairType};
use awheel_core::{
    aggregator::Aggregator,
    rw_wheel::{
        read::{aggregation::combine_or_insert, ReadWheel},
        write::DEFAULT_WRITE_AHEAD_SLOTS,
        WheelExt,
    },
    time::{Duration, NumericalDuration},
    Entry,
    Options,
    RwWheel,
};

use awheel_core::rw_wheel::read::aggregation::iter::Iter;
use core::{iter::Iterator, mem};

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "stats")]
use awheel_stats::profile_scope;

#[doc(hidden)]
#[repr(C)]
#[derive(Debug, Clone)]
pub struct PairsWheel<A: Aggregator> {
    num_slots: usize,
    capacity: usize,
    slots: Box<[Option<A::PartialAggregate>]>,
    tail: usize,
    head: usize,
}

impl<A: Aggregator> PairsWheel<A> {
    pub fn with_capacity(capacity: usize) -> Self {
        let num_slots = awheel_core::capacity_to_slots!(capacity);
        Self {
            num_slots,
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
    pub fn push(&mut self, data_opt: Option<A::PartialAggregate>) {
        if let Some(data) = data_opt {
            combine_or_insert::<A>(self.slot(self.head), data);
        }
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
    fn num_slots(&self) -> usize {
        self.num_slots
    }
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

/// A Builder type for [LazyWindowWheel]
#[derive(Copy, Clone)]
pub struct Builder {
    range: usize,
    slide: usize,
    write_ahead: usize,
    time: u64,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            range: 0,
            slide: 0,
            write_ahead: DEFAULT_WRITE_AHEAD_SLOTS,
            time: 0,
        }
    }
}

impl Builder {
    /// Configures the builder to create a wheel with the given watermark
    pub fn with_watermark(mut self, watermark: u64) -> Self {
        self.time = watermark;
        self
    }
    /// Configures the builder to create a wheel with the given write-ahead capacity
    pub fn with_write_ahead(mut self, write_ahead: usize) -> Self {
        self.write_ahead = write_ahead;
        self
    }
    /// Configures the builder to create a window with the given range
    pub fn with_range(mut self, range: Duration) -> Self {
        self.range = range.whole_milliseconds() as usize;
        self
    }
    /// Configures the builder to create a window with the given slide
    pub fn with_slide(mut self, slide: Duration) -> Self {
        self.slide = slide.whole_milliseconds() as usize;
        self
    }
    /// Consumes the builder and returns a [LazyWindowWheel]
    pub fn build<A: Aggregator>(self) -> LazyWindowWheel<A> {
        assert!(
            self.range >= self.slide,
            "Range must be larger or equal to slide"
        );
        LazyWindowWheel::new(self.time, self.write_ahead, self.range, self.slide)
    }
}

/// A window wheel that uses the Pairs technique to store partial aggregates
/// for a RANGE r and SLIDE s. It utilises a regular HAW for insertions and populating
/// window slices.
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
    fn new(time: u64, write_ahead: usize, range: usize, slide: usize) -> Self {
        let state = State::new(time, range, slide);
        let options = Options::default()
            .with_watermark(time)
            .with_write_ahead(write_ahead);
        Self {
            range,
            slide,
            pairs_wheel: PairsWheel::with_capacity(pairs_capacity(range, slide)),
            wheel: RwWheel::with_options(options),
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
        profile_scope!(&self.stats.window_computation_ns);
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
                    .interval(self.state.current_pair_duration());

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
                    profile_scope!(&self.stats.cleanup_ns);

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
        profile_scope!(&self.stats.advance_ns);
        self.advance(Duration::milliseconds(diff as i64))
    }
    #[inline]
    fn insert(&mut self, entry: Entry<A::Input>) {
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.insert_ns);

        self.wheel.insert(entry);
    }
    /// Returns a reference to the underlying HAW
    fn wheel(&self) -> &ReadWheel<A> {
        self.wheel.read()
    }
    #[cfg(feature = "stats")]
    fn stats(&self) -> &crate::stats::Stats {
        let agg_store_size = self.wheel.read().as_ref().size_bytes();
        self.stats.size_bytes.set(agg_store_size);
        &self.stats
    }
}
