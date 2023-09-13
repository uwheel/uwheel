use core::mem;

use crate::{state::State, WindowExt};

use super::util::{pairs_capacity, PairType};
use awheel_core::{
    aggregator::{Aggregator, InverseExt},
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

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "stats")]
use awheel_stats::profile_scope;

/// A fixed-sized wheel used to maintain partial aggregates for slides that can later
/// be used to inverse windows.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct InverseWheel<A: Aggregator> {
    num_slots: usize,
    capacity: usize,
    slots: Box<[Option<A::PartialAggregate>]>,
    tail: usize,
    head: usize,
}

impl<A: Aggregator> InverseWheel<A> {
    fn with_capacity(capacity: usize) -> Self {
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
    fn tick(&mut self) -> Option<A::PartialAggregate> {
        let tail = self.tail;
        self.tail = self.wrap_add(self.tail, 1);
        // Tick next partial agg to be inversed
        // 1: [0-10] 2: [10-20] -> need that to be [0-20] so we combine
        let partial_agg = self.slot(tail).take();
        if let Some(agg) = partial_agg {
            combine_or_insert::<A>(self.slot(self.tail), agg);
        }

        partial_agg
    }

    fn clear_tail_and_tick(&mut self) {
        *self.slot(self.tail) = None;
        let _ = self.tick();
    }
    #[inline]
    fn push(&mut self, data_opt: Option<A::PartialAggregate>) {
        if let Some(data) = data_opt {
            combine_or_insert::<A>(self.slot(self.head), data);
        }
        self.head = self.wrap_add(self.head, 1);
    }

    #[inline]
    fn slot(&mut self, idx: usize) -> &mut Option<A::PartialAggregate> {
        &mut self.slots[idx]
    }
}

impl<A: Aggregator> WheelExt for InverseWheel<A> {
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

/// A Builder type for [EagerWindowWheel]
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
    /// Configures the builder to create a wheel with the given write-ahead capacity
    pub fn with_write_ahead(mut self, write_ahead: usize) -> Self {
        self.write_ahead = write_ahead;
        self
    }

    /// Configures the builder to create a wheel with the given watermark
    pub fn with_watermark(mut self, watermark: u64) -> Self {
        self.time = watermark;
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
    /// Consumes the builder and returns a [EagerWindowWheel]
    pub fn build<A: Aggregator + InverseExt>(self) -> EagerWindowWheel<A> {
        assert!(
            self.range >= self.slide,
            "Range must be larger or equal to slide"
        );
        EagerWindowWheel::new(self.time, self.write_ahead, self.range, self.slide)
    }
}

/// Wrapper on top of HAW to implement Sliding Window Aggregation
///
/// Requires an aggregation function that supports invertibility
pub struct EagerWindowWheel<A: Aggregator + InverseExt> {
    range: usize,
    slide: usize,
    // Inverse Wheel maintaining partial aggregates per slide
    inverse_wheel: InverseWheel<A>,
    // Regular HAW used together with inverse_wheel to answer a specific Sliding Window
    wheel: RwWheel<A>,
    state: State,
    next_full_rotation: u64,
    // How many seconds we are in the current rotation of ``RANGE``
    current_secs_rotation: u64,
    // a cached partial aggregate holding data for last full rotation (RANGE)
    last_rotation: Option<A::PartialAggregate>,
    #[cfg(feature = "stats")]
    stats: super::stats::Stats,
}

impl<A: Aggregator + InverseExt> EagerWindowWheel<A> {
    fn new(time: u64, write_ahead: usize, range: usize, slide: usize) -> Self {
        let state = State::new(time, range, slide);
        let pair_slots = pairs_capacity(range, slide);
        let options = Options::default()
            .with_watermark(time)
            .with_write_ahead(write_ahead);
        Self {
            range,
            slide,
            inverse_wheel: InverseWheel::with_capacity(pair_slots),
            wheel: RwWheel::with_options(options),
            state,
            next_full_rotation: time + range as u64,
            current_secs_rotation: 0,
            last_rotation: None,
            #[cfg(feature = "stats")]
            stats: Default::default(),
        }
    }
    fn range_interval_duration(&self) -> Duration {
        Duration::seconds((self.range / 1000) as i64)
    }

    #[inline]
    fn merge_pairs(&mut self) {
        // how many "pairs" we need to pop off from the Pairs wheel
        let removals = match self.state.pair_type {
            PairType::Even(_) => 1,
            PairType::Uneven(_, _) => 2,
        };
        for _i in 0..removals - 1 {
            let _ = self.inverse_wheel.tick();
        }
    }
    #[inline]
    fn compute_window(&mut self) -> A::PartialAggregate {
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.window_computation_ns);

        let inverse = self.inverse_wheel.tick().unwrap_or_default();

        {
            #[cfg(feature = "stats")]
            profile_scope!(&self.stats.cleanup_ns);
            self.merge_pairs();
        }

        let last_rotation = self.last_rotation.unwrap();
        let current_rotation = self
            .wheel
            .read()
            .interval(Duration::seconds(self.current_secs_rotation as i64))
            .unwrap_or_default();

        // Function: combine(inverse_combine(last_rotation, slice), current_rotation);
        // ⊕((⊖(last_rotation, slice)), current_rotation)
        A::combine(A::inverse_combine(last_rotation, inverse), current_rotation)
    }
}
impl<A: Aggregator + InverseExt> WindowExt<A> for EagerWindowWheel<A> {
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)> {
        let ticks = duration.whole_seconds();
        let mut window_results = Vec::new();
        for _tick in 0..ticks {
            self.wheel.advance(1.seconds());
            self.current_secs_rotation += 1;
            self.state.pair_ticks_remaining -= 1;

            if self.state.pair_ticks_remaining == 0 {
                // pair ended

                // Take partial aggregates from SLIDE interval and insert into InverseWheel
                let partial = self
                    .wheel
                    .read()
                    .interval(self.state.current_pair_duration());

                self.inverse_wheel.push(partial);

                // Update pair metadata
                self.state.update_pair_len();

                self.state.next_pair_end =
                    self.wheel.read().watermark() + self.state.current_pair_len as u64;
                self.state.pair_ticks_remaining =
                    self.state.current_pair_duration().whole_seconds() as usize;

                if self.wheel.read().watermark() == self.state.next_window_end {
                    if self.state.next_window_end == self.next_full_rotation {
                        {
                            // Need to scope the for profiling
                            #[cfg(feature = "stats")]
                            profile_scope!(&self.stats.window_computation_ns);

                            let window_result = self
                                .wheel
                                .read()
                                .interval(self.range_interval_duration())
                                .unwrap_or_default();
                            self.last_rotation = Some(window_result);

                            window_results.push((
                                self.wheel.read().watermark(),
                                Some(A::lower(window_result)),
                            ));
                        }
                        #[cfg(feature = "stats")]
                        profile_scope!(&self.stats.cleanup_ns);

                        // If we are working with uneven pairs, we need to adjust range.
                        let next_rotation_distance = if self.state.pair_type.is_uneven() {
                            self.range as u64 - 1000
                        } else {
                            self.range as u64
                        };

                        self.next_full_rotation += next_rotation_distance;
                        self.current_secs_rotation = 0;

                        // If we have already completed a full RANGE
                        // then we need to reset the current inverse tail
                        if (self
                            .wheel
                            .read()
                            .current_time_in_cycle()
                            .whole_milliseconds() as usize)
                            > self.range
                        {
                            self.inverse_wheel.clear_tail_and_tick();
                        }

                        self.merge_pairs();
                    } else {
                        let window = self.compute_window();
                        window_results
                            .push((self.wheel.read().watermark(), Some(A::lower(window))));
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

#[cfg(test)]
mod tests {
    use awheel_core::aggregator::sum::U64SumAggregator;

    use super::*;

    #[test]
    fn inverse_wheel_test() {
        let mut iwheel: InverseWheel<U64SumAggregator> = InverseWheel::with_capacity(64);
        iwheel.push(Some(2u64));
        iwheel.push(Some(3u64));
        iwheel.push(Some(10u64));

        assert_eq!(iwheel.tick().unwrap(), 2u64);
        assert_eq!(iwheel.tick().unwrap(), 5u64);
        assert_eq!(iwheel.tick().unwrap(), 15u64);
    }
}
