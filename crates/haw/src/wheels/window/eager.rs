use super::util::{create_pair_type, pairs_capacity, PairType};
use crate::{
    aggregator::{Aggregator, InverseExt},
    time::{Duration, NumericalDuration},
    wheels::{aggregation::combine_or_insert, wrap_add},
    Entry,
    Error,
    Wheel,
};
#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

use super::WindowWheel;

/// A fixed-sized wheel used to maintain partial aggregates for slides that can later
/// be used to inverse windows.
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Clone)]
pub struct InverseWheel<A: Aggregator> {
    capacity: usize,
    aggregator: A,
    slots: Box<[Option<A::PartialAggregate>]>,
    tail: usize,
    head: usize,
}

impl<A: Aggregator> InverseWheel<A> {
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
        let tail = self.tail;
        self.tail = wrap_add(self.tail, 1, self.capacity);
        // Tick next partial agg to be inversed
        // 1: [0-10] 2: [10-20] -> need that to be [0-20] so we combine
        let partial_agg = self.slot(tail).take();
        if let Some(agg) = partial_agg {
            combine_or_insert::<A>(self.slot(self.tail), agg, &Default::default());
        }

        partial_agg
    }
    pub fn tail(&self) -> (usize, Option<A::PartialAggregate>) {
        (self.tail, self.slots[self.tail].as_ref().copied())
    }

    /// Returns `true` if the wheel is empty or `false` if it contains slots
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail == self.head
    }

    pub fn clear_tail_and_tick(&mut self) {
        *self.slot(self.tail) = None;
        let _ = self.tick();
    }
    pub fn reset_tail(&mut self) {
        *self.slot(self.tail) = None;
    }
    #[inline]
    pub fn push(&mut self, data: A::PartialAggregate, aggregator: &A) {
        combine_or_insert(self.slot(self.head), data, aggregator);
        self.head = wrap_add(self.head, 1, self.capacity);
    }

    #[inline]
    fn slot(&mut self, idx: usize) -> &mut Option<A::PartialAggregate> {
        &mut self.slots[idx]
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
    pub fn build<A: Aggregator + InverseExt>(self) -> EagerWindowWheel<A> {
        // TODO: sanity check of range and slide
        EagerWindowWheel::new(self.time, self.range, self.slide)
    }
}

/// Wrapper on top of HAW to implement Sliding Window Aggregation
///
/// Requires an aggregation function that supports invertibility
#[allow(dead_code)]
pub struct EagerWindowWheel<A: Aggregator + InverseExt> {
    range: usize,
    slide: usize,
    pair_ticks_remaining: usize,
    current_pair_len: usize,
    pair_type: PairType,
    next_pair_end: u64,
    in_p1: bool,

    // Inverse Wheel maintaining partial aggregates per slide
    inverse_wheel: InverseWheel<A>,
    // Regular HAW used together with inverse_wheel to answer a specific Sliding Window
    wheel: Wheel<A>,
    // When the next window starts
    next_window_start: u64,
    // When the next window ends
    next_window_end: u64,
    // When next full rotation has happend
    next_full_rotation: u64,
    // How many seconds we are in the current rotation of ``RANGE``
    current_secs_rotation: u64,
    // a cached partial aggregate holding data for last full rotation (RANGE)
    last_rotation: Option<A::PartialAggregate>,
    aggregator: A,
}

impl<A: Aggregator + InverseExt> EagerWindowWheel<A> {
    pub fn new(time: u64, range: usize, slide: usize) -> Self {
        let pair_type = create_pair_type(range, slide);
        let current_pair_len = match pair_type {
            PairType::Even(slide) => slide,
            PairType::Uneven(_, p2) => p2,
        };
        let next_pair_end = time + current_pair_len as u64;
        let pair_slots = pairs_capacity(range, slide);
        Self {
            range,
            slide,
            pair_type,
            next_pair_end,
            current_pair_len,
            pair_ticks_remaining: current_pair_len / 1000,
            in_p1: false,
            inverse_wheel: InverseWheel::with_capacity(pair_slots),
            wheel: Wheel::new(time),
            next_window_start: time + slide as u64,
            next_window_end: time + range as u64,
            next_full_rotation: time + range as u64,
            current_secs_rotation: 0,
            last_rotation: None,
            aggregator: Default::default(),
        }
    }
    fn range_interval_duration(&self) -> Duration {
        Duration::seconds((self.range / 1000) as i64)
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
    fn merge_pairs(&mut self) {
        // how many "pairs" we need to pop off from the Pairs wheel
        let removals = match self.pair_type {
            PairType::Even(_) => 1,
            PairType::Uneven(_, _) => 2,
        };
        for _i in 0..removals - 1 {
            let _ = self.inverse_wheel.tick();
        }
    }
    #[inline]
    fn compute_window(&mut self) -> A::PartialAggregate {
        let inverse = self.inverse_wheel.tick().unwrap_or_default();
        let last_rotation = self.last_rotation.unwrap();
        let current_rotation = self
            .wheel
            .interval(Duration::seconds(self.current_secs_rotation as i64))
            .unwrap_or_default();

        self.merge_pairs();

        // Function: combine(inverse_combine(last_rotation, slice), current_rotation);
        // ⊕((⊖(last_rotation, slice)), current_rotation)
        self.aggregator.combine(
            self.aggregator.inverse_combine(last_rotation, inverse),
            current_rotation,
        )
    }
}
impl<A: Aggregator + InverseExt> WindowWheel<A> for EagerWindowWheel<A> {
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)> {
        let ticks = duration.whole_seconds();
        let mut window_results = Vec::new();
        for _tick in 0..ticks {
            self.wheel.advance(1.seconds());
            self.current_secs_rotation += 1;
            self.pair_ticks_remaining -= 1;

            if self.pair_ticks_remaining == 0 {
                // pair ended

                // Take partial aggregates from SLIDE interval and insert into InverseWheel
                let partial = self
                    .wheel
                    .interval(self.current_pair_duration())
                    .unwrap_or_default();

                self.inverse_wheel.push(partial, &self.aggregator);

                // Update pair metadata
                self.update_pair_len();

                self.next_pair_end = self.wheel.watermark() + self.current_pair_len as u64;
                self.pair_ticks_remaining = self.current_pair_duration().whole_seconds() as usize;

                if self.wheel.watermark() == self.next_window_end {
                    if self.next_window_end == self.next_full_rotation {
                        let window_result =
                            self.wheel.interval(self.range_interval_duration()).unwrap();
                        self.last_rotation = Some(window_result);

                        window_results.push((
                            self.wheel.watermark(),
                            Some(self.aggregator.lower(window_result)),
                        ));

                        // If we are working with uneven pairs, we need to adjust range.
                        let next_rotation_distance = if self.pair_type.is_uneven() {
                            self.range as u64 - 1000
                        } else {
                            self.range as u64
                        };

                        self.next_full_rotation += next_rotation_distance;
                        self.current_secs_rotation = 0;

                        // If we have already completed a full RANGE
                        // then we need to reset the current inverse tail
                        if (self.wheel.current_time_in_cycle().whole_milliseconds() as usize)
                            > self.range
                        {
                            self.inverse_wheel.clear_tail_and_tick();
                        }

                        self.merge_pairs();
                    } else {
                        let window = self.compute_window();
                        window_results
                            .push((self.wheel.watermark(), Some(self.aggregator.lower(window))));
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
        self.advance(Duration::milliseconds(diff as i64))
    }
    #[inline]
    fn insert(&mut self, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        self.wheel.insert(entry)
    }
    /// Returns a reference to the underlying HAW
    fn wheel(&self) -> &Wheel<A> {
        &self.wheel
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregator::U64SumAggregator;

    use super::*;

    #[test]
    fn inverse_wheel_test() {
        let mut iwheel: InverseWheel<U64SumAggregator> = InverseWheel::with_capacity(64);
        let aggregator = U64SumAggregator;
        iwheel.push(2u64, &aggregator);
        iwheel.push(3u64, &aggregator);
        iwheel.push(10u64, &aggregator);

        assert_eq!(iwheel.tick().unwrap(), 2u64);
        assert_eq!(iwheel.tick().unwrap(), 5u64);
        assert_eq!(iwheel.tick().unwrap(), 15u64);
    }
}
