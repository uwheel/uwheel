use super::util::capacity;
use crate::{
    aggregator::{Aggregator, InverseExt},
    time::Duration,
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
        assert!(capacity.is_power_of_two(), "Capacity must be power of two");
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
            // Tick next partial agg to be inversed
            // 1: [0-10] 2: [10-20] -> need that to be [0-20] so we combine
            let partial_agg = self.slot(tail).take();
            if let Some(agg) = partial_agg {
                Self::insert(self.slot(self.tail), agg, &Default::default());
            }
            partial_agg
        } else {
            None
        }
    }

    /// Returns `true` if the wheel is empty or `false` if it contains slots
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail == self.head
    }

    pub fn clear_current_tail(&mut self) {
        *self.slot(self.tail) = Some(Default::default());
    }
    #[inline]
    pub fn push(&mut self, data: A::PartialAggregate, aggregator: &A) {
        Self::insert(self.slot(self.head), data, aggregator);
        self.head = self.wrap_add(self.head, 1);
    }

    #[inline]
    fn slot(&mut self, idx: usize) -> &mut Option<A::PartialAggregate> {
        &mut self.slots[idx]
    }
    #[inline]
    fn insert(slot: &mut Option<A::PartialAggregate>, entry: A::PartialAggregate, aggregator: &A) {
        match slot {
            Some(curr) => {
                let new_curr = aggregator.combine(*curr, entry);
                *curr = new_curr;
            }
            None => {
                *slot = Some(entry);
            }
        }
    }

    /// Returns the current number of used slots (includes empty NONE slots as well)
    pub fn len(&self) -> usize {
        count(self.tail, self.head, self.capacity)
    }
    /// Returns the index in the underlying buffer for a given logical element
    /// index + addend.
    #[inline]
    fn wrap_add(&self, idx: usize, addend: usize) -> usize {
        wrap_index(idx.wrapping_add(addend), self.capacity)
    }
}

/// Returns the index in the underlying buffer for a given logical element index.
#[inline]
fn wrap_index(index: usize, size: usize) -> usize {
    // size is always a power of 2
    debug_assert!(size.is_power_of_two());
    index & (size - 1)
}

/// Calculate the number of elements left to be read in the buffer
#[inline]
fn count(tail: usize, head: usize, size: usize) -> usize {
    // size is always a power of 2
    (head.wrapping_sub(tail)) & (size - 1)
}

/*
/// WIP: Function that takes a range and slide duration and calculates the upper bound of ⊕ operations required to compute a window.
pub fn wheels_cost(range: Duration, slide: Duration) {
    // If RANGE and SLIDE same granularity (e.g., range 10 sec, slide 2 sec).
    // then cost: RANGE.

    // let d = |RANGE.g - SLIDE.g|;

    // 1. last_rotation(RANGE) cached
    // 2. inverse_combine(1, inverse_slice) (1 ⊕)
    // 3. current_rotation(d + RANGE-1⊕)
    // 4.  (2) ⊕ (3)
    // First window worst-case: RANGE ⊕
    // Worst case: Upper bound of ⊕ operations: 2 + d + R-1
    // Best case: 2 + d
    // worst case with HAW: 2 + 4 + 50 = 56 aggregate calls

    let range_as_secs = range.whole_seconds();
    let slide_as_secs = slide.whole_seconds();
    // closure that turns i64 to None if it is zero
    let to_option = |num: i64| {
        if num == 0 {
            None
        } else {
            Some(num as usize)
        }
    };

    // if largest granularity = 1 then best case
    // let dur = Duration::seconds(range_as_secs);

    //let dur = Duration::seconds(range_as_secs - slide_as_secs);

    //let second = to_option(dur.whole_seconds() % crate::SECONDS as i64);
    //let minute = to_option(dur.whole_minutes() % crate::MINUTES as i64);
    //let hour = to_option(dur.whole_hours() % crate::HOURS as i64);
    //let day = to_option(dur.whole_days() % crate::DAYS as i64);
    //let week = to_option(dur.whole_weeks() % crate::WEEKS as i64);
    //let year = to_option((dur.whole_weeks() / crate::WEEKS as i64) % crate::YEARS as i64);
    //dbg!((second, minute, hour, day, week, year));
}
*/

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
    window_results: Vec<A::PartialAggregate>,
    first_window: bool,
    aggregator: A,
}

impl<A: Aggregator + InverseExt> EagerWindowWheel<A> {
    pub fn new(time: u64, range: usize, slide: usize) -> Self {
        Self {
            range,
            slide,
            inverse_wheel: InverseWheel::with_capacity(capacity(range, slide)),
            wheel: Wheel::new(time),
            next_window_start: time + slide as u64,
            next_window_end: time + range as u64,
            next_full_rotation: time + range as u64,
            current_secs_rotation: 0,
            last_rotation: None,
            window_results: Vec::new(),
            first_window: true,
            aggregator: Default::default(),
        }
    }
    fn slide_interval_duration(&self) -> Duration {
        Duration::seconds((self.slide / 1000) as i64)
    }
    fn range_interval_duration(&self) -> Duration {
        Duration::seconds((self.range / 1000) as i64)
    }
    #[inline]
    pub fn query(&mut self) -> Option<A::Aggregate> {
        None
    }
}
impl<A: Aggregator + InverseExt> WindowWheel<A> for EagerWindowWheel<A> {
    // Currently assumes per SLIDE advance call
    fn advance_to(&mut self, new_watermark: u64) {
        //let diff = new_watermark.saturating_sub(self.wheel.watermark());
        //let ticks = diff / self.slide as u64;

        if new_watermark >= self.next_window_start {
            self.wheel.advance_to(self.next_window_start);

            self.next_window_start += self.slide as u64;
            // Take partial aggregates from SLIDE interval and insert into InverseWheel
            let partial = self
                .wheel
                .interval(self.slide_interval_duration())
                .unwrap_or_default();
            self.inverse_wheel.push(partial, &self.aggregator);
        }
        if new_watermark >= self.next_window_end {
            self.wheel.advance_to(self.next_window_end);

            if self.next_window_end == self.next_full_rotation {
                // Window has rolled up fully so we can access the results directly
                let window_result = self.wheel.interval(self.range_interval_duration()).unwrap();
                self.last_rotation = Some(window_result);

                self.window_results.push(window_result);
                self.next_full_rotation += self.range as u64;
                self.current_secs_rotation = 0;
                if !self.first_window {
                    self.inverse_wheel.clear_current_tail();
                    let _ = self.inverse_wheel.tick();
                } else {
                    self.first_window = false;
                }
            } else {
                // bump the current rotation
                self.current_secs_rotation +=
                    Duration::seconds((self.slide / 1000) as i64).whole_seconds() as u64;
                let inverse = self.inverse_wheel.tick().unwrap_or_default();

                // Optimization: Keep around Partial Aggregate for last rotation? Direct Access instead of potential scan
                // Otherwise N ⊕ calls will be required to calculate the RANGE interval. For example RANGE 5 DAYS -> combine 5 last slots in days wheel.
                let last_rotation = self.last_rotation.unwrap();
                let current_rotation = self
                    .wheel
                    .interval(Duration::seconds(self.current_secs_rotation as i64))
                    .unwrap_or_default();
                //dbg!((last_rotation, current_rotation, inverse));

                // Function: combine(inverse_combine(last_rotation, slice), current_rotation);
                // ⊕((⊖(last_rotation, slice)), current_rotation)
                // last_rotation: worst-case RANGE ⊕ operations required (at most 51 weeks)
                // current_rotation: worst-case d + RANGE-1 ⊕ ops where d is granularity distance between RANGE and SLIDE. (at most 4 + 51 = 55)
                let window_result = self.aggregator.combine(
                    self.aggregator.inverse_combine(last_rotation, inverse),
                    current_rotation,
                );

                self.window_results.push(window_result);
            }
            self.next_window_end += self.slide as u64;
        }

        // Make sure we have advanced to the new watermark
        self.wheel.advance_to(new_watermark);
    }
    #[inline]
    fn insert(&mut self, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        self.wheel.insert(entry)
    }
    /// Returns a reference to the underlying HAW
    fn wheel(&self) -> &Wheel<A> {
        &self.wheel
    }
    // just for testing now
    fn results(&self) -> &[A::PartialAggregate] {
        &self.window_results
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
