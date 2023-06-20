pub mod invertible;

use crate::{
    aggregator::{Aggregator, InverseExt},
    time::Duration,
    Entry,
    Error,
    Wheel,
};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[derive(Debug, Clone, Copy)]
pub enum PairType {
    // Slice size when range % slide = 0
    Even(usize),
    // Two pairs (p1, p2) when range % slide != 0
    Uneven(usize, usize),
}

pub fn create_pair_type(range: usize, slide: usize) -> PairType {
    let p2 = range % slide;
    if p2 == 0 {
        PairType::Even(slide)
    } else {
        let p1 = slide - p2;
        PairType::Uneven(p1, p2)
    }
}
pub const fn panes_space(range: usize, slide: usize) -> usize {
    assert!(range >= slide, "Range needs to be larger than slide");
    range / gcd(range, slide)
}
pub const fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let temp = b;
        b = a % b;
        a = temp;
    }
    a
}

// Verifies that returned capacity is a power of two
pub const fn capacity(range: usize, slide: usize) -> usize {
    let space = panes_space(range, slide);
    if space.is_power_of_two() {
        space
    } else {
        space.next_power_of_two()
    }
}

#[inline]
const fn ceil_div(a: usize, b: usize) -> usize {
    (a + (b - 1)) / b
}

// Based on a Range and Slide, generate number of slots required using the Pairs technique
pub const fn pairs_space(range: usize, slide: usize) -> usize {
    assert!(range >= slide, "Range needs to be larger than slide");
    let p2 = range % slide;
    if p2 == 0 {
        range / slide
    } else {
        ceil_div(2 * range, slide)
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
    pub fn build<A: Aggregator + InverseExt>(self) -> WindowWheel<A> {
        // TODO: sanity check of range and slide
        WindowWheel::new(self.time, self.range, self.slide)
    }
}

/// Wrapper on top of HAW to implement Sliding Window Aggregation
///
/// Requires an aggregation function that supports invertibility
#[allow(dead_code)]
pub struct WindowWheel<A: Aggregator> {
    //pairs_wheel: PairsWheel<
    range: usize,
    slide: usize,
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

impl<A: Aggregator> WindowWheel<A> {
    pub fn new(time: u64, range: usize, slide: usize) -> Self {
        Self {
            range,
            slide,
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
    fn _slide_interval_duration(&self) -> Duration {
        Duration::seconds((self.slide / 1000) as i64)
    }
    fn _range_interval_duration(&self) -> Duration {
        Duration::seconds((self.range / 1000) as i64)
    }
    // Currently assumes per SLIDE advance call
    pub fn advance_to(&mut self, new_watermark: u64) {
        //let diff = new_watermark.saturating_sub(self.wheel.watermark());
        //let ticks = diff / SLIDE as u64;
        self.wheel.advance_to(new_watermark);
    }
    pub fn results(&self) -> &[A::PartialAggregate] {
        &self.window_results
    }
    #[inline]
    pub fn query(&mut self) -> Option<A::Aggregate> {
        None
    }
    #[inline]
    pub fn insert(&mut self, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        self.wheel.insert(entry)
    }
    /// Returns a reference to the underlying HAW
    pub fn wheel(&self) -> &Wheel<A> {
        &self.wheel
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregator::U64SumAggregator;

    use super::*;

    #[test]
    fn simple_window_wheel_test() {
        let mut wheel: WindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::hours(1))
            .with_slide(Duration::seconds(1))
            .build();
        assert_eq!(wheel.query(), None);
    }
}
