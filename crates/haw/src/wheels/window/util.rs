use crate::{
    aggregator::{Aggregator, InverseExt},
    time::Duration,
};

use super::WindowWheel;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

#[derive(Debug, Clone, Copy)]
pub enum PairType {
    /// Slice size when range % slide = 0
    Even(usize),
    /// Two pairs (p1, p2) when range % slide != 0
    Uneven(usize, usize),
}
impl PairType {
    pub fn is_uneven(&self) -> bool {
        matches!(self, PairType::Uneven { .. })
    }
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

// Verifies that returned capacity is a power of two
pub const fn pairs_capacity(range: usize, slide: usize) -> usize {
    let space = pairs_space(range, slide);
    if space.is_power_of_two() {
        space
    } else {
        space.next_power_of_two()
    }
}

/// Calculates the number of aggregate operations required to for a final window aggregation
/// with a RANGE r and SLIDE s together with a Lazy Wheel
pub fn lazy_window_query_cost(range: Duration, slide: Duration) -> usize {
    assert!(
        is_quantizable(range),
        "Range {} is not quantizable to time intervals of HAW",
        range,
    );
    assert!(
        is_quantizable(slide),
        "Slide {} is not quantizable to time intervals of HAW",
        slide,
    );
    // The number of combine operations is at most [2r/s] and at best [r/s]
    let range_ms = range.whole_milliseconds() as usize;
    let slide_ms = slide.whole_milliseconds() as usize;
    pairs_space(range_ms, slide_ms)
}

/// Calculates the number of aggregate operations required to for a final window aggregation
/// with a RANGE r and SLIDE s together with a Eager Wheel
pub fn eager_window_query_cost(range: Duration, slide: Duration) -> usize {
    assert!(
        is_quantizable(range),
        "Range {} is not quantizable to time intervals of HAW",
        range,
    );
    assert!(
        is_quantizable(slide),
        "Slide {} is not quantizable to time intervals of HAW",
        slide,
    );

    let d = granularity_distance(range, slide);
    let range_intervals = count_intervals(range) as usize;
    if range_intervals == 1 {
        // (e.g., 1 MIN, 1 HOUR, 1 DAY, 1 WEEK, or 1 YEAR)
        let distance_combines = d.saturating_sub(1);
        2 + distance_combines
    } else if d == 0 {
        // Range: R
        let slide_intervals = count_intervals(slide) as usize;
        let total = 2 + range_intervals - slide_intervals;
        if total > range_intervals {
            total
        } else {
            range_intervals
        }
    } else {
        // 2 + d + (r-1)
        let distance_combines = d.saturating_sub(1);
        2 + distance_combines + (range_intervals.saturating_sub(1))
    }
}

// Checks whether the duration is quantizable to time intervals of HAW
const fn is_quantizable(duration: Duration) -> bool {
    let seconds = duration.whole_seconds();

    if seconds > 0 && seconds <= 59
        || seconds % Duration::minutes(1).whole_seconds() == 0
        || seconds % Duration::hours(1).whole_seconds() == 0
        || seconds % Duration::days(1).whole_seconds() == 0
        || seconds % Duration::weeks(1).whole_seconds() == 0
        || seconds % Duration::years(1).whole_seconds() == 0
    {
        // TODO: add check that verifies it fits within crate::YEARS
        return true;
    }

    false
}

#[allow(clippy::bool_to_int_with_if)]
fn granularity_distance(range: Duration, slide: Duration) -> usize {
    let granularity = |seconds: i64| -> usize {
        if seconds >= Duration::years(1).whole_seconds() {
            5 // years
        } else if seconds >= Duration::weeks(1).whole_seconds() {
            4 // weeks
        } else if seconds >= Duration::days(1).whole_seconds() {
            3 // days
        } else if seconds >= Duration::hours(1).whole_seconds() {
            2 // hours
        } else if seconds >= Duration::minutes(1).whole_seconds() {
            1 // minutes
        } else {
            0 // seconds
        }
    };
    granularity(range.whole_seconds()) - granularity(slide.whole_seconds())
}

fn count_intervals(range: Duration) -> i64 {
    let range_secs = range.whole_seconds();

    if range_secs == 0 {
        return 0; // Zero duration, no intervals
    }
    let years_secs = Duration::years(1).whole_seconds();
    let weeks_secs = Duration::weeks(1).whole_seconds();
    let day_secs = Duration::days(1).whole_seconds();
    let hour_secs = Duration::hours(1).whole_seconds();
    let min_secs = Duration::minutes(1).whole_seconds();

    if range_secs % years_secs == 0 {
        range_secs / years_secs
    } else if range_secs % weeks_secs == 0 {
        range_secs / weeks_secs
    } else if range_secs % day_secs == 0 {
        range_secs / day_secs
    } else if range_secs % hour_secs == 0 {
        range_secs / hour_secs
    } else if range_secs % min_secs == 0 {
        range_secs / min_secs
    } else {
        range_secs
    }
}

// Calculates query cost for each version and returns the window
// wheel that requires the least amount of aggregate operations.
pub fn window_wheel<A: Aggregator + InverseExt>(
    range: Duration,
    slide: Duration,
) -> Box<dyn WindowWheel<A>> {
    let eager_cost = eager_window_query_cost(range, slide);
    let lazy_cost = lazy_window_query_cost(range, slide);
    if lazy_cost < eager_cost {
        Box::new(
            super::lazy::Builder::default()
                .with_range(range)
                .with_slide(slide)
                .build(),
        )
    } else {
        Box::new(
            super::eager::Builder::default()
                .with_range(range)
                .with_slide(slide)
                .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::NumericalDuration;

    #[test]
    fn granularity_distance_cost_test() {
        assert_eq!(granularity_distance(30.seconds(), 10.seconds()), 0);
        assert_eq!(granularity_distance(1.minutes(), 10.seconds()), 1);
        assert_eq!(granularity_distance(1.hours(), 10.seconds()), 2);
        assert_eq!(granularity_distance(1.days(), 10.seconds()), 3);
        assert_eq!(granularity_distance(1.weeks(), 10.seconds()), 4);
        assert_eq!(granularity_distance(1.years(), 10.seconds()), 5);

        assert_eq!(granularity_distance(1.hours(), 10.minutes()), 1);
    }
    #[test]
    fn cost_plot_test() {
        dbg!(lazy_window_query_cost(10.seconds(), 5.seconds()));
        dbg!(lazy_window_query_cost(30.seconds(), 5.seconds()));
        dbg!(lazy_window_query_cost(59.seconds(), 5.seconds()));
        dbg!(lazy_window_query_cost(1.minutes(), 5.seconds()));
        dbg!(lazy_window_query_cost(59.minutes(), 5.seconds()));
        dbg!(lazy_window_query_cost(1.hours(), 5.seconds()));
        dbg!(lazy_window_query_cost(23.hours(), 5.seconds()));
        dbg!(lazy_window_query_cost(1.days(), 5.seconds()));
        dbg!(lazy_window_query_cost(6.days(), 5.seconds()));
        dbg!(lazy_window_query_cost(1.weeks(), 5.seconds()));
        dbg!(lazy_window_query_cost(51.weeks(), 5.seconds()));
        dbg!(lazy_window_query_cost(1.years(), 5.seconds()));

        dbg!(eager_window_query_cost(10.seconds(), 5.seconds()));
        dbg!(eager_window_query_cost(30.seconds(), 5.seconds()));
        dbg!(eager_window_query_cost(59.seconds(), 5.seconds()));
        dbg!(eager_window_query_cost(1.minutes(), 5.seconds()));
        dbg!(eager_window_query_cost(59.minutes(), 5.seconds()));
        dbg!(eager_window_query_cost(1.hours(), 5.seconds()));
        dbg!(eager_window_query_cost(23.hours(), 5.seconds()));
        dbg!(eager_window_query_cost(1.days(), 5.seconds()));
        dbg!(eager_window_query_cost(6.days(), 5.seconds()));
        dbg!(eager_window_query_cost(1.weeks(), 5.seconds()));
        dbg!(eager_window_query_cost(51.weeks(), 5.seconds()));
        dbg!(eager_window_query_cost(1.years(), 5.seconds()));
    }
    #[test]
    fn window_aggregation_cost_test() {
        assert_eq!(eager_window_query_cost(2.minutes(), 10.seconds()), 3);
        assert_eq!(eager_window_query_cost(3.minutes(), 10.seconds()), 4);
        assert_eq!(eager_window_query_cost(4.minutes(), 10.seconds()), 5);

        assert_eq!(eager_window_query_cost(51.weeks(), 10.seconds()), 56);
        assert_eq!(lazy_window_query_cost(51.weeks(), 10.seconds()), 3084480);

        assert_eq!(lazy_window_query_cost(60.seconds(), 10.seconds()), 6);

        assert_eq!(eager_window_query_cost(10.seconds(), 3.seconds()), 10);
        assert_eq!(eager_window_query_cost(20.seconds(), 5.seconds()), 20);
        assert_eq!(eager_window_query_cost(40.seconds(), 5.seconds()), 40);

        assert_eq!(eager_window_query_cost(59.seconds(), 5.seconds()), 59);
        assert_eq!(lazy_window_query_cost(59.seconds(), 5.seconds()), 24);

        assert_eq!(eager_window_query_cost(59.minutes(), 5.seconds()), 60);
        assert_eq!(lazy_window_query_cost(59.minutes(), 5.seconds()), 708);

        assert_eq!(eager_window_query_cost(59.minutes(), 5.seconds()), 60);
        assert_eq!(lazy_window_query_cost(59.minutes(), 5.seconds()), 708);

        // seconds slide
        assert_eq!(eager_window_query_cost(1.minutes(), 10.seconds()), 2);
        assert_eq!(eager_window_query_cost(1.hours(), 10.seconds()), 3);
        assert_eq!(eager_window_query_cost(1.days(), 10.seconds()), 4);
        assert_eq!(eager_window_query_cost(1.weeks(), 10.seconds()), 5);
        assert_eq!(eager_window_query_cost(1.years(), 10.seconds()), 6);

        // minutes slide
        assert_eq!(eager_window_query_cost(1.hours(), 10.minutes()), 2);
        assert_eq!(eager_window_query_cost(1.days(), 10.minutes()), 3);
        assert_eq!(eager_window_query_cost(1.weeks(), 10.minutes()), 4);
        assert_eq!(eager_window_query_cost(1.years(), 10.minutes()), 5);

        // hours slide
        assert_eq!(eager_window_query_cost(1.days(), 10.hours()), 2);
        assert_eq!(eager_window_query_cost(1.weeks(), 10.hours()), 3);
        assert_eq!(eager_window_query_cost(1.years(), 10.hours()), 4);

        // days slide
        assert_eq!(eager_window_query_cost(1.weeks(), 1.days()), 2);
        assert_eq!(eager_window_query_cost(1.years(), 1.days()), 3);
    }
}
