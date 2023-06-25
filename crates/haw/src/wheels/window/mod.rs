use crate::{aggregator::Aggregator, Entry, Error, Wheel};

pub mod eager;
pub mod lazy;
mod util;

pub trait WindowWheel<A: Aggregator> {
    fn insert(&mut self, entry: Entry<A::Input>) -> Result<(), Error<A::Input>>;
    fn advance_to(&mut self, watermark: u64);
    /// Returns a reference to the underlying HAW
    fn wheel(&self) -> &Wheel<A>;
    fn results(&self) -> &[A::PartialAggregate];
}

#[cfg(test)]
mod tests {
    use crate::{
        aggregator::{InverseExt, U64SumAggregator},
        time::Duration,
        wheels::window::lazy::LazyWindowWheel,
    };
    use eager::{Builder, EagerWindowWheel, InverseWheel};
    use util::*;

    use super::*;

    #[test]
    fn complexity_test() {
        // Window Wheel works best when time granularities align to HAW

        // Worst case for current rotation = Interval(RANGE - SLIDE); RANGE = 2 days SLIDE 10 => 1 Day  23 hours 59 minutes, 50 seconds

        // fn last_rotation():
        // best-case 0 ⊕ when RANGE(1) or rotation_count == range.
        // worst-case N ⊕ operations required  when RANGE(>1)
        // number of ⊕ operations decided by window spec at initialization => remains constant O(1)

        // fn current_rotation():
        // worst-case (RANGE - SLIDE): [wx....wy]
        // let wheels = distance(RANGE.granularity, SLIDE.granularity);
        // worst-case: wheels + RANGE-1
        // best-case: wheels
        // When RANGE > 1
        // Worst-case Complexity: O(d + r-1)
        // When RANGE == 1 aka first slot of granularity
        // Best-case Complexity: O(d)

        // d = distance
        // d + n

        // RANGE: 1 day SLIDE: 10 second
        dbg!(panes_space(
            Duration::days(1).whole_milliseconds() as usize,
            Duration::seconds(10).whole_milliseconds() as usize
        ));
        dbg!(pairs_space(
            Duration::days(1).whole_milliseconds() as usize,
            Duration::seconds(10).whole_milliseconds() as usize
        ));
        // For Window Wheel: Worst-case number of aggregate operations => 23 hours 59 minutes, 50 seconds
        // 1. last_rotation ⊖ inverse_slice (1 coin) - O(1)
        // 2. seconds total ⊕ minutes total (1 coin) - O(1)
        // 3. (2) ⊕ hours total (1 coin) - O(1)
        // 4. (3) ⊕ (1)
        // Worst-case: Number of aggregate operations needed: 4
        // Best-case: Number of aggregate operations needed 1 (access directly from hours wheel 2 slots)
        let wheels_worst_case = 4;
        let wheels_best_case = 1;
        dbg!((wheels_best_case, wheels_worst_case));
        //wheels_cost(Duration::days(1), Duration::seconds(10));

        // RANGE: 2 day SLIDE: 10 second
        dbg!(panes_space(
            Duration::days(2).whole_milliseconds() as usize,
            Duration::seconds(10).whole_milliseconds() as usize
        ));
        dbg!(pairs_space(
            Duration::days(2).whole_milliseconds() as usize,
            Duration::seconds(10).whole_milliseconds() as usize
        ));

        // For Window Wheel: Worst-case number of aggregate operations => 1 day 23 hours 59 minutes, 50 seconds
        // 1. last_rotation ⊖ inverse_slice (1 coin) - O(1)
        // 2. seconds total ⊕ minutes total (1 coin) - O(1)
        // 3. (2) ⊕ hours total (1 coin) - O(1)
        // 4. (3) ⊕ days interval(1) (1 coin)
        // 5. (4) ⊕ (1)
        // Worst-case: Number of aggregate operations needed: 5
        // Best-case: Number of aggregate operations needed 1 (access directly from hours wheel 2 slots)
        let wheels_worst_case = 5;
        let wheels_best_case = 1;
        dbg!((wheels_best_case, wheels_worst_case));

        // RANGE: 1 hr SLIDE: 10 second
        dbg!(panes_space(
            Duration::hours(1).whole_milliseconds() as usize,
            Duration::seconds(10).whole_milliseconds() as usize
        ));
        dbg!(pairs_space(
            Duration::hours(1).whole_milliseconds() as usize,
            Duration::seconds(10).whole_milliseconds() as usize
        ));

        // For Window Wheel: Worst-case number of aggregate operations => 59 minutes, 50 seconds
        // 1. last_rotation ⊖ inverse_slice (1 coin) - O(1)
        // 2. seconds total ⊕ minutes total (1 coin) - O(1)
        // 3. (1) ⊕ (2) (1 coin) - O(1)
        // Worst-case: Number of aggregate operations needed: 3
        // Best-case: Number of aggregate operations needed 0 (access directly from hour wheel slot)
        let wheels_worst_case = 3;
        let wheels_best_case = 0;
        dbg!((wheels_best_case, wheels_worst_case));

        dbg!(panes_space(
            Duration::seconds(10).whole_milliseconds() as usize,
            Duration::seconds(3).whole_milliseconds() as usize
        ));
        dbg!(pairs_space(
            Duration::seconds(10).whole_milliseconds() as usize,
            Duration::seconds(3).whole_milliseconds() as usize
        ));
        // For Window Wheel: Worst-case number of aggregate operations => 10 seconds?

        // RANGE: 51 weeks SLIDE: 10 second
        dbg!(panes_space(
            Duration::weeks(51).whole_milliseconds() as usize,
            Duration::seconds(10).whole_milliseconds() as usize
        ));
        dbg!(pairs_space(
            Duration::weeks(51).whole_milliseconds() as usize,
            Duration::seconds(10).whole_milliseconds() as usize
        ));

        // For Window Wheel: Worst-case number of aggregate operations => 50 weeks, 6 days, 23 hours, 59 minutes, 50 seconds
        // 1. last_rotation ⊖ inverse_slice (1 coin) - O(1)
        // 2. seconds total ⊕ minutes total (1 coin) - O(1)
        // 3. (2) ⊕ hours total (1 coin) - O(1)
        // 4. (3) ⊕  days total (1 coin) - O(1)
        // 5. (4) ⊕  weeks interval (50)⊕ (1 coin) O(50)
        // Worst-case: 2 + d + R-1 => 2 + 4 + 50 => 56
        // Worst-case: Number of aggregate operations needed: 56
        // Best-case: Number of aggregate operations needed 0 (if interval == rotation_count) then total()
        let wheels_worst_case = 56;
        let wheels_best_case = 0;
        dbg!((wheels_best_case, wheels_worst_case));

        // RANGE: 10 seconds SLIDE: 3 seconds
        dbg!(panes_space(
            Duration::seconds(10).whole_milliseconds() as usize,
            Duration::seconds(3).whole_milliseconds() as usize
        ));
        dbg!(pairs_space(
            Duration::seconds(10).whole_milliseconds() as usize,
            Duration::seconds(3).whole_milliseconds() as usize
        ));
        // For Window Wheel: Worst-case number of aggregate operations =>
        // 1. last_rotation: (10⊕), ammortized 0 ⊕ calls when it is cached. (10 coin)
        // 2. inverse_combine: (1) ⊕ inverse_slice (1 coin)
        // 3. (2) ⊕  seconds interval (10)⊕ (10 coins)
        // Worst-case: Number of aggregate operations needed: 10 + 1 + 10? (21)
    }

    #[test]
    fn window_poc_test() {
        // 10s range, 2 slide
        // NOTE: We don't need inverse when range and slide are within same HAW granularity
        // for example: in this case we can always just interval the last 10 seconds to get correct window result.
        let mut wheel: Wheel<U64SumAggregator> = Wheel::new(0);
        // w1
        wheel.insert(Entry::new(1, 1000)).unwrap();
        // w2
        wheel.insert(Entry::new(1, 3000)).unwrap();
        // w3
        wheel.insert(Entry::new(1, 4000)).unwrap();
        // w3
        wheel.insert(Entry::new(1, 5000)).unwrap();
        // w4
        wheel.insert(Entry::new(1, 6000)).unwrap();
        // w4
        wheel.insert(Entry::new(1, 7000)).unwrap();
        // w5
        wheel.insert(Entry::new(1, 9000)).unwrap();

        // w6
        wheel.insert(Entry::new(10, 11000)).unwrap();

        // advance to 10
        wheel.advance_to(10000);

        let w1_result = wheel.seconds_unchecked().interval(10);
        assert_eq!(w1_result, Some(7));

        // next window is at 12 [2-12] and it should not include data from [0-2]
        wheel.advance_to(12000);
        let last_10_sec = wheel.seconds_unchecked().interval(10).unwrap();
        // result w2 should be is 6+10=16
        assert_eq!(last_10_sec, 16);
    }
    #[test]
    fn window_poc_two_test() {
        // 60s range, 10s slide
        let mut iwheel: InverseWheel<U64SumAggregator> = InverseWheel::with_capacity(64);
        let mut wheel: Wheel<U64SumAggregator> = Wheel::new(0);
        let aggregator = U64SumAggregator;
        // w1
        wheel.insert(Entry::new(1, 9000)).unwrap();
        iwheel.push(1, &Default::default());
        // w2
        wheel.insert(Entry::new(1, 15000)).unwrap();
        iwheel.push(1, &Default::default());
        // w3
        wheel.insert(Entry::new(1, 25000)).unwrap();
        iwheel.push(1, &Default::default());
        // w3
        wheel.insert(Entry::new(1, 35000)).unwrap();
        iwheel.push(1, &Default::default());

        iwheel.push(0, &Default::default());

        // w4
        wheel.insert(Entry::new(1, 59000)).unwrap();
        iwheel.push(1, &Default::default());

        // advance to 60s
        wheel.advance_to(60000);

        // w4
        wheel.insert(Entry::new(3, 69000)).unwrap();
        iwheel.push(3, &Default::default());

        // [0-60]
        let w1_result = wheel.minutes_unchecked().interval(1);
        assert_eq!(w1_result, Some(5));

        // [10-70]
        // advance to 70s
        wheel.advance_to(70000);

        // w5
        wheel.insert(Entry::new(5, 75000)).unwrap();
        iwheel.push(5, &Default::default());
        iwheel.push(0, &Default::default());
        iwheel.push(0, &Default::default());
        iwheel.push(0, &Default::default());

        // w6
        wheel.insert(Entry::new(10, 110000)).unwrap();
        iwheel.push(10, &Default::default());

        let last_min = wheel.minutes_unchecked().interval(1).unwrap();
        //let last_ten = wheel.seconds_unchecked().interval(10).unwrap();
        let last_ten = wheel.seconds_unchecked().total().unwrap();
        let inverse = iwheel.tick().unwrap(); // data within [0-10]
        dbg!((last_min, last_ten, inverse));
        let inversed_min = aggregator.inverse_combine(last_min, inverse);
        let w2_result = aggregator.combine(inversed_min, last_ten);
        // 4 + 3
        assert_eq!(w2_result, 7);

        // [20-80]
        // advance to 80s
        // now inverse [0-20]
        wheel.advance_to(80000);
        //let inverse = 1 + 1; // combine [0-20] (1 at 9000 and 1 at 15000)
        let inverse = iwheel.tick().unwrap(); // data within [0-10]
        let last_min = wheel.minutes_unchecked().interval(1).unwrap();
        let last_twenty = wheel.seconds_unchecked().total().unwrap();
        let inversed_min = aggregator.inverse_combine(last_min, inverse);
        let w3_result = inversed_min + last_twenty;
        // [20-80] should be 11
        assert_eq!(w3_result, 11);

        // [30-90]
        // advance to 90s
        // now inverse [0-30]
        wheel.advance_to(90000);
        let inverse = iwheel.tick().unwrap(); // data within [0-30]
        let last_min = wheel.minutes_unchecked().interval(1).unwrap();
        // [0-60] (Minutes(1)) - [0-30] (inverse wheel) + [60-90] (seconds.total())
        let last_30 = wheel.seconds_unchecked().total().unwrap();
        let inversed_min = aggregator.inverse_combine(last_min, inverse);
        let w3_result = inversed_min + last_30;
        // [30-90] should be 10
        assert_eq!(w3_result, 10);

        // [40-100]
        // advance to 100s
        // now inverse [0-40]
        wheel.advance_to(100000);
        let inverse = iwheel.tick().unwrap(); // data within [0-40]
        let last_min = wheel.minutes_unchecked().interval(1).unwrap();
        // [0-60] (Minutes(1)) - [0-30] (inverse wheel) + [60-90] (seconds.total())
        let total = wheel.seconds_unchecked().total().unwrap();
        let inversed_min = aggregator.inverse_combine(last_min, inverse);
        let w4_result = inversed_min + total;
        // [40-100] should be 9
        assert_eq!(w4_result, 9);

        // [50-110]
        // advance to 110s
        // now inverse [0-50]
        wheel.advance_to(110000);
        let inverse = iwheel.tick().unwrap(); // data within [0-50]
        let last_min = wheel.minutes_unchecked().interval(1).unwrap();
        let total = wheel.seconds_unchecked().total().unwrap();
        let inversed_min = aggregator.inverse_combine(last_min, inverse);
        let w5_result = inversed_min + total;
        // [50-110] should be ?
        assert_eq!(w5_result, 9);

        // [60-120]
        // advance to 120s
        // Don't need to inverse when we caused a full rotation again into minutes wheel
        wheel.advance_to(120000);
        iwheel.clear_tail_and_tick();
        let last_min = wheel.minutes_unchecked().interval(1).unwrap();
        let w6_result = last_min;
        assert_eq!(w6_result, 18);

        // [70-130]
        // advance to 130s
        // [60-70] should contain 3 which we should inverse
        wheel.advance_to(130000);
        let inverse = iwheel.tick().unwrap_or_default();
        dbg!(inverse);
        let last_min = wheel.minutes_unchecked().interval(1).unwrap();
        dbg!(last_min);
        let total = wheel.seconds_unchecked().total().unwrap_or_default();
        let inversed_min = aggregator.inverse_combine(last_min, inverse);
        let w7_result = inversed_min + total;
        // [70-130] should be 18-3 = 15
        assert_eq!(w7_result, 15);
    }

    fn window_60_sec_range_10_sec_slide(mut wheel: impl WindowWheel<U64SumAggregator>) {
        // w1
        wheel.insert(Entry::new(1, 9000)).unwrap();
        // w2
        wheel.insert(Entry::new(1, 15000)).unwrap();
        // w3
        wheel.insert(Entry::new(1, 25000)).unwrap();
        // w3
        wheel.insert(Entry::new(1, 35000)).unwrap();

        // w4
        wheel.insert(Entry::new(1, 59000)).unwrap();

        wheel.advance_to(10000);
        wheel.advance_to(20000);
        wheel.advance_to(30000);
        wheel.advance_to(40000);
        wheel.advance_to(50000);
        wheel.advance_to(60000);

        wheel.insert(Entry::new(3, 69000)).unwrap();

        wheel.advance_to(70000);

        wheel.insert(Entry::new(5, 75000)).unwrap();

        wheel.insert(Entry::new(10, 110000)).unwrap();

        wheel.advance_to(80000);
        wheel.advance_to(90000);
        wheel.advance_to(100000);
        wheel.advance_to(110000);
        wheel.advance_to(120000);
        wheel.advance_to(130000);

        let results = wheel.results();
        let expected = &[5, 7, 11, 10, 9, 9, 18, 15];
        assert_eq!(results, expected);
    }
    #[test]
    fn window_60_sec_range_10_sec_slide_lazy_test() {
        let wheel: LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(Duration::minutes(1))
            .with_slide(Duration::seconds(10))
            .build();
        window_60_sec_range_10_sec_slide(wheel);
    }
    #[test]
    fn window_60_sec_range_10_sec_slide_eager_test() {
        let wheel: EagerWindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::minutes(1))
            .with_slide(Duration::seconds(10))
            .build();
        window_60_sec_range_10_sec_slide(wheel);
    }

    fn window_120_sec_range_10_sec_slide(mut wheel: impl WindowWheel<U64SumAggregator>) {
        // w1
        wheel.insert(Entry::new(1, 9000)).unwrap();
        // w2
        wheel.insert(Entry::new(1, 15000)).unwrap();
        // w3
        wheel.insert(Entry::new(1, 25000)).unwrap();
        // w3
        wheel.insert(Entry::new(1, 35000)).unwrap();

        // w4
        wheel.insert(Entry::new(1, 59000)).unwrap();

        wheel.advance_to(10000);
        wheel.advance_to(20000);
        wheel.advance_to(30000);
        wheel.advance_to(40000);
        wheel.advance_to(50000);
        wheel.advance_to(60000);

        wheel.insert(Entry::new(3, 69000)).unwrap();

        wheel.advance_to(70000);

        wheel.insert(Entry::new(5, 75000)).unwrap();

        wheel.insert(Entry::new(10, 110000)).unwrap();

        wheel.advance_to(80000);
        wheel.advance_to(90000);
        wheel.advance_to(100000);
        wheel.advance_to(110000);
        // First window triggered
        wheel.advance_to(120000);

        wheel.insert(Entry::new(3, 125000)).unwrap();

        // 2nd window triggered [10-130] -> should be (23 - 1) + 3 = 25
        wheel.advance_to(130000);

        // 3nd window triggered [20-140] -> should be (25 -1)
        wheel.advance_to(140000);

        // 4nd window triggered [30-150] -> should be (24-1) = 23
        wheel.advance_to(150000);

        // 5nd window triggered [40-160] -> should be (23 -1 ) = 22
        wheel.advance_to(160000);

        let results = wheel.results();
        let expected = &[23, 25, 24, 23, 22];
        assert_eq!(results, expected);
    }

    #[test]
    fn window_2_min_range_10_sec_slide_eager_test() {
        let wheel: EagerWindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::minutes(2))
            .with_slide(Duration::seconds(10))
            .build();

        window_120_sec_range_10_sec_slide(wheel);
    }
    #[test]
    fn window_2_min_range_10_sec_slide_lazy_test() {
        let wheel: LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(Duration::minutes(2))
            .with_slide(Duration::seconds(10))
            .build();

        window_120_sec_range_10_sec_slide(wheel);
    }
    #[test]
    fn window_10_sec_range_3_sec_slide_lazy_test() {
        let wheel: LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(Duration::seconds(10))
            .with_slide(Duration::seconds(3))
            .build();
        window_10_sec_range_3_sec_slide(wheel);
    }
    #[test]
    fn window_10_sec_range_3_sec_slide_eager_test() {
        let wheel: EagerWindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::seconds(10))
            .with_slide(Duration::seconds(3))
            .build();
        window_10_sec_range_3_sec_slide(wheel);
    }

    fn window_10_sec_range_3_sec_slide(mut wheel: impl WindowWheel<U64SumAggregator>) {
        // Based on Figure 4 in https://asterios.katsifodimos.com/assets/publications/window-semantics-encyclopediaBigDAta18.pdf
        for i in 1..=16 {
            wheel.insert(Entry::new(i, i * 1000 - 1)).unwrap();
        }
        for i in 1..=16 {
            wheel.advance_to(i * 1000);
        }

        // w1: reduce[1..=10] = 55
        // w2: reduce[4..=13] = 85
        // w3: reduce[7..=16] = 115

        let results = wheel.results();
        let expected = &[55, 85, 115];
        assert_eq!(results, expected);
    }

    #[test]
    fn window_wheel_range_1hr_slide_10_secs() {
        let mut wheel: EagerWindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::hours(1))
            .with_slide(Duration::seconds(10))
            .build();
        // w1
        wheel.insert(Entry::new(1, 9000)).unwrap();
        // w2
        wheel.insert(Entry::new(1, 15000)).unwrap();
        // w3
        wheel.insert(Entry::new(1, 25000)).unwrap();
        // w3
        wheel.insert(Entry::new(1, 35000)).unwrap();

        // w4
        wheel.insert(Entry::new(1, 59000)).unwrap();

        wheel.advance_to(10000);
        wheel.advance_to(20000);
        wheel.advance_to(30000);
        wheel.advance_to(40000);
        wheel.advance_to(50000);
        wheel.advance_to(60000);

        wheel.insert(Entry::new(3, 69000)).unwrap();

        wheel.advance_to(70000);

        wheel.insert(Entry::new(5, 75000)).unwrap();

        wheel.insert(Entry::new(10, 110000)).unwrap();

        wheel.advance_to(80000);
        wheel.advance_to(90000);
        wheel.advance_to(100000);
        wheel.advance_to(110000);
        // First window triggered
        wheel.advance_to(120000);

        wheel.insert(Entry::new(3, 125000)).unwrap();

        // 2nd window triggered [10-130] -> should be (23 - 1) + 3 = 25
        wheel.advance_to(130000);

        // 3nd window triggered [20-140] -> should be (25 -1)
        wheel.advance_to(140000);

        // 4nd window triggered [30-150] -> should be (24-1) = 23
        wheel.advance_to(150000);

        // 5nd window triggered [40-160] -> should be (23 -1 ) = 22
        wheel.advance_to(160000);

        let mut time = 170000u64;
        while time < 3600000 {
            wheel.advance_to(time);
            time += 10000;
        }
        wheel.advance_to(3600000 + 10000);

        wheel.advance_to(3600000 + 20000);

        wheel.advance_to(3600000 + 30000);
        wheel.advance_to(3600000 + 40000);
        let results = wheel.results();
        let expected = &[26, 25, 24, 23];
        assert_eq!(results, expected);

        //wheel.wheel().interval(Duration::seconds((86400 * 2) - 10));
        wheel.wheel().interval(Duration::seconds(
            Duration::weeks(50).whole_seconds() - 10_i64,
        ));
    }
}
