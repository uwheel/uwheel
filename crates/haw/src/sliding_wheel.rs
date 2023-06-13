use crate::{
    aggregator::{Aggregator, Inverse},
    inverse_wheel::InverseWheel,
    Entry,
    Error,
    Wheel,
};

/*
#[derive(Debug, Clone, Copy)]
pub enum PairType {
    // Slice size when range % slide = 0
    Even(usize),
    // Two pairs (p1, p2) when range % slide != 0
    Uneven(usize, usize),
}

impl PairType {}

fn create_pair_type(range: usize, slide: usize) -> PairType {
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
pub const fn space(range: usize, slide: usize) -> usize {
    assert!(range >= slide, "Range needs to be larger than slide");
    let p2 = range % slide;
    if p2 == 0 {
        range / slide
    } else {
        ceil_div(2 * range, slide)
    }
}

pub struct SlidingWheel<const RANGE: usize, const SLIDE: usize, A: Aggregator>
where
    [(); capacity(RANGE, SLIDE)]: Sized,
{
    waw: Waw<64, A>,
    watermark: u64,
    // Eager Wheel: Perform eager calculation? Requires more slots.
    // But maintain O(1) complexity for query and update in contrast to eager versions in cutty paper
    // [x0(head), x1, x2, x3, x4]
    // maintain at max 8640 slides/panes.
    // when slide has been reached (i.e. when new window starts): take current slide and insert into inverse wheel.
    //inverse_wheel: Waw<{ capacity(RANGE, SLIDE) }, A>,
    wheel: AggregationWheel<{ capacity(RANGE, SLIDE) }, A>,
    pair_type: PairType,
    tick_unit: u64,
    next_tick_ms: u64,
    // tick_size: usize,
    aggregator: A,
}
impl<const RANGE: usize, const SLIDE: usize, A: Aggregator> SlidingWheel<RANGE, SLIDE, A>
where
    [(); capacity(RANGE, SLIDE)]: Sized,
{
    pub fn new() -> Self {
        let space = space(RANGE, SLIDE);
        let pair_type = create_pair_type(RANGE, SLIDE);
        let tick_unit = match pair_type {
            PairType::Even(slide) => slide,
            PairType::Uneven(p1, _) => p1,
        } as u64;

        Self {
            waw: Waw::default(),
            watermark: 0,
            wheel: AggregationWheel::with_capacity(space),
            tick_unit,
            next_tick_ms: 0 + tick_unit,
            pair_type,
            aggregator: Default::default(),
        }
    }
    pub fn advance_to(&mut self, watermark: u64) {
        let diff = watermark - self.watermark;
        let ticks = diff / self.tick_unit;
        for _i in 0..ticks {
            self.tick();
        }
        // let full = P2;
        // tick += p1;
        // if 0 then tick (P1) causes 1 shift.
        // if !0 then tick(P1) bumps tick
        // if tick == P2 then Causes 1 shift.
        // reset to 0

        // bump time here?
    }
    pub fn tick(&mut self) {
        self.watermark += self.tick_unit;
        dbg!(self.watermark);

        // when ticking we insert (1) slice into head; (2) update next window to be fired
        // pre-compute window result.
        // Do we need two wheels?

        // [0, 1, 2, 3]
        // when ticking we can pre-aggregate 1: the next window
        // when window fires it can be returned in O(1)

        // oldest_window_total: wheel.total();

        // Tick the write-ahead wheel and insert into aggregation wheel if there is any entry
        if let Some(window) = self.waw.tick() {
            let partial_agg = self.aggregator.lift(window);
            self.wheel.insert_head(partial_agg, &self.aggregator);
        }

        // Tick aggregation wheel
        if let Some(_) = self.wheel.tick() {
            // TODO: Can we reuse the aggregates and insert into HAW?
        }
    }
    pub fn wheel(&self) -> &AggregationWheel<{ capacity(RANGE, SLIDE) }, A> {
        &self.wheel
    }
    #[inline]
    pub fn query(&mut self) -> Option<A::Aggregate> {
        let slots = space(RANGE, SLIDE);
        self.wheel.lower_interval(slots)
    }
    pub fn insert(&mut self, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        let watermark = self.watermark;

        // If timestamp is below the watermark, then reject it.
        if entry.timestamp < watermark {
            Err(Error::Late { entry, watermark })
        } else {
            // TODO: calculate based on current watermark and PairType where to insert
            // Based on current wm: are we at P1 or P2?

            // time diff
            let diff = entry.timestamp - self.watermark;
            let slots = diff / self.tick_unit;
            /*
            let slots = match self.pair_type {
                PairType::Even(slice) => diff / slice as u64,
                PairType::Uneven(p1, p2) => {
                    // TODO: when we have unequal pair sizes, how to locate which slot to write into?
                    // Are we currently (wm) in P1 or P2?
                    unimplemented!();
                }
            };
            */
            dbg!(slots);
            // calculate how many slices forward we can write.
            // This depends on if PairType is even or uneven.
            // if even we can calculate easily which slice
            if self.waw.can_write_ahead(slots) {
                self.waw.write_ahead(slots, entry.data, &self.aggregator);
                Ok(())
            } else {
                // cannot fit within the write-ahead wheel, return it to the user to handle it..
                let write_ahead_ms =
                    Duration::from_secs(self.waw.write_ahead_len() as u64).as_millis();
                let max_write_ahead_ts = self.watermark + write_ahead_ms as u64;
                Err(Error::Overflow {
                    entry,
                    max_write_ahead_ts,
                })
            }
        }
    }
}
*/

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

// Wrapper on top of HAW to implement Sliding Window Aggregation
// Idea: Support Sliding Window Aggregation using HAW + Support OLAP queries
#[allow(dead_code)]
pub struct WindowWheel<const RANGE: usize, const SLIDE: usize, A: Aggregator + Inverse>
where
    [(); capacity(RANGE, SLIDE)]: Sized,
{
    // Inverse Wheel maintaing partial aggregates per slide
    inverse_wheel: InverseWheel<A>,
    // Regular HAW used together with inverse_wheel to answer a specific Sliding Window
    wheel: Wheel<A>,
    // When the next window starts
    next_window_start: u64,
    // When next full rotation has happend
    next_full_rotation: u64,
    aggregator: A,
}

impl<const RANGE: usize, const SLIDE: usize, A: Aggregator + Inverse> WindowWheel<RANGE, SLIDE, A>
where
    [(); capacity(RANGE, SLIDE)]: Sized,
{
    pub fn new(time: u64) -> Self {
        let capacity = capacity(RANGE, SLIDE);
        dbg!(capacity);
        Self {
            inverse_wheel: InverseWheel::with_capacity(capacity),
            wheel: Wheel::new(time),
            next_window_start: time + SLIDE as u64,
            next_full_rotation: time + RANGE as u64,
            aggregator: Default::default(),
        }
    }
    // should it return Window results? i.e. Vec<A::Aggregate>
    pub fn advance_to(&mut self, watermark: u64) {
        // Here we need to introduce our logic
        // If we reach a new window start, then we add last X slide into InverseWheel
        // Partial aggregates between [WBegin - WBegin']
        // For example the last 10 seconds, last 2 hours. Depends on slide granularity
        self.wheel.advance_to(watermark);
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
        let aggregator = U64SumAggregator::default();
        // w1
        wheel.insert(Entry::new(1, 9000)).unwrap();
        iwheel.write_ahead(0, 1, &Default::default());
        // w2
        wheel.insert(Entry::new(1, 15000)).unwrap();
        iwheel.write_ahead(1, 1, &Default::default());
        // w3
        wheel.insert(Entry::new(1, 25000)).unwrap();
        iwheel.write_ahead(2, 1, &Default::default());
        // w3
        wheel.insert(Entry::new(1, 35000)).unwrap();
        iwheel.write_ahead(3, 1, &Default::default());

        iwheel.write_ahead(4, 0, &Default::default());
        // w4
        wheel.insert(Entry::new(1, 59000)).unwrap();
        iwheel.write_ahead(5, 1, &Default::default());

        // advance to 60s
        wheel.advance_to(60000);

        // w4
        wheel.insert(Entry::new(3, 69000)).unwrap();
        iwheel.write_ahead(6, 3, &Default::default());

        // [0-60]
        let w1_result = wheel.minutes_unchecked().interval(1);
        assert_eq!(w1_result, Some(5));

        // [10-70]
        // advance to 70s
        wheel.advance_to(70000);

        // w5
        wheel.insert(Entry::new(5, 75000)).unwrap();
        iwheel.write_ahead(7, 5, &Default::default());
        iwheel.write_ahead(8, 0, &Default::default());
        iwheel.write_ahead(9, 0, &Default::default());
        iwheel.write_ahead(10, 0, &Default::default());

        // w6
        wheel.insert(Entry::new(10, 110000)).unwrap();
        iwheel.write_ahead(11, 10, &Default::default());

        let last_min = wheel.minutes_unchecked().interval(1).unwrap();
        //let last_ten = wheel.seconds_unchecked().interval(10).unwrap();
        let last_ten = wheel.seconds_unchecked().total().unwrap();
        let inverse = iwheel.tick().unwrap(); // data within [0-10]
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
        iwheel.clear_current_tail();
        let inverse = iwheel.tick().unwrap();
        dbg!(inverse);
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
    #[test]
    fn window_wheel_real_test() {
        // 60s range, 10s slide
        let mut wheel: WindowWheel<60, 10, U64SumAggregator> = WindowWheel::new(0);
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

        // advance to 60s
        wheel.advance_to(60000);
    }
}