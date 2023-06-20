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
    pub fn build<A: Aggregator + InverseExt>(self) -> InvertibleWindowWheel<A> {
        // TODO: sanity check of range and slide
        InvertibleWindowWheel::new(self.time, self.range, self.slide)
    }
}

/// Wrapper on top of HAW to implement Sliding Window Aggregation
///
/// Requires an aggregation function that supports invertibility
#[allow(dead_code)]
pub struct InvertibleWindowWheel<A: Aggregator + InverseExt> {
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

impl<A: Aggregator + InverseExt> InvertibleWindowWheel<A> {
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
    // Currently assumes per SLIDE advance call
    pub fn advance_to(&mut self, new_watermark: u64) {
        //let diff = new_watermark.saturating_sub(self.wheel.watermark());
        //let ticks = diff / SLIDE as u64;

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
                //let last_rotation = self.wheel.interval(self.range_interval_duration()).unwrap();
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
                //dbg!(window_result);
            }
            self.next_window_end += self.slide as u64;
        }

        // Make sure we have advanced to the new watermark
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
        let mut wheel: InvertibleWindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::minutes(1))
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
        wheel.advance_to(120000);
        wheel.advance_to(130000);

        let results = wheel.results();
        let expected = &[5, 7, 11, 10, 9, 9, 18, 15];
        assert_eq!(results, expected);
    }
    #[test]
    fn window_wheel_range_120_slide_10() {
        // 120s range, 10s slide
        let mut wheel: InvertibleWindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::minutes(2))
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

        let results = wheel.results();
        let expected = &[23, 25, 24, 23, 22];
        assert_eq!(results, expected);
    }

    #[test]
    fn window_wheel_range_1hr_slide_10_secs() {
        let mut wheel: InvertibleWindowWheel<U64SumAggregator> = Builder::default()
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
    #[test]
    fn window_10_sec_range_3_sec_slide_test() {
        let mut wheel: InvertibleWindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::seconds(10))
            .with_slide(Duration::seconds(3))
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

        wheel.advance_to(3000);
        wheel.advance_to(3000 * 2);
        wheel.advance_to(3000 * 3);
        wheel.advance_to(3000 * 4);
        wheel.advance_to(3000 * 5);

        //let results = wheel.results();
        //let expected = &[1, 1, 1, 1];
        //assert_eq!(results, expected);
    }
}
