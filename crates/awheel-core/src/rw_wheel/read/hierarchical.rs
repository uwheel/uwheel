use core::{
    cmp,
    fmt::{self, Display},
    iter::IntoIterator,
    marker::PhantomData,
    ops::RangeBounds,
    option::{
        Option,
        Option::{None, Some},
    },
};
use time::OffsetDateTime;

use super::{
    super::write::WriteAheadWheel,
    aggregation::{maybe::MaybeWheel, AggregationWheel},
    Kind,
    Lazy,
};
use crate::{
    aggregator::Aggregator,
    rw_wheel::read::{
        aggregation::{combine_or_insert, WheelSlot},
        Mode,
    },
    time_internal::{self, Duration},
};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "profiler")]
use super::stats::Stats;
#[cfg(feature = "profiler")]
use awheel_stats::profile_scope;

crate::cfg_timer! {
    #[cfg(not(feature = "std"))]
    use alloc::{boxed::Box, rc::Rc};
    #[cfg(feature = "std")]
    use std::rc::Rc;
    use crate::rw_wheel::timer::{RawTimerWheel, TimerError, TimerAction};
    use core::cell::RefCell;
}
use super::aggregation::conf::WheelConf;

/// Default Second tick represented in milliseconds
pub const SECOND_TICK_MS: u64 = time::Duration::SECOND.whole_milliseconds() as u64;
/// Default Minute tick represented in milliseconds
pub const MINUTE_TICK_MS: u64 = time::Duration::MINUTE.whole_milliseconds() as u64;
/// Default Hour tick represented in milliseconds
pub const HOUR_TICK_MS: u64 = time::Duration::HOUR.whole_milliseconds() as u64;
/// Default Day tick represented in milliseconds
pub const DAY_TICK_MS: u64 = time::Duration::DAY.whole_milliseconds() as u64;
/// Default Week tick represented in milliseconds
pub const WEEK_TICK_MS: u64 = time::Duration::WEEK.whole_milliseconds() as u64;
/// Default Year tick represented in milliseconds
pub const YEAR_TICK_MS: u64 = WEEK_TICK_MS * 52;

/// Configuration for a Hierarchical Aggregation Wheel
#[derive(Clone, Copy, Debug)]
pub struct HawConf {
    /// Initial watermark of the wheel
    pub watermark: u64,
    /// Config for the seconds wheel
    pub seconds: WheelConf,
    /// Config for the minutes wheel
    pub minutes: WheelConf,
    /// Config for the hours wheel
    pub hours: WheelConf,
    /// Config for the days wheel
    pub days: WheelConf,
    /// Config for the weeks wheel
    pub weeks: WheelConf,
    /// Config for the years wheel
    pub years: WheelConf,
}

impl Default for HawConf {
    fn default() -> Self {
        Self {
            watermark: 0,
            seconds: WheelConf::new(SECOND_TICK_MS, SECONDS),
            minutes: WheelConf::new(MINUTE_TICK_MS, MINUTES),
            hours: WheelConf::new(HOUR_TICK_MS, HOURS),
            days: WheelConf::new(DAY_TICK_MS, DAYS),
            weeks: WheelConf::new(WEEK_TICK_MS, WEEKS),
            years: WheelConf::new(YEAR_TICK_MS, YEARS),
        }
    }
}

impl HawConf {
    /// Configures the initial watermark
    pub fn with_watermark(mut self, watermark: u64) -> Self {
        self.seconds.set_watermark(watermark);
        self.minutes.set_watermark(watermark);
        self.hours.set_watermark(watermark);
        self.days.set_watermark(watermark);
        self.weeks.set_watermark(watermark);
        self.years.set_watermark(watermark);

        self.watermark = watermark;
        self
    }
    /// Configures the seconds granularity
    pub fn with_seconds(mut self, seconds: WheelConf) -> Self {
        self.seconds = seconds;
        self
    }
    /// Configures the minutes granularity
    pub fn with_minutes(mut self, minutes: WheelConf) -> Self {
        self.minutes = minutes;
        self
    }
    /// Configures the hours granularity
    pub fn with_hours(mut self, hours: WheelConf) -> Self {
        self.hours = hours;
        self
    }
    /// Configures the days granularity
    pub fn with_days(mut self, days: WheelConf) -> Self {
        self.days = days;
        self
    }
    /// Configures the minutes granularity
    pub fn with_weeks(mut self, weeks: WheelConf) -> Self {
        self.weeks = weeks;
        self
    }
    /// Configures the years granularity
    pub fn with_years(mut self, years: WheelConf) -> Self {
        self.years = years;
        self
    }
}

/// Default capacity of second slots
pub const SECONDS: usize = 60;
/// Default capacity of minute slots
pub const MINUTES: usize = 60;
/// Default capacity of hour slots
pub const HOURS: usize = 24;
/// Default capacity of day slots
pub const DAYS: usize = 7;
/// Default capacity of week slots
pub const WEEKS: usize = 52;
/// Default capacity of year slots
pub const YEARS: usize = 10;

#[derive(Debug)]
struct Granularities {
    pub second: Option<usize>,
    pub minute: Option<usize>,
    pub hour: Option<usize>,
    pub day: Option<usize>,
    pub week: Option<usize>,
    pub year: Option<usize>,
}

/// Hierarchical Aggregation Wheel
///
/// Similarly to Hierarchical Wheel Timers, HAW exploits the hierarchical nature of time and utilise several aggregation wheels,
/// each with a different time granularity. This enables a compact representation of aggregates across time
/// with a low memory footprint and makes it highly compressible and efficient to store on disk. HAWs are event-time driven and uses the notion of a Watermark which means that no timestamps are stored as they are implicit in the wheel slots. It is up to the user of the wheel to advance the watermark and thus roll up aggregates continously up the time hierarchy.

/// For instance, to store aggregates with second granularity up to 10 years, we would need the following aggregation wheels:

/// * Seconds wheel with 60 slots
/// * Minutes wheel with 60 slots
/// * Hours wheel with 24 slots
/// * Days wheel with 7 slots
/// * Weeks wheel with 52 slots
/// * Years wheel with 10 slots
///
/// The above scheme results in a total of 213 wheel slots. This is the minimum number of slots
/// required to support rolling up aggregates across 10 years with second granularity.
#[repr(C)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone)]
pub struct Haw<A, K = Lazy>
where
    A: Aggregator,
    K: Kind,
{
    watermark: u64,
    seconds_wheel: MaybeWheel<A>,
    minutes_wheel: MaybeWheel<A>,
    hours_wheel: MaybeWheel<A>,
    days_wheel: MaybeWheel<A>,
    weeks_wheel: MaybeWheel<A>,
    years_wheel: MaybeWheel<A>,
    #[cfg_attr(feature = "serde", serde(skip))]
    #[cfg(feature = "timer")]
    timer: Rc<RefCell<RawTimerWheel<TimerAction<A, K>>>>,
    _marker: PhantomData<K>,
    #[cfg(feature = "profiler")]
    stats: Stats,
}

impl<A, K> Haw<A, K>
where
    A: Aggregator,
    K: Kind,
{
    const MINUTES_AS_SECS: u64 = time::Duration::MINUTE.whole_seconds() as u64;
    const HOURS_AS_SECS: u64 = time::Duration::HOUR.whole_seconds() as u64;
    const DAYS_AS_SECS: u64 = time::Duration::DAY.whole_seconds() as u64;
    const WEEK_AS_SECS: u64 = time::Duration::WEEK.whole_seconds() as u64;
    const YEAR_AS_SECS: u64 = Self::WEEK_AS_SECS * WEEKS as u64;
    const CYCLE_LENGTH_SECS: u64 = Self::CYCLE_LENGTH.whole_seconds() as u64;

    const SECOND_AS_MS: u64 = time::Duration::SECOND.whole_milliseconds() as u64;

    const TOTAL_SECS_IN_WHEEL: u64 = Self::YEAR_AS_SECS * YEARS as u64;
    /// Duration of a full wheel cycle
    pub const CYCLE_LENGTH: time::Duration =
        time::Duration::seconds((Self::YEAR_AS_SECS * (YEARS as u64 + 1)) as i64); // need 1 extra to force full cycle rotation
    /// Total number of wheel slots across all granularities
    pub const TOTAL_WHEEL_SLOTS: usize = SECONDS + MINUTES + HOURS + DAYS + WEEKS + YEARS;

    /// Creates a new Wheel starting from the given time and configuration
    ///
    /// Time is represented as milliseconds
    pub fn new(time: u64, conf: HawConf) -> Self {
        let mut seconds = conf.seconds;
        seconds.set_watermark(time);

        let mut minutes = conf.minutes;
        minutes.set_watermark(time);

        let mut hours = conf.hours;
        hours.set_watermark(time);

        let mut days = conf.days;
        days.set_watermark(time);

        let mut weeks = conf.weeks;
        weeks.set_watermark(time);

        let mut years = conf.years;
        years.set_watermark(time);

        Self {
            watermark: time,
            seconds_wheel: MaybeWheel::new(seconds),
            minutes_wheel: MaybeWheel::new(minutes),
            hours_wheel: MaybeWheel::new(hours),
            days_wheel: MaybeWheel::new(days),
            weeks_wheel: MaybeWheel::new(weeks),
            years_wheel: MaybeWheel::new(years),
            #[cfg(feature = "timer")]
            timer: Rc::new(RefCell::new(RawTimerWheel::default())),
            _marker: PhantomData,
            #[cfg(feature = "profiler")]
            stats: Stats::default(),
        }
    }

    /// Returns how many wheel slots are utilised
    pub fn len(&self) -> usize {
        self.seconds_wheel.len()
            + self.minutes_wheel.len()
            + self.hours_wheel.len()
            + self.days_wheel.len()
            + self.weeks_wheel.len()
            + self.years_wheel.len()
    }

    /// Returns true if the internal wheel time has never been advanced
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if all slots in the hierarchy are utilised
    pub fn is_full(&self) -> bool {
        self.len() == Self::TOTAL_WHEEL_SLOTS
    }
    /// Returns memory used in bytes for all levels
    pub fn size_bytes(&self) -> usize {
        let secs = self.seconds_wheel.size_bytes();
        let min = self.minutes_wheel.size_bytes();
        let hr = self.hours_wheel.size_bytes();
        let day = self.days_wheel.size_bytes();
        let week = self.weeks_wheel.size_bytes();
        let year = self.years_wheel.size_bytes();

        secs + min + hr + day + week + year
    }

    /// Returns how many ticks (seconds) are left until the wheel is fully utilised
    pub fn remaining_ticks(&self) -> u64 {
        Self::TOTAL_SECS_IN_WHEEL - self.current_time_in_cycle().whole_seconds() as u64
    }

    /// Returns Duration that represents where the wheel currently is in its cycle
    #[inline]
    pub fn current_time_in_cycle(&self) -> time_internal::Duration {
        let secs = self.seconds_wheel.rotation_count() as u64;
        let min_secs = self.minutes_wheel.rotation_count() as u64 * Self::MINUTES_AS_SECS;
        let hr_secs = self.hours_wheel.rotation_count() as u64 * Self::HOURS_AS_SECS;
        let day_secs = self.days_wheel.rotation_count() as u64 * Self::DAYS_AS_SECS;
        let week_secs = self.weeks_wheel.rotation_count() as u64 * Self::WEEK_AS_SECS;
        let year_secs = self.years_wheel.rotation_count() as u64 * Self::YEAR_AS_SECS;
        let cycle_time = secs + min_secs + hr_secs + day_secs + week_secs + year_secs;
        time_internal::Duration::seconds(cycle_time as i64)
    }

    /// Advance the watermark of the wheel by the given [time::Duration]
    #[inline(always)]
    pub fn advance(&mut self, duration: time_internal::Duration, waw: &mut WriteAheadWheel<A>) {
        let ticks: usize = duration.whole_seconds() as usize;

        // helper fn to tick N times
        let tick_n = |ticks: usize, haw: &mut Self, waw: &mut WriteAheadWheel<A>| {
            for _ in 0..ticks {
                // tick the write wheel and freeze mutable aggregate
                haw.tick(waw.tick().map(|m| A::freeze(m)));
            }
        };
        if ticks <= Self::CYCLE_LENGTH_SECS as usize {
            tick_n(ticks, self, waw);
        } else {
            // Exceeds full cycle length, clear all!
            self.clear();
        }

        // if ticks <= SECONDS {
        //     tick_n(ticks, self, waw);
        // } else if ticks <= Self::CYCLE_LENGTH_SECS as usize {
        //     // force full rotation
        //     let rem_ticks = self.seconds_wheel.get_or_insert().ticks_remaining();
        //     tick_n(rem_ticks, self, waw);
        //     ticks -= rem_ticks;

        //     // calculate how many fast_ticks we can perform
        //     let fast_ticks = ticks.saturating_div(SECONDS);

        //     if fast_ticks == 0 {
        //         // if fast ticks is 0, then tick normally
        //         tick_n(ticks, self, waw);
        //     } else {
        //         // perform a number of fast ticks
        //         // NOTE: currently a fast tick is a full SECONDS rotation.
        //         let fast_tick_ms = (SECONDS - 1) as u64 * Self::SECOND_AS_MS;
        //         for _ in 0..fast_ticks {
        //             self.seconds_wheel.get_or_insert().fast_skip_tick();
        //             self.watermark += fast_tick_ms;
        //             *waw.watermark_mut() += fast_tick_ms;
        //             self.tick(waw.tick().map(|m| A::freeze(m)));
        //             ticks -= SECONDS;
        //         }
        //         // tick any remaining ticks
        //         tick_n(ticks, self, waw);
        //     }
    }

    /// Advances the watermark by the given duration and returns deltas that were applied
    #[inline(always)]
    pub fn advance_and_emit_deltas(
        &mut self,
        duration: time_internal::Duration,
        waw: &mut WriteAheadWheel<A>,
    ) -> Vec<Option<A::PartialAggregate>> {
        let ticks: usize = duration.whole_seconds() as usize;

        let mut deltas = Vec::with_capacity(ticks);
        // Naivé way, no fast ticking..
        for _ in 0..ticks {
            let delta = waw.tick().map(|m| A::freeze(m));
            // tick wheel first in case any timer has to fire
            self.tick(delta);
            // insert delta to our vec
            deltas.push(delta);
        }
        deltas
    }

    /// Advances the time of the wheel aligned by the lowest unit (Second)
    #[inline]
    pub(crate) fn advance_to(&mut self, watermark: u64, waw: &mut WriteAheadWheel<A>) {
        let diff = watermark.saturating_sub(self.watermark());
        self.advance(time_internal::Duration::milliseconds(diff as i64), waw);
    }

    /// Advances the wheel by applying a set of deltas where each delta represents the lowest unit of time
    ///
    /// Note that deltas are processed in the order of the iterator. If you have the following deltas
    /// [Some(10),  Some(20)], it will first insert Some(10) into the wheel and then Some(20).
    #[inline]
    pub fn delta_advance(&mut self, deltas: impl IntoIterator<Item = Option<A::PartialAggregate>>) {
        for delta in deltas {
            self.tick(delta);
        }
    }

    /// Clears the state of all wheels
    pub fn clear(&mut self) {
        self.seconds_wheel.clear();
        self.minutes_wheel.clear();
        self.hours_wheel.clear();
        self.days_wheel.clear();
        self.weeks_wheel.clear();
        self.years_wheel.clear();
    }

    /// Return the current watermark as milliseconds for this wheel
    #[inline]
    pub fn watermark(&self) -> u64 {
        self.watermark
    }
    /// Returns the aggregate in the given time interval
    pub fn interval_and_lower(&self, dur: time_internal::Duration) -> Option<A::Aggregate> {
        self.interval(dur).map(|partial| A::lower(partial))
    }
    /// Combines partial aggregates within the given date range and lowers it to a final aggregate
    #[inline]
    pub fn combine_range_and_lower(
        &self,
        _start: OffsetDateTime,
        _end: OffsetDateTime,
    ) -> Option<A::Aggregate> {
        // self.combine_range(start, end).map(|(agg, _)| A::lower(agg))
        unimplemented!();
    }

    // Generates query nodes given a start and end date
    // TODO: refactor
    #[inline]
    fn generate_query_nodes(
        start: OffsetDateTime,
        end: OffsetDateTime,
    ) -> Vec<(OffsetDateTime, OffsetDateTime)> {
        let mut nodes = Vec::new();
        let mut current_start = start;
        let new_curr_end = |current_start: OffsetDateTime, end: &OffsetDateTime| {
            let second = current_start.second();
            let minute = current_start.minute();
            let hour = current_start.hour();
            let day = current_start.day();
            let next_days = time::Duration::days(31 - day as i64);
            let next_hours = time::Duration::hours(24 - hour as i64);
            let next_minutes = time::Duration::minutes(60 - minute as i64);
            let next_seconds = time::Duration::seconds(60 - second as i64);

            let dur = *end - current_start;

            if second > 0 {
                // we in second gran, need to jump to next min.
                if current_start + next_seconds > *end {
                    current_start + time::Duration::seconds(dur.whole_seconds())
                } else {
                    current_start + next_seconds
                }
            } else if minute > 0 {
                if current_start + next_minutes > *end {
                    let m = current_start + time::Duration::minutes(dur.whole_minutes());
                    // dbg!(m);
                    if m == current_start {
                        current_start + time::Duration::seconds(dur.whole_seconds() as i64)
                    } else {
                        m
                    }
                } else {
                    current_start + next_minutes
                }
            } else if hour > 0 {
                // we in hour gran, need to jump to next day.
                if current_start + next_hours > *end {
                    // get hours to jump to next point
                    let whole_hours = dur.whole_hours();
                    let whole_minutes = dur.whole_minutes();
                    let h = if whole_hours > 0 {
                        current_start + time::Duration::hours(dur.whole_hours())
                    } else if whole_minutes > 0 {
                        current_start + time::Duration::minutes(dur.whole_minutes())
                    } else {
                        current_start + time::Duration::seconds(dur.whole_seconds())
                    };
                    // let h = current_start + time::Duration::minutes(dur.whole_minutes());

                    if h == current_start {
                        let sh =
                            current_start + time::Duration::minutes(dur.whole_minutes() as i64);
                        // dbg!(sh);
                        if sh == current_start {
                            *end
                        } else {
                            sh
                        }
                    } else {
                        // dbg!(h);
                        h
                    }
                } else {
                    current_start + next_hours
                }
            } else if day > 0 {
                if current_start + next_days > *end {
                    let whole_days = dur.whole_days();
                    let whole_hours = dur.whole_hours();
                    let whole_minutes = dur.whole_minutes();
                    let d = if whole_days > 0 {
                        current_start + time::Duration::days(dur.whole_days())
                    } else if whole_hours > 0 {
                        current_start + time::Duration::hours(dur.whole_hours())
                    } else if whole_minutes > 0 {
                        current_start + time::Duration::minutes(dur.whole_minutes())
                    } else {
                        current_start + time::Duration::seconds(dur.whole_seconds())
                    };

                    // let d = current_start + time::Duration::days(dur.whole_days());
                    if d == current_start {
                        let sd = current_start + time::Duration::hours(dur.whole_hours() as i64);
                        // dbg!(sh);
                        if sd == current_start {
                            *end
                        } else {
                            sd
                        }
                    } else {
                        // dbg!(h);
                        d
                    }
                } else {
                    current_start + next_days
                }
            } else {
                unimplemented!("Dimensions weeks, years not supported yet");
            }
        };

        // while we have not reached the end, keep building query nodes.
        while current_start < end {
            let current_end = new_curr_end(current_start, &end);
            nodes.push((current_start, current_end));
            current_start = current_end;
        }

        // function that returns a score of the query node which is used during sorting
        let granularity_score = |start: &OffsetDateTime, end: &OffsetDateTime| {
            let (sh, sm, ss) = start.time().as_hms();
            let (eh, em, es) = end.time().as_hms();
            if ss > 0 || es > 0 {
                // second rank
                0
            } else if sm > 0 || em > 0 {
                // miute rank
                1
            } else if sh > 0 || eh > 0 {
                // hour rank
                2
            } else {
                // day rank (00:00:00 - 00:00:00)
                3
            }
        };

        // Sort query nodes based on lowest granularity in order to execute nodes in order
        // so that the execution visits wheels sequentially and not randomly.
        nodes.sort_unstable_by(|a, b| {
            let a_score = granularity_score(&a.0, &a.1);
            let b_score = granularity_score(&b.0, &b.1);
            a_score.cmp(&b_score)
        });

        nodes
    }
    /// Combines partial aggregates within the given date range
    #[inline]
    pub fn combine_range_smart(
        &self,
        start: OffsetDateTime,
        end: OffsetDateTime,
    ) -> Option<A::PartialAggregate> {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.combine_range);

        assert!(
            end >= start,
            "End date needs to be equal or larger than start date"
        );
        // Generate query nodes and execute
        let nodes = Self::generate_query_nodes(start, end);
        // dbg!(start, end);
        // dbg!(&nodes);
        nodes.into_iter().fold(None, |mut acc, node| {
            match self.combine_range(node.0, node.1) {
                Some(agg) => {
                    combine_or_insert::<A>(&mut acc, agg);
                    acc
                }
                None => acc,
            }
        })
    }

    /// Combines partial aggregates within the given date range
    #[inline]
    pub fn combine_range(
        &self,
        start: OffsetDateTime,
        end: OffsetDateTime,
    ) -> Option<A::PartialAggregate> {
        // NOTE: very naivé version, still needs checks and can do more optimizations
        assert!(
            end >= start,
            "End date needs to be equal or larger than start date"
        );
        let is_seconds = start.second() != 0 || end.second() != 0;
        let is_minutes = start.minute() != 0 || end.minute() != 0;
        let is_hours = start.hour() != 0 || end.hour() != 0;
        let is_days = start.day() != 0 || end.day() != 0;

        let watermark_date =
            |wm: u64| OffsetDateTime::from_unix_timestamp((wm as i64) / 1000).unwrap();

        // TODO: Refactor and move inner logic to inner wheel?
        if is_seconds {
            // We have the number of seconds in the range
            let seconds = (end - start).whole_seconds();
            // dbg!(seconds);
            if let Some(wheel) = self.seconds_wheel.as_ref() {
                let watermark = watermark_date(wheel.watermark());
                let watermark_start_diff: usize = (watermark - start).whole_seconds() as usize;
                let start_pos = watermark_start_diff - seconds as usize;
                let end_pos = start_pos + seconds as usize;
                wheel.combine_range(start_pos..end_pos)
            } else {
                panic!("sec wheel not initialized");
            }
        } else if is_minutes {
            // We have the number of minutes in the range
            let minutes = (end - start).whole_minutes();
            // dbg!(minutes);
            if let Some(wheel) = self.minutes_wheel.as_ref() {
                let watermark = watermark_date(wheel.watermark());
                // start must be lower than watermark
                // end must be lower or equal to watermark
                let watermark_start_diff: usize = (watermark - start).whole_minutes() as usize;
                let start_pos = watermark_start_diff - minutes as usize;
                let end_pos = start_pos + minutes as usize;
                wheel.combine_range(start_pos..end_pos)
            } else {
                panic!("min wheel not initialized");
            }
        } else if is_hours {
            let hours = (end - start).whole_hours();
            // dbg!(hours);
            if let Some(wheel) = self.hours_wheel.as_ref() {
                let watermark = watermark_date(wheel.watermark());
                let watermark_start_diff: usize = (watermark - start).whole_hours() as usize;
                let start_pos = watermark_start_diff - hours as usize;
                let end_pos = start_pos + hours as usize;
                wheel.combine_range(start_pos..end_pos)
            } else {
                panic!("hours wheel not initialized");
            }
        } else if is_days {
            let days = (end - start).whole_days();
            // dbg!(days);
            if let Some(wheel) = self.days_wheel.as_ref() {
                let watermark = watermark_date(wheel.watermark());
                let watermark_start_diff: usize = (watermark - start).whole_days() as usize;
                let start_pos = watermark_start_diff - days as usize;
                let end_pos = start_pos + days as usize;
                wheel.combine_range(start_pos..end_pos)
            } else {
                panic!("days wheel not initialized");
            }
        } else {
            unimplemented!();
        }
    }

    // helper function to convert a time interval to the responding time granularities
    #[inline]
    fn duration_to_granularities(dur: time_internal::Duration) -> Granularities {
        // closure that turns i64 to None if it is zero
        let to_option = |num: i64| {
            if num == 0 {
                None
            } else {
                Some(num as usize)
            }
        };
        Granularities {
            second: to_option(dur.whole_seconds() % SECONDS as i64),
            minute: to_option(dur.whole_minutes() % MINUTES as i64),
            hour: to_option(dur.whole_hours() % HOURS as i64),
            day: to_option(dur.whole_days() % DAYS as i64),
            week: to_option(dur.whole_weeks() % WEEKS as i64),
            year: to_option((dur.whole_weeks() / WEEKS as i64) % YEARS as i64),
        }
    }

    /// Schedules a timer to fire once the given time has been reached
    #[cfg(feature = "timer")]
    pub(crate) fn schedule_once(
        &self,
        time: u64,
        f: impl Fn(&Haw<A, K>) + 'static,
    ) -> Result<(), TimerError<TimerAction<A, K>>> {
        self.timer
            .borrow_mut()
            .schedule_at(time, TimerAction::Oneshot(Box::new(f)))
    }
    /// Schedules a timer to fire repeatedly
    #[cfg(feature = "timer")]
    pub(crate) fn schedule_repeat(
        &self,
        at: u64,
        interval: time_internal::Duration,
        f: impl Fn(&Haw<A, K>) + 'static,
    ) -> Result<(), TimerError<TimerAction<A, K>>> {
        self.timer
            .borrow_mut()
            .schedule_at(at, TimerAction::Repeat((at, interval, Box::new(f))))
    }

    /// Returns the a set of partial aggregates based on the downsampling function
    pub fn downsample<R>(
        &self,
        _range: R,
        _interval: Duration,
        _slide: Duration,
    ) -> Option<Vec<A::PartialAggregate>>
    where
        R: RangeBounds<u64>,
    {
        // # Issue: https://github.com/Max-Meldrum/awheel/issues/87
        unimplemented!();
    }

    /// Returns the partial aggregate in the given time range
    pub fn time_range<R>(&self, _range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<u64>,
    {
        // # Issue: https://github.com/Max-Meldrum/awheel/issues/88

        // assert_eq!(start > end, "start must be larger than end");
        // 1. Have to check whether this HAW is configured to support the specified range
        // 2. Next locate the wheel which can be used to answer this query (e.g., is the query in sec,min, hour granularity)
        unimplemented!();
    }

    /// Returns the partial aggregate in the given time interval
    ///
    /// This function combines lazily rolled up aggregates meaning that the result may not be up to date
    /// depending on the current wheel cycle.
    ///
    /// # Note
    ///
    /// The given time duration must be quantizable to the time intervals of the HAW
    #[inline]
    pub fn interval(&self, dur: time_internal::Duration) -> Option<A::PartialAggregate> {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.interval);

        if Self::is_quantizable(dur) {
            let granularities = Self::duration_to_granularities(dur);
            match K::mode() {
                Mode::Lazy => self.lazy_combine(granularities),
                Mode::Eager => {
                    let cycle_time = self.current_time_in_cycle();
                    // if the given duration is higher than the current cycle time,
                    // return the landmark window for the wheel instead.
                    if dur > cycle_time {
                        self.landmark()
                    } else {
                        self.eager_combine(granularities)
                    }
                }
            }
        } else {
            None
        }
    }

    #[inline]
    fn eager_combine(&self, granularties: Granularities) -> Option<A::PartialAggregate> {
        let Granularities {
            second,
            minute,
            hour,
            day,
            week,
            year,
        } = granularties;
        match (year, week, day, hour, minute, second) {
            // y
            (Some(1), None, None, None, None, None) => self.years_wheel.head(),
            (Some(year), None, None, None, None, None) => self.years_wheel.interval_or_total(year),
            // w
            (None, Some(1), None, None, None, None) => self.weeks_wheel.head(),
            (None, Some(week), None, None, None, None) => self.weeks_wheel.interval_or_total(week), // d
            // d
            (None, None, Some(1), None, None, None) => self.days_wheel.head(),
            (None, None, Some(day), None, None, None) => self.days_wheel.interval_or_total(day),
            // h
            (None, None, None, Some(1), None, None) => self.hours_wheel.head(),
            (None, None, None, Some(hour), None, None) => self.hours_wheel.interval_or_total(hour),
            // m
            (None, None, None, None, Some(1), None) => self.minutes_wheel.head(),
            (None, None, None, None, Some(min), None) => self.minutes_wheel.interval_or_total(min),
            // s
            (None, None, None, None, None, Some(sec)) => self.seconds_wheel.interval_or_total(sec),
            _t @ (_, _, _, _, _, _) => {
                // Invalid interval given
                // NOTE: should we return an error indicating this or simply return None
                None
            }
        }
    }

    #[inline]
    fn lazy_combine(&self, granularties: Granularities) -> Option<A::PartialAggregate> {
        let Granularities {
            second,
            minute,
            hour,
            day,
            week,
            year,
        } = granularties;

        // dbg!((second, minute, hour, day, week, year));

        match (year, week, day, hour, minute, second) {
            // ywdhms
            (Some(year), Some(week), Some(day), Some(hour), Some(minute), Some(second)) => {
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);
                let day = cmp::min(self.days_wheel.rotation_count(), day);
                let week = cmp::min(self.weeks_wheel.rotation_count(), week);

                let sec = self.seconds_wheel.interval_or_total(second);
                let min = self.minutes_wheel.interval_or_total(minute);
                let hr = self.hours_wheel.interval_or_total(hour);
                let day = self.days_wheel.interval_or_total(day);
                let week = self.weeks_wheel.interval_or_total(week);
                let year = self.years_wheel.interval(year);

                Self::reduce([sec, min, hr, day, week, year])
            }
            // wdhms
            (None, Some(week), Some(day), Some(hour), Some(minute), Some(second)) => {
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);
                let day = cmp::min(self.days_wheel.rotation_count(), day);

                let sec = self.seconds_wheel.interval_or_total(second);
                let min = self.minutes_wheel.interval_or_total(minute);
                let hr = self.hours_wheel.interval_or_total(hour);
                let day = self.days_wheel.interval_or_total(day);
                let week = self.days_wheel.interval(week);

                Self::reduce([sec, min, hr, day, week])
            }
            // dhms
            (None, None, Some(day), Some(hour), Some(minute), Some(second)) => {
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);

                let sec = self.seconds_wheel.interval_or_total(second);
                let min = self.minutes_wheel.interval_or_total(minute);
                let hr = self.hours_wheel.interval_or_total(hour);
                let day = self.days_wheel.interval(day);

                Self::reduce([sec, min, hr, day])
            }
            // dhm
            (None, None, Some(day), Some(hour), Some(minute), None) => {
                // Do not query below rotation count
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);

                let min = self.minutes_wheel.interval_or_total(minute);
                let hr = self.hours_wheel.interval_or_total(hour);
                let day = self.days_wheel.interval(day);
                Self::reduce([min, hr, day])
            }
            // dh
            (None, None, Some(day), Some(hour), None, None) => {
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);
                let hr = self.hours_wheel.interval_or_total(hour);
                let day = self.days_wheel.interval(day);
                Self::reduce([hr, day])
            }
            // d
            (None, None, Some(day), None, None, None) => self.days_wheel.interval(day),
            // hms
            (None, None, None, Some(hour), Some(minute), Some(second)) => {
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);

                let sec = self.seconds_wheel.interval_or_total(second);
                let min = self.minutes_wheel.interval_or_total(minute);
                let hr = self.hours_wheel.interval(hour);
                Self::reduce([sec, min, hr])
            }
            // hm
            (None, None, None, Some(hour), Some(minute), None) => {
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let min = self.minutes_wheel.interval_or_total(minute);

                let hr = self.hours_wheel.interval(hour);
                Self::reduce([min, hr])
            }
            // yw
            (Some(year), Some(week), None, None, None, None) => {
                let week = cmp::min(self.weeks_wheel.rotation_count(), week);
                let week = self.weeks_wheel.interval_or_total(week);
                let year = self.years_wheel.interval(year);
                Self::reduce([year, week])
            }
            // wd
            (None, Some(week), Some(day), None, None, None) => {
                let day = cmp::min(self.days_wheel.rotation_count(), day);
                let day = self.days_wheel.interval_or_total(day);
                let week = self.weeks_wheel.interval(week);
                Self::reduce([week, day])
            }
            // y
            (Some(year), None, None, None, None, None) => self.years_wheel.interval_or_total(year),
            // w
            (None, Some(week), None, None, None, None) => self.weeks_wheel.interval_or_total(week),
            // h
            (None, None, None, Some(hour), None, None) => self.hours_wheel.interval_or_total(hour),
            // hs
            (None, None, None, Some(hour), None, Some(second)) => {
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let sec = self.seconds_wheel.interval_or_total(second);
                let hour = self.hours_wheel.interval_or_total(hour);
                Self::reduce([hour, sec])
            }
            // ms
            (None, None, None, None, Some(minute), Some(second)) => {
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let sec = self.seconds_wheel.interval_or_total(second);
                let min = self.minutes_wheel.interval(minute);
                Self::reduce([min, sec])
            }
            // m
            (None, None, None, None, Some(minute), None) => {
                self.minutes_wheel.interval_or_total(minute)
            }
            // s
            (None, None, None, None, None, Some(second)) => {
                self.seconds_wheel.interval_or_total(second)
            }
            t @ (_, _, _, _, _, _) => {
                panic!("combine_time was given invalid Time arguments {:?}", t);
            }
        }
    }

    // Checks whether the duration is quantizable to time intervals of HAW
    #[inline]
    const fn is_quantizable(duration: time_internal::Duration) -> bool {
        let seconds = duration.whole_seconds();

        if seconds > 0 && seconds <= 59
            || seconds % time_internal::Duration::minutes(1).whole_seconds() == 0
            || seconds % time_internal::Duration::hours(1).whole_seconds() == 0
            || seconds % time_internal::Duration::days(1).whole_seconds() == 0
            || seconds % time_internal::Duration::weeks(1).whole_seconds() == 0
            || seconds % time_internal::Duration::years(1).whole_seconds() == 0
            || seconds <= time_internal::Duration::years(YEARS as i64).whole_seconds()
        {
            return true;
        }

        false
    }

    /// Executes a Landmark Window that combines total partial aggregates across all wheels
    #[inline]
    pub fn landmark(&self) -> Option<A::PartialAggregate> {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.landmark);

        let wheels = [
            self.seconds_wheel.total(),
            self.minutes_wheel.total(),
            self.hours_wheel.total(),
            self.days_wheel.total(),
            self.weeks_wheel.total(),
            self.years_wheel.total(),
        ];
        Self::reduce(wheels)
    }
    /// Executes a Landmark Window that combines total partial aggregates across all wheels and lowers the result
    #[inline]
    pub fn landmark_and_lower(&self) -> Option<A::Aggregate> {
        self.landmark().map(|partial| A::lower(partial))
    }

    #[inline]
    fn reduce(
        partial_aggs: impl IntoIterator<Item = Option<A::PartialAggregate>>,
    ) -> Option<A::PartialAggregate> {
        partial_aggs
            .into_iter()
            .reduce(|acc, b| match (acc, b) {
                (Some(curr), Some(agg)) => Some(A::combine(curr, agg)),
                (None, Some(_)) => b,
                _ => acc,
            })
            .flatten()
    }

    /// Tick the wheel by a single unit (second)
    ///
    /// In the worst case, a tick may cause a rotation of all the wheels in the hierarchy.
    #[inline]
    fn tick(&mut self, partial_opt: Option<A::PartialAggregate>) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.tick);

        self.watermark += Self::SECOND_AS_MS;

        // let mut seconds = self.seconds_wheel.get_or_insert();
        // seconds.insert_identity();

        // if the partial has some value then update the wheel(s)
        // if let Some(partial) = partial_opt {
        let partial = partial_opt.unwrap_or(A::IDENTITY);
        self.seconds_wheel
            .get_or_insert()
            .insert_slot(WheelSlot::new(partial_opt, None));

        // pre-aggregate all wheel heads if Eager aggregation is used
        if let Mode::Eager = K::mode() {
            if let Some(ref mut minutes) = self.minutes_wheel.as_mut() {
                minutes.insert_head(partial);
                if let Some(ref mut hours) = self.hours_wheel.as_mut() {
                    hours.insert_head(partial);
                    if let Some(ref mut days) = self.days_wheel.as_mut() {
                        days.insert_head(partial);
                        if let Some(ref mut weeks) = self.weeks_wheel.as_mut() {
                            weeks.insert_head(partial);
                            if let Some(ref mut years) = self.years_wheel.as_mut() {
                                years.insert_head(partial);
                            }
                        }
                    }
                }
            }
        }
        // }

        match K::mode() {
            Mode::Lazy => self.lazy_tick(),
            Mode::Eager => self.eager_tick(),
        }

        // Fire any outgoing timers
        #[cfg(feature = "timer")]
        {
            let mut timer = self.timer.borrow_mut();

            for action in timer.advance_to(self.watermark) {
                match action {
                    TimerAction::Oneshot(udf) => {
                        udf(self);
                    }
                    TimerAction::Repeat((at, interval, udf)) => {
                        udf(self);
                        let new_at = at + interval.whole_milliseconds() as u64;
                        let _ =
                            timer.schedule_at(new_at, TimerAction::Repeat((new_at, interval, udf)));
                    }
                }
            }
        }
    }

    #[inline]
    fn lazy_tick(&mut self) {
        let seconds = self.seconds_wheel.get_or_insert();
        // full rotation of seconds wheel
        if let Some(rot_data) = seconds.tick() {
            // insert 60 seconds worth of partial aggregates into minute wheel and then tick it
            let minutes = self.minutes_wheel.get_or_insert();

            minutes.insert_slot(rot_data);

            // full rotation of minutes wheel
            if let Some(rot_data) = minutes.tick() {
                // insert 60 minutes worth of partial aggregates into hours wheel and then tick it
                let hours = self.hours_wheel.get_or_insert();

                hours.insert_slot(rot_data);

                // full rotation of hours wheel
                if let Some(rot_data) = hours.tick() {
                    // insert 24 hours worth of partial aggregates into days wheel and then tick it
                    let days = self.days_wheel.get_or_insert();
                    days.insert_slot(rot_data);

                    // full rotation of days wheel
                    if let Some(rot_data) = days.tick() {
                        // insert 7 days worth of partial aggregates into weeks wheel and then tick it
                        let weeks = self.weeks_wheel.get_or_insert();

                        weeks.insert_slot(rot_data);

                        // full rotation of weeks wheel
                        if let Some(rot_data) = weeks.tick() {
                            // insert 1 years worth of partial aggregates into year wheel and then tick it
                            let years = self.years_wheel.get_or_insert();
                            years.insert_slot(rot_data);

                            // tick but ignore full rotations as this is the last hierarchy
                            let _ = years.tick();
                        }
                    }
                }
            }
        }
    }
    #[inline]
    fn eager_tick(&mut self) {
        let seconds = self.seconds_wheel.get_or_insert();

        if let Some(seconds_total) = seconds.tick() {
            let minutes = self.minutes_wheel.get_or_insert();

            let minutes_opt = minutes.tick();
            minutes.insert_slot(seconds_total);

            if let Some(minutes_total) = minutes_opt {
                let hours = self.hours_wheel.get_or_insert();

                let hours_opt = hours.tick();
                hours.insert_slot(minutes_total);
                if let Some(hours_total) = hours_opt {
                    let days = self.days_wheel.get_or_insert();

                    let days_opt = days.tick();
                    days.insert_slot(hours_total);

                    if let Some(days_total) = days_opt {
                        let weeks = self.weeks_wheel.get_or_insert();

                        let weeks_opt = weeks.tick();
                        weeks.insert_slot(days_total);

                        if let Some(weeks_total) = weeks_opt {
                            let years = self.years_wheel.get_or_insert();

                            let _ = years.tick();
                            years.insert_slot(weeks_total);
                            // years wheel is last in the hierarchy, nothing more to do..
                        }
                    }
                }
            }
        }
    }

    /// Returns a reference to the seconds wheel
    pub fn seconds(&self) -> Option<&AggregationWheel<A>> {
        self.seconds_wheel.as_ref()
    }
    /// Returns an unchecked reference to the seconds wheel
    pub fn seconds_unchecked(&self) -> &AggregationWheel<A> {
        self.seconds_wheel.as_ref().unwrap()
    }

    /// Returns a reference to the minutes wheel
    pub fn minutes(&self) -> Option<&AggregationWheel<A>> {
        self.minutes_wheel.as_ref()
    }
    /// Returns an unchecked reference to the minutes wheel
    pub fn minutes_unchecked(&self) -> &AggregationWheel<A> {
        self.minutes_wheel.as_ref().unwrap()
    }
    /// Returns a reference to the hours wheel
    pub fn hours(&self) -> Option<&AggregationWheel<A>> {
        self.hours_wheel.as_ref()
    }
    /// Returns an unchecked reference to the hours wheel
    pub fn hours_unchecked(&self) -> &AggregationWheel<A> {
        self.hours_wheel.as_ref().unwrap()
    }
    /// Returns a reference to the days wheel
    pub fn days(&self) -> Option<&AggregationWheel<A>> {
        self.days_wheel.as_ref()
    }
    /// Returns an unchecked reference to the days wheel
    pub fn days_unchecked(&self) -> &AggregationWheel<A> {
        self.days_wheel.as_ref().unwrap()
    }

    /// Returns a reference to the weeks wheel
    pub fn weeks(&self) -> Option<&AggregationWheel<A>> {
        self.weeks_wheel.as_ref()
    }

    /// Returns an unchecked reference to the weeks wheel
    pub fn weeks_unchecked(&self) -> &AggregationWheel<A> {
        self.weeks_wheel.as_ref().unwrap()
    }

    /// Returns a reference to the years wheel
    pub fn years(&self) -> Option<&AggregationWheel<A>> {
        self.years_wheel.as_ref()
    }
    /// Returns a reference to the years wheel
    pub fn years_unchecked(&self) -> &AggregationWheel<A> {
        self.years_wheel.as_ref().unwrap()
    }

    /// Merges two wheels
    ///
    /// Note that the time in `other` may be advanced and thus change state
    pub(crate) fn merge(&mut self, other: &mut Self) {
        let other_watermark = other.watermark();

        // make sure both wheels are aligned by time
        if self.watermark() > other_watermark {
            other.advance_to(self.watermark(), &mut WriteAheadWheel::default());
        } else {
            self.advance_to(other_watermark, &mut WriteAheadWheel::default());
        }

        // merge all aggregation wheels
        self.seconds_wheel.merge(&other.seconds_wheel);
        self.minutes_wheel.merge(&other.minutes_wheel);
        self.hours_wheel.merge(&other.hours_wheel);
        self.days_wheel.merge(&other.days_wheel);
        self.weeks_wheel.merge(&other.weeks_wheel);
        self.years_wheel.merge(&other.years_wheel);
    }
    #[cfg(feature = "profiler")]
    /// Returns a reference to the stats of the [HAW]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
}

/// A type containing error variants that may arise when querying a wheel
#[derive(Copy, Clone, Debug)]
pub enum QueryError {
    /// Invalid Date Range
    Invalid,
}
impl Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::Invalid => write!(f, "Invalid Date Range"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{aggregator::sum::U64SumAggregator, time_internal::NumericalDuration};
    use time::macros::datetime;

    use super::*;

    #[test]
    fn delta_advance_test() {
        let mut haw: Haw<U64SumAggregator> = Haw::new(0, Default::default());
        // oldest to newest
        let deltas = vec![Some(10), None, Some(50), None];

        // Visualization:
        // 10
        // 0, 10
        // 50, 0, 10
        // 0, 50, 0, 10
        haw.delta_advance(deltas);

        assert_eq!(haw.interval(1.seconds()), Some(0));
        assert_eq!(haw.interval(2.seconds()), Some(50));
        assert_eq!(haw.interval(3.seconds()), Some(50));
        assert_eq!(haw.interval(4.seconds()), Some(60));
    }

    #[test]
    fn range_query_sec_test() {
        // 2023-11-09 00:00:00
        let watermark = 1699488000000;
        let conf = HawConf::default().with_watermark(watermark);
        let mut haw: Haw<U64SumAggregator> = Haw::new(watermark, conf);
        // oldest to newest
        let deltas = vec![Some(10), None, Some(50), None];

        haw.delta_advance(deltas);

        assert_eq!(haw.interval(1.seconds()), Some(0));
        assert_eq!(haw.interval(2.seconds()), Some(50));
        assert_eq!(haw.interval(3.seconds()), Some(50));
        assert_eq!(haw.interval(4.seconds()), Some(60));

        let start = datetime!(2023-11-09 00:00:00 UTC);
        let end = datetime!(2023-11-09 00:00:02 UTC);

        let agg = haw.combine_range(start, end);
        assert_eq!(agg, Some(10));

        let start = datetime!(2023-11-09 00:00:00 UTC);
        let end = datetime!(2023-11-09 00:00:04 UTC);
        let agg = haw.combine_range(start, end);
        assert_eq!(agg, Some(60));
        let agg = haw.combine_range_smart(start, end);
        assert_eq!(agg, Some(60));

        // let start = datetime!(2023-11-24 10:15:23 UTC);
        // let end = datetime!(2023-11-24 13:20:50 UTC);

        // let a = haw.combine_range_smart(start, end);
        // let a = haw.generate_query_nodes(start, end);
    }

    #[test]
    fn range_query_min_test() {
        // 2023-11-09 00:00:00
        let watermark = 1699488000000;
        let conf = HawConf::default().with_watermark(watermark);
        let mut haw: Haw<U64SumAggregator> = Haw::new(watermark, conf);

        let deltas: Vec<Option<u64>> = (0..180).map(|_| Some(1)).collect();
        haw.delta_advance(deltas);
        assert_eq!(haw.watermark(), 1699488180000);

        // time is 2023-11-09 00:03:00
        // queryable minutes are 00:00 - 00:03
        let start = datetime!(2023-11-09 00:00 UTC);
        let end = datetime!(2023-11-09 00:03 UTC);

        let agg = haw.combine_range(start, end);
        // 1 for each second rolled-up over 3 minutes > 180
        assert_eq!(agg, Some(180));
    }

    #[test]
    fn range_query_hour_test() {
        // 2023-11-09 00:00:00
        let watermark = 1699488000000;
        let conf = HawConf::default().with_watermark(watermark);
        let mut haw: Haw<U64SumAggregator> = Haw::new(watermark, conf);

        let hours_as_secs = 3600;
        let hours = hours_as_secs * 3;
        let deltas: Vec<Option<u64>> = (0..hours).map(|_| Some(1)).collect();
        // advance wheel by 3 hours
        haw.delta_advance(deltas);

        let start = datetime!(2023-11-09 00:00 UTC);
        let end = datetime!(2023-11-09 01:00 UTC);

        assert_eq!(haw.combine_range(start, end), Some(3600));
        assert_eq!(haw.combine_range_smart(start, end), Some(3600));

        let start = datetime!(2023-11-09 00:00 UTC);
        let end = datetime!(2023-11-09 03:00 UTC);

        assert_eq!(haw.combine_range(start, end), Some(10800));
        assert_eq!(haw.combine_range_smart(start, end), Some(10800));
    }

    #[test]
    fn range_query_day_test() {
        // 2023-11-09 00:00:00
        let watermark = 1699488000000;
        let conf = HawConf::default().with_watermark(watermark);
        let mut haw: Haw<U64SumAggregator> = Haw::new(watermark, conf);

        let days_as_secs = 3600 * 24;
        let days = days_as_secs * 3;
        let deltas: Vec<Option<u64>> = (0..days).map(|_| Some(1)).collect();
        // advance wheel by 3 days
        haw.delta_advance(deltas);

        let start = datetime!(2023 - 11 - 09 00:00:00 UTC);
        let end = datetime!(2023 - 11 - 10 00:00:00 UTC);

        assert_eq!(haw.combine_range(start, end), Some(3600 * 24));
        assert_eq!(haw.combine_range_smart(start, end), Some(3600 * 24));

        let start = datetime!(2023 - 11 - 09 00:00:00 UTC);
        let end = datetime!(2023 - 11 - 12 00:00:00 UTC);

        assert_eq!(haw.combine_range(start, end), Some(259200));
        assert_eq!(haw.combine_range_smart(start, end), Some(259200));
    }
}
