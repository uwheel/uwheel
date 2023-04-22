//! # Hierarchical Aggregation Wheels
//!
//! ## What it is
//!
//! Hierarchical Aggregation Wheel is a compact index that pre-computes and maintains aggregates across stream event time.
//!
//! Key features:
//!
//! * Fast insertions
//! * Event-time Integration
//! * Compact and highly compressible
//! * Bounded Query Latency
//!
//! ## How it works
//!
//! Similarly to Hierarchical Wheel Timers, we exploit the hierarchical nature of time and utilise several aggregation wheels,
//! each with a different time granularity. This enables a compact representation of aggregates across time
//! with a low memory footprint and makes it highly compressible and efficient to store on disk.
//! For instance, to store aggregates with second granularity up to 10 years, we would need the following aggregation wheels:
//!
//! * Seconds wheel with 60 slots
//! * Minutes wheel with 60 slots
//! * Hours wheel with 24 slots
//! * Days wheel with 7 slots
//! * Weeks wheel with 4 slots
//! * Months wheel with 12 slots
//! * Years wheel with 10 slots
//!
//! The above scheme results in a total of 177 wheel slots. This is the minimum number of slots
//! required to support rolling up aggregates across 10 years with second granularity.
//!
//! Internally, a low watermark is maintained. Insertions with timestamps below the watermark will be ignored.
//! It is up to the user of the wheel to advance the watermark and thus roll up aggregates continously up the time hierarchy.
//! Note that the wheel may insert aggregates above the watermark, but state is only queryable below the watermark point.
//!
//!
//! # Feature Flags
//!
//! - `std` (_enabled by default_)
//!     - Enables features that rely on the standard library
//! - `alloc` (_enabled by default via std_)
//!     - Enables a number of features that require the ability to dynamically allocate memory.
//! - `years_size_10` (_enabled by default_)
//!     - Enables rolling up aggregates across 10 years
//! - `years_size_100`
//!     - Enables rolling up aggregates across 100 years
//! - `drill_down` (_implicitly enables alloc_)
//!     - Enables drill-down operations on wheels at the cost of more storage
//! - `rkyv`
//!     - Enables serialisation & deserialisation using the [rkyv](https://docs.rs/rkyv/latest/rkyv/) framework.

#![feature(slice_range)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(all(feature = "alloc", not(feature = "std")))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

#[doc(hidden)]
#[macro_use]
pub mod macros;

use core::{
    cmp,
    fmt,
    fmt::{Debug, Display},
    iter::IntoIterator,
    matches,
    option::{
        Option,
        Option::{None, Some},
    },
    panic,
    result::{
        Result,
        Result::{Err, Ok},
    },
    time::Duration,
    write,
};

#[cfg(all(feature = "rkyv", feature = "alloc"))]
use rkyv::{
    de::deserializers::{SharedDeserializeMap, SharedDeserializeMapError},
    ser::serializers::{
        AlignedSerializer,
        AllocScratch,
        CompositeSerializer,
        FallbackScratch,
        HeapScratch,
        SharedSerializeMap,
    },
    AlignedVec,
};
#[cfg(feature = "rkyv")]
use rkyv::{ser::Serializer, with::Skip, Archive, Deserialize, Infallible, Serialize};

#[cfg(all(feature = "rkyv", not(feature = "alloc")))]
use rkyv::{
    ser::serializers::{BufferSerializer, BufferSerializerError},
    AlignedBytes,
};

/// Aggregation Wheel based on a fixed-sized circular buffer
///
/// This is the core data structure that is reused between different hierarchies (e.g., seconds, minutes, hours, days)
pub mod agg_wheel;
/// Aggregation Interface adopted from the work of [Tangwongsan et al.](http://www.vldb.org/pvldb/vol8/p702-tangwongsan.pdf)
pub mod aggregator;
#[cfg(feature = "alloc")]
/// A Map maintaining a [Wheel] per key
pub mod map;
/// Time utilities
///
/// Heavily borrowed from the [time](https://docs.rs/time/latest/time/) crate
pub mod time;
/// Data types used in this crate
pub mod types;

use aggregator::Aggregator;

use agg_wheel::AggregationWheel;

#[cfg(not(any(feature = "years_size_10", feature = "years_size_100")))]
core::compile_error!(r#"one of ["years_size_10", "years_size_100"] features must be enabled"#);

#[cfg(all(feature = "years_size_10", feature = "years_size_100"))]
core::compile_error!(
    "\"years_size_10\" and \"years_size_100\" are mutually-exclusive features. You may need to set \
    `default-features = false` or compile with `--no-default-features`."
);

pub const SECONDS: usize = 60;
pub const MINUTES: usize = 60;
pub const HOURS: usize = 24;
pub const DAYS: usize = 7;
pub const WEEKS: usize = 4;
pub const MONTHS: usize = 12;

#[cfg(feature = "years_size_10")]
pub const YEARS: usize = 10;
#[cfg(feature = "years_size_100")]
pub const YEARS: usize = 100;

// Default Wheel Capacities (power of two)
const SECONDS_CAP: usize = 128; // This is set as 128 instead of 64 to support write ahead slots
const MINUTES_CAP: usize = 64;
const HOURS_CAP: usize = 32;
const DAYS_CAP: usize = 8;
const WEEKS_CAP: usize = 8;
const MONTHS_CAP: usize = 16;
const YEARS_CAP: usize = YEARS.next_power_of_two();

/// Type alias for an AggregationWheel representing seconds
pub type SecondsWheel<A> = AggregationWheel<SECONDS_CAP, A>;
/// Type alias for an AggregationWheel representing minutes
pub type MinutesWheel<A> = AggregationWheel<MINUTES_CAP, A>;
/// Type alias for an AggregationWheel representing hours
pub type HoursWheel<A> = AggregationWheel<HOURS_CAP, A>;
/// Type alias for an AggregationWheel representing days
pub type DaysWheel<A> = AggregationWheel<DAYS_CAP, A>;
/// Type alias for an AggregationWheel representing weeks
pub type WeeksWheel<A> = AggregationWheel<WEEKS_CAP, A>;
/// Type alias for an AggregationWheel representing months
pub type MonthsWheel<A> = AggregationWheel<MONTHS_CAP, A>;
/// Type alias for an AggregationWheel representing years
pub type YearsWheel<A> = AggregationWheel<YEARS_CAP, A>;

cfg_rkyv! {
    // Alias for an aggregators [`PartialAggregate`] type
    type PartialAggregate<A> = <A as Aggregator>::PartialAggregate;
    // Alias for an aggregators [`PartialAggregate`] archived type
    type Archived<A> = <<A as Aggregator>::PartialAggregate as Archive>::Archived;
    // Alias for the default serializer used by the crate
    #[cfg(feature = "alloc")]
    type DefaultSerializer = CompositeSerializer<
        AlignedSerializer<AlignedVec>,
        FallbackScratch<HeapScratch<4096>, AllocScratch>,
        SharedSerializeMap,
    >;
}

/// A type containing error variants that may arise when using a wheel
#[derive(Debug)]
pub enum Error<T: Debug> {
    Late {
        entry: Entry<T>,
        watermark: u64,
    },
    Overflow {
        entry: Entry<T>,
        max_write_ahead_ts: u64,
    },
}
impl<T: Debug> Display for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Late { entry, watermark } => {
                write!(f, "late entry {entry} current watermark is {watermark}")
            }
            Error::Overflow {
                entry,
                max_write_ahead_ts,
            } => {
                write!(f, "entry {entry} does not fit within wheel, expected timestamp below {max_write_ahead_ts}")
            }
        }
    }
}

impl<T: Debug> Error<T> {
    pub fn is_late(&self) -> bool {
        matches!(self, Error::Late { .. })
    }
    pub fn is_overflow(&self) -> bool {
        matches!(self, Error::Overflow { .. })
    }
}

/// Entry that can be inserted into the Wheel
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Copy, Clone)]
pub struct Entry<T: Debug> {
    /// Data to be lifted by the aggregator
    pub data: T,
    /// Event timestamp of this entry
    pub timestamp: u64,
}
impl<T: Debug> Entry<T> {
    pub fn new(data: T, timestamp: u64) -> Self {
        Self { data, timestamp }
    }
}
impl<T: Debug> fmt::Display for Entry<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(data: {:?}, timestamp: {})", self.data, self.timestamp)
    }
}

/// Hierarchical aggregation wheel data structure
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(not(feature = "drill_down"), derive(Copy))]
#[derive(Clone, Debug)]
pub struct Wheel<A>
where
    A: Aggregator,
{
    #[cfg_attr(feature = "rkyv", with(Skip))]
    aggregator: A,
    watermark: u64,
    seconds_wheel: SecondsWheel<A>,
    minutes_wheel: MinutesWheel<A>,
    hours_wheel: HoursWheel<A>,
    days_wheel: DaysWheel<A>,
    #[cfg_attr(feature = "rkyv", omit_bounds)]
    weeks_wheel: WeeksWheel<A>,
    #[cfg_attr(feature = "rkyv", omit_bounds)]
    months_wheel: MonthsWheel<A>,
    years_wheel: YearsWheel<A>,
    // for some reason rkyv fails to compile without this.
    #[cfg(feature = "rkyv")]
    _marker: A::PartialAggregate,
}

impl<A> Wheel<A>
where
    A: Aggregator,
{
    const SECOND_AS_MS: u64 = time::Duration::SECOND.whole_milliseconds() as u64;
    const MINUTES_AS_SECS: u64 = time::Duration::MINUTE.whole_seconds() as u64;
    const HOURS_AS_SECS: u64 = time::Duration::HOUR.whole_seconds() as u64;
    const DAYS_AS_SECS: u64 = time::Duration::DAY.whole_seconds() as u64;
    const WEEK_AS_SECS: u64 = time::Duration::WEEK.whole_seconds() as u64;
    const MONTH_AS_SECS: u64 = Self::WEEK_AS_SECS * WEEKS as u64;
    const YEAR_AS_SECS: u64 = Self::MONTH_AS_SECS * MONTHS as u64;
    const CYCLE_LENGTH_SECS: u64 = Self::CYCLE_LENGTH.whole_seconds() as u64;

    const TOTAL_SECS_IN_WHEEL: u64 = Self::YEAR_AS_SECS * YEARS as u64;
    pub const CYCLE_LENGTH: time::Duration =
        time::Duration::seconds((Self::YEAR_AS_SECS * (YEARS as u64 + 1)) as i64); // need 1 extra to force full cycle rotation
    pub const TOTAL_WHEEL_SLOTS: usize = SECONDS + MINUTES + HOURS + DAYS + WEEKS + MONTHS + YEARS;
    pub const MAX_WRITE_AHEAD_SLOTS: usize = SECONDS_CAP - SECONDS;

    #[cfg(feature = "drill_down")]
    /// Creates a new Wheel starting from the given time with drill-down capabilities
    ///
    /// Time is represented as milliseconds
    pub fn with_drill_down(time: u64) -> Self {
        Self {
            aggregator: A::default(),
            watermark: time,
            seconds_wheel: AggregationWheel::with_drill_down(SECONDS),
            minutes_wheel: AggregationWheel::with_drill_down(MINUTES),
            hours_wheel: AggregationWheel::with_drill_down(HOURS),
            days_wheel: AggregationWheel::with_drill_down(DAYS),
            weeks_wheel: AggregationWheel::with_drill_down(WEEKS),
            months_wheel: AggregationWheel::with_drill_down(MONTHS),
            years_wheel: AggregationWheel::with_drill_down(YEARS),
            #[cfg(feature = "rkyv")]
            _marker: Default::default(),
        }
    }

    /// Creates a new Wheel starting from the given time
    ///
    /// Time is represented as milliseconds
    pub fn new(time: u64) -> Self {
        Self {
            aggregator: A::default(),
            watermark: time,
            seconds_wheel: AggregationWheel::new(SECONDS),
            minutes_wheel: AggregationWheel::new(MINUTES),
            hours_wheel: AggregationWheel::new(HOURS),
            days_wheel: AggregationWheel::new(DAYS),
            weeks_wheel: AggregationWheel::new(WEEKS),
            months_wheel: AggregationWheel::new(MONTHS),
            years_wheel: AggregationWheel::new(YEARS),
            #[cfg(feature = "rkyv")]
            _marker: Default::default(),
        }
    }

    /// Returns how many wheel slots are utilised
    pub fn len(&self) -> usize {
        self.seconds_wheel.len()
            + self.minutes_wheel.len()
            + self.hours_wheel.len()
            + self.days_wheel.len()
            + self.weeks_wheel.len()
            + self.months_wheel.len()
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

    /// Returns how many slots (seconds) in front of the current watermark that can be written to
    pub fn write_ahead_len(&self) -> usize {
        self.seconds_wheel.write_ahead_len()
    }

    /// Returns how many ticks (seconds) are left until the wheel is fully utilised
    pub fn remaining_ticks(&self) -> u64 {
        Self::TOTAL_SECS_IN_WHEEL - self.current_time_in_cycle().whole_seconds() as u64
    }

    /// Returns Duration that represents where the wheel currently is in its cycle
    #[inline]
    pub fn current_time_in_cycle(&self) -> time::Duration {
        let secs = self.seconds_wheel.rotation_count() as u64;
        let min_secs = self.minutes_wheel.rotation_count() as u64 * Self::MINUTES_AS_SECS;
        let hr_secs = self.hours_wheel.rotation_count() as u64 * Self::HOURS_AS_SECS;
        let day_secs = self.days_wheel.rotation_count() as u64 * Self::DAYS_AS_SECS;
        let week_secs = self.weeks_wheel.rotation_count() as u64 * Self::WEEK_AS_SECS;
        let month_secs = self.months_wheel.rotation_count() as u64 * Self::MONTH_AS_SECS;
        let year_secs = self.years_wheel.rotation_count() as u64 * Self::YEAR_AS_SECS;
        let cycle_time = secs + min_secs + hr_secs + day_secs + week_secs + month_secs + year_secs;
        time::Duration::seconds(cycle_time as i64)
    }

    /// Advance the watermark of the wheel by the given [time::Duration]
    #[inline]
    pub fn advance(&mut self, duration: time::Duration) {
        let mut ticks: usize = duration.whole_seconds() as usize;

        // helper fn to tick N times
        let tick_n = |ticks: usize, haw: &mut Self| {
            for _ in 0..ticks {
                haw.tick();
            }
        };

        if ticks <= SECONDS {
            tick_n(ticks, self);
        } else if ticks <= Self::CYCLE_LENGTH_SECS as usize {
            // force full rotation
            let rem_ticks = self.seconds_wheel.ticks_remaining();
            tick_n(rem_ticks, self);
            ticks -= rem_ticks;

            // calculate how many fast_ticks we can perform
            let fast_ticks = ticks.saturating_div(SECONDS);

            if fast_ticks == 0 {
                // if fast ticks is 0, then tick normally
                tick_n(ticks, self);
            } else {
                // perform a number of fast ticks
                // NOTE: currently a fast tick is a full SECONDS rotation.
                let fast_tick_ms = (SECONDS - 1) as u64 * Self::SECOND_AS_MS;
                for _ in 0..fast_ticks {
                    self.seconds_wheel.fast_skip_tick();
                    self.watermark += fast_tick_ms;
                    self.tick();
                    ticks -= SECONDS;
                }
                // tick any remaining ticks
                tick_n(ticks, self);
            }
        } else {
            // Exceeds full cycle length, clear all!
            self.clear();
        }
    }

    /// Advances the time of the wheel aligned by the lowest unit (Second)
    #[inline]
    pub fn advance_to(&mut self, watermark: u64) {
        let diff = watermark.saturating_sub(self.watermark());
        self.advance(time::Duration::milliseconds(diff as i64));
    }

    /// Clears the state of all wheels
    pub fn clear(&mut self) {
        self.seconds_wheel.clear();
        self.minutes_wheel.clear();
        self.hours_wheel.clear();
        self.days_wheel.clear();
        self.weeks_wheel.clear();
        self.months_wheel.clear();
        self.years_wheel.clear();
    }

    /// Inserts entry into the wheel
    #[inline]
    pub fn insert(&mut self, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        let watermark = self.watermark();

        // If timestamp is below the watermark, then reject it.
        if entry.timestamp < watermark {
            Err(Error::Late { entry, watermark })
        } else {
            let diff = entry.timestamp - self.watermark;
            let seconds = Duration::from_millis(diff).as_secs();

            if self.seconds_wheel().can_write_ahead(seconds) {
                // lift the entry to a partial aggregate and insert
                let partial_agg = self.aggregator.lift(entry.data);
                self.seconds_wheel
                    .write_ahead(seconds, partial_agg, &self.aggregator);
                Ok(())
            } else {
                // cannot fit within the wheel, return it to the user to handle it..
                let write_ahead_ms = Duration::from_secs(self.write_ahead_len() as u64).as_millis();
                let max_write_ahead_ts = self.watermark + write_ahead_ms as u64;
                Err(Error::Overflow {
                    entry,
                    max_write_ahead_ts,
                })
            }
        }
    }

    /// Return the current watermark as milliseconds for this wheel
    #[inline]
    pub fn watermark(&self) -> u64 {
        self.watermark
    }

    /// Returns a full aggregate in the given time interval
    pub fn interval(&self, dur: time::Duration) -> Option<A::Aggregate> {
        // closure that turns i64 to None if it is zero
        let to_option = |num: i64| {
            if num == 0 {
                None
            } else {
                Some(num as usize)
            }
        };
        let second = to_option(dur.whole_seconds() % SECONDS as i64); // % SECONDS_CAP?
        let minute = to_option(dur.whole_minutes() % MINUTES as i64);
        let hour = to_option(dur.whole_hours() % HOURS as i64);
        let day = to_option(dur.whole_days() % DAYS as i64);
        // TODO: integrate week, month and year.
        //let week = to_option(dur.whole_weeks() % WEEKS as i64);
        //let month = to_option((dur.whole_weeks() / 4) % MONTHS as i64);
        //let year = to_option((dur.whole_weeks() / 48) % YEARS as i64);
        //dbg!((second, minute, hour, day, week, month, year));
        self.combine_and_lower_time(second, minute, hour, day)
    }

    /// Combines partial aggregates based on the the given time and lowers the result
    #[inline]
    pub fn combine_and_lower_time(
        &self,
        second: Option<usize>,
        minute: Option<usize>,
        hour: Option<usize>,
        day: Option<usize>,
    ) -> Option<A::Aggregate> {
        // Single-Wheel Query => time must be lower than wheel.len() otherwise it wil panic as range out of bound
        // Multi-Wheel Query => Need to make sure we don't duplicate data across granularities.
        let aggregator = &self.aggregator;

        // TODO: currently panics if any ranges are out of bound
        // TODO: fix type conversions
        match (day, hour, minute, second) {
            // dhms
            (Some(day), Some(hour), Some(minute), Some(second)) => {
                // Do not query below rotation count
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);

                let sec = self.seconds_wheel.combine_up_to_head(second, aggregator);
                let min = self.minutes_wheel.combine_up_to_head(minute, aggregator);
                let hr = self.hours_wheel.combine_up_to_head(hour, aggregator);
                let day = self.days_wheel.combine_up_to_head(day, aggregator);

                Self::reduce([sec, min, hr, day], aggregator).map(|total| aggregator.lower(total))
            }
            // dhm
            (Some(day), Some(hour), Some(minute), None) => {
                // Do not query below rotation count
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);

                let min = self.minutes_wheel.combine_up_to_head(minute, aggregator);
                let hr = self.hours_wheel.combine_up_to_head(hour, aggregator);
                let day = self.days_wheel.combine_up_to_head(day, aggregator);
                Self::reduce([min, hr, day], aggregator).map(|total| aggregator.lower(total))
            }
            // dh
            (Some(day), Some(hour), None, None) => {
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);
                let hr = self.hours_wheel.combine_up_to_head(hour, aggregator);
                let day = self.days_wheel.combine_up_to_head(day, aggregator);
                Self::reduce([hr, day], aggregator).map(|total| aggregator.lower(total))
            }
            // d
            (Some(day), None, None, None) => {
                let day = self.days_wheel.combine_up_to_head(day, aggregator);
                Self::reduce([day], aggregator).map(|total| aggregator.lower(total))
            }
            // hms
            (None, Some(hour), Some(minute), Some(second)) => {
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);

                let sec = self.seconds_wheel.combine_up_to_head(second, aggregator);
                let min = self.minutes_wheel.combine_up_to_head(minute, aggregator);
                let hr = self.hours_wheel.combine_up_to_head(hour, aggregator);
                Self::reduce([sec, min, hr], aggregator).map(|total| aggregator.lower(total))
            }
            // hm
            (None, Some(hour), Some(minute), None) => {
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let min = self.minutes_wheel.combine_up_to_head(minute, aggregator);

                let hr = self.hours_wheel.combine_up_to_head(hour, aggregator);
                Self::reduce([min, hr], aggregator).map(|total| aggregator.lower(total))
            }
            // h
            (None, Some(hour), None, None) => {
                let hr = self.hours_wheel.combine_up_to_head(hour, aggregator);
                Self::reduce([hr], aggregator).map(|total| aggregator.lower(total))
            }
            // ms
            (None, None, Some(minute), Some(second)) => {
                //let second = cmp::min(self.seconds_wheel.rotation_count(), second.into()) as usize;
                let sec = self.seconds_wheel.combine_up_to_head(second, aggregator);
                let min = self.minutes_wheel.combine_up_to_head(minute, aggregator);
                Self::reduce([sec, min], aggregator).map(|total| aggregator.lower(total))
            }
            // m
            (None, None, Some(minute), None) => {
                let min = self.minutes_wheel.combine_up_to_head(minute, aggregator);
                Self::reduce([min], aggregator).map(|total| aggregator.lower(total))
            }
            // s
            (None, None, None, Some(second)) => {
                let sec = self.seconds_wheel.combine_up_to_head(second, aggregator);
                Self::reduce([sec], aggregator).map(|total| aggregator.lower(total))
            }
            (_, _, _, _) => {
                panic!("combine_and_lower_time was given invalid Time arguments");
            }
        }
    }

    /// Executes a Landmark Window that combines total partial aggregates across all wheels
    #[inline]
    pub fn landmark(&self) -> Option<A::Aggregate> {
        let wheels = [
            self.seconds_wheel.total(),
            self.minutes_wheel.total(),
            self.hours_wheel.total(),
            self.days_wheel.total(),
            self.weeks_wheel.total(),
            self.months_wheel.total(),
            self.years_wheel.total(),
        ];
        Self::reduce(wheels, &self.aggregator).map(|partial_agg| self.aggregator.lower(partial_agg))
    }

    #[inline]
    fn reduce(
        partial_aggs: impl IntoIterator<Item = Option<A::PartialAggregate>>,
        aggregator: &A,
    ) -> Option<A::PartialAggregate> {
        partial_aggs
            .into_iter()
            .reduce(|acc, b| match (acc, b) {
                (Some(curr), Some(agg)) => Some(aggregator.combine(curr, agg)),
                (None, Some(_)) => b,
                _ => acc,
            })
            .flatten()
    }

    /// Tick the wheel by a single unit (second)
    ///
    /// In the worst case, a tick may cause a rotation of all the wheels in the hierarchy.
    #[inline]
    fn tick(&mut self) {
        let aggregator = &self.aggregator;

        self.watermark += Self::SECOND_AS_MS;

        // full rotation of seconds wheel
        if let Some(rot_data) = self.seconds_wheel.tick(aggregator) {
            // insert 60 seconds worth of partial aggregates into minute wheel and then tick it
            self.minutes_wheel
                .insert_rotation_data(rot_data, aggregator);

            // full rotation of minutes wheel
            if let Some(rot_data) = self.minutes_wheel.tick(aggregator) {
                // insert 60 minutes worth of partial aggregates into hours wheel and then tick it
                self.hours_wheel.insert_rotation_data(rot_data, aggregator);

                // full rotation of hours wheel
                if let Some(rot_data) = self.hours_wheel.tick(aggregator) {
                    // insert 24 hours worth of partial aggregates into days wheel and then tick it
                    self.days_wheel.insert_rotation_data(rot_data, aggregator);

                    // full rotation of days wheel
                    if let Some(rot_data) = self.days_wheel.tick(aggregator) {
                        // insert 7 days worth of partial aggregates into weeks wheel and then tick it
                        self.weeks_wheel.insert_rotation_data(rot_data, aggregator);

                        // full rotation of weeks wheel
                        if let Some(rot_data) = self.weeks_wheel.tick(aggregator) {
                            // insert 1 week worth of partial aggregates into month wheel and then tick it
                            self.months_wheel.insert_rotation_data(rot_data, aggregator);

                            if let Some(rot_data) = self.months_wheel.tick(aggregator) {
                                // insert 1 years worth of partial aggregates into year wheel and then tick it
                                self.years_wheel.insert_rotation_data(rot_data, aggregator);

                                // tick but ignore full rotations as this is the last hierarchy
                                let _ = self.years_wheel.tick(aggregator);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Returns a reference to the seconds wheel
    pub fn seconds_wheel(&self) -> &SecondsWheel<A> {
        &self.seconds_wheel
    }
    /// Returns a reference to the minutes wheel
    pub fn minutes_wheel(&self) -> &MinutesWheel<A> {
        &self.minutes_wheel
    }
    /// Returns a reference to the hours wheel
    pub fn hours_wheel(&self) -> &HoursWheel<A> {
        &self.hours_wheel
    }
    /// Returns a reference to the days wheel
    pub fn days_wheel(&self) -> &DaysWheel<A> {
        &self.days_wheel
    }

    /// Returns a reference to the weeks wheel
    pub fn weeks_wheel(&self) -> &WeeksWheel<A> {
        &self.weeks_wheel
    }
    /// Returns a reference to the months wheel
    pub fn months_wheel(&self) -> &MonthsWheel<A> {
        &self.months_wheel
    }

    /// Returns a reference to the years wheel
    pub fn years_wheel(&self) -> &YearsWheel<A> {
        &self.years_wheel
    }

    /// Merges two wheels
    ///
    /// Note that the time in `other` may be advanced and thus change state
    pub fn merge(&mut self, other: &mut Self) {
        let other_watermark = other.watermark();

        // make sure both wheels are aligned by time
        if self.watermark() > other_watermark {
            other.advance_to(self.watermark());
        } else {
            self.advance_to(other_watermark);
        }

        // merge all aggregation wheels
        self.seconds_wheel
            .merge(&other.seconds_wheel, &self.aggregator);
        self.minutes_wheel
            .merge(&other.minutes_wheel, &self.aggregator);
        self.hours_wheel.merge(&other.hours_wheel, &self.aggregator);
        self.days_wheel.merge(&other.days_wheel, &self.aggregator);
        self.weeks_wheel.merge(&other.weeks_wheel, &self.aggregator);
        self.months_wheel
            .merge(&other.months_wheel, &self.aggregator);
        self.years_wheel.merge(&other.years_wheel, &self.aggregator);
    }

    #[cfg(all(feature = "rkyv", not(feature = "alloc")))]
    /// Converts the wheel to bytes as a fixed-sized byte array
    ///
    /// Does not perform any allocations and works in no_std mode.
    pub fn serialize_to_bytes<const N: usize>(
        &self,
    ) -> Result<(usize, AlignedBytes<N>), BufferSerializerError>
    where
        PartialAggregate<A>: Serialize<BufferSerializer<AlignedBytes<N>>>,
    {
        let mut serializer = BufferSerializer::new(AlignedBytes([0u8; N]));
        let pos = serializer.serialize_value(self)?;
        Ok((pos, serializer.into_inner()))
    }

    #[cfg(all(feature = "rkyv", feature = "alloc"))]
    /// Converts the wheel to bytes using the default serializer
    pub fn as_bytes(&self) -> AlignedVec
    where
        PartialAggregate<A>: Serialize<DefaultSerializer>,
    {
        let mut serializer = DefaultSerializer::default();
        serializer.serialize_value(self).unwrap();
        serializer.into_serializer().into_inner()
    }

    #[cfg(all(feature = "rkyv", feature = "alloc"))]
    /// Deserialise given bytes into a Wheel
    ///
    /// This function will deserialize the whole wheel.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SharedDeserializeMapError>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, SharedDeserializeMap>,
    {
        unsafe { rkyv::from_bytes_unchecked(bytes) }
    }

    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a seconds wheel
    ///
    /// For read-only operations that only target the seconds wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn seconds_wheel_from_bytes(bytes: &[u8]) -> SecondsWheel<A>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived = unsafe { rkyv::archived_root::<Self>(bytes) };
        archived.seconds_wheel.deserialize(&mut Infallible).unwrap()
    }

    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a minutes wheel
    ///
    /// For read-only operations that only target the minutes wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn minutes_wheel_from_bytes(bytes: &[u8]) -> MinutesWheel<A>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived_root = unsafe { rkyv::archived_root::<Self>(bytes) };
        archived_root
            .minutes_wheel
            .deserialize(&mut Infallible)
            .unwrap()
    }

    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a hours wheel
    ///
    /// For read-only operations that only target the hours wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn hours_wheel_from_bytes(bytes: &[u8]) -> HoursWheel<A>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived_root = unsafe { rkyv::archived_root::<Self>(bytes) };
        archived_root
            .hours_wheel
            .deserialize(&mut Infallible)
            .unwrap()
    }

    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a days wheel
    ///
    /// For read-only operations that only target the days wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn days_wheel_from_bytes(bytes: &[u8]) -> DaysWheel<A>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived_root = unsafe { rkyv::archived_root::<Self>(bytes) };
        archived_root
            .days_wheel
            .deserialize(&mut Infallible)
            .unwrap()
    }

    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a weeks wheel
    ///
    /// For read-only operations that only target the weeks wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn weeks_wheel_from_bytes(bytes: &[u8]) -> WeeksWheel<A>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived_root = unsafe { rkyv::archived_root::<Self>(bytes) };
        archived_root
            .weeks_wheel
            .deserialize(&mut Infallible)
            .unwrap()
    }
    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a months wheel
    ///
    /// For read-only operations that only target the months wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn months_wheel_from_bytes(bytes: &[u8]) -> MonthsWheel<A>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived_root = unsafe { rkyv::archived_root::<Self>(bytes) };
        archived_root
            .months_wheel
            .deserialize(&mut Infallible)
            .unwrap()
    }
    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a years wheel
    ///
    /// For read-only operations that only target the years wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn years_wheel_from_bytes(bytes: &[u8]) -> YearsWheel<A>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived_root = unsafe { rkyv::archived_root::<Self>(bytes) };
        archived_root
            .years_wheel
            .deserialize(&mut Infallible)
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::{aggregator::U32SumAggregator, time::*, *};

    #[test]
    fn interval_test() {
        let mut time = 0;
        let mut wheel = Wheel::<U32SumAggregator>::new(time);
        wheel.advance(1.seconds());

        assert!(wheel.insert(Entry::new(1u32, 1000)).is_ok());
        assert!(wheel.insert(Entry::new(5u32, 5000)).is_ok());
        assert!(wheel.insert(Entry::new(11u32, 11000)).is_ok());

        wheel.advance(5.seconds());
        assert_eq!(wheel.watermark(), 6000);

        assert_eq!(wheel.interval(5.seconds()), Some(6u32));
        assert_eq!(wheel.interval(1.seconds()), Some(5u32));

        time = 12000;
        wheel.advance_to(time);

        assert!(wheel.insert(Entry::new(100u32, 61000)).is_ok());
        assert!(wheel.insert(Entry::new(100u32, 63000)).is_ok());
        assert!(wheel.insert(Entry::new(100u32, 67000)).is_ok());

        // go pass seconds wheel
        time = 65000;
        wheel.advance_to(time);

        assert_eq!(wheel.interval(63.seconds()), Some(117u32));
        assert_eq!(wheel.interval(64.seconds()), Some(217u32));
        // TODO: this does not work properly.
        // assert_eq!(wheel.interval(120.seconds()), Some(217u32));
        assert_eq!(wheel.interval(2.days()), None);
        assert_eq!(wheel.interval(50.hours()), None);
    }

    #[test]
    fn mixed_timestamp_insertions_test() {
        let aggregator = U32SumAggregator;
        let mut time = 1000;
        let mut wheel = Wheel::new(time);
        wheel.advance_to(time);

        assert!(wheel.insert(Entry::new(1u32, 1000)).is_ok());
        assert!(wheel.insert(Entry::new(5u32, 5000)).is_ok());
        assert!(wheel.insert(Entry::new(11u32, 11000)).is_ok());

        assert_eq!(wheel.seconds_wheel().lower(0, &aggregator), Some(1u32));

        time = 6000; // new watermark
        wheel.advance_to(time);

        assert_eq!(wheel.seconds_wheel().total(), Some(6u32));
        // check we get the same result by combining the range of last 6 seconds
        assert_eq!(
            wheel
                .seconds_wheel()
                .combine_and_lower_range(0..5, &aggregator),
            Some(6u32)
        );
    }

    #[test]
    fn write_ahead_test() {
        let mut time = 0;
        let mut wheel = Wheel::<U32SumAggregator>::new(time);

        assert_eq!(wheel.seconds_wheel().write_ahead_len(), SECONDS_CAP);

        time += 58000; // 58 seconds
        wheel.advance_to(time);
        // head: 58
        // tail:0
        assert_eq!(wheel.seconds_wheel().write_ahead_len(), SECONDS_CAP - 58);

        // current watermark is 58000, this should be rejected
        assert!(wheel
            .insert(Entry::new(11u32, 11000))
            .unwrap_err()
            .is_late());

        // current watermark is 58000, with max_write_ahead_ts 128000.
        // should overflow
        assert!(wheel
            .insert(Entry::new(11u32, 158000))
            .unwrap_err()
            .is_overflow());

        time = 150000; // 150 seconds
        wheel.advance_to(time);
        assert_eq!(
            wheel.write_ahead_len(),
            Wheel::<U32SumAggregator>::MAX_WRITE_AHEAD_SLOTS
        );
    }

    #[test]
    fn full_cycle_test() {
        let mut wheel = Wheel::<U32SumAggregator>::new(0);

        let ticks = wheel.remaining_ticks() - 1;
        wheel.advance(time::Duration::seconds(ticks as i64));

        // one tick away from full cycle clear
        assert_eq!(wheel.seconds_wheel.rotation_count(), SECONDS - 1);
        assert_eq!(wheel.minutes_wheel.rotation_count(), MINUTES - 1);
        assert_eq!(wheel.hours_wheel.rotation_count(), HOURS - 1);
        assert_eq!(wheel.days_wheel.rotation_count(), DAYS - 1);
        assert_eq!(wheel.weeks_wheel.rotation_count(), WEEKS - 1);
        assert_eq!(wheel.months_wheel.rotation_count(), MONTHS - 1);
        assert_eq!(wheel.years_wheel.rotation_count(), YEARS - 1);

        // force full cycle clear
        wheel.advance(1.seconds());

        // rotation count of all wheels should be zero
        assert_eq!(wheel.seconds_wheel.rotation_count(), 0);
        assert_eq!(wheel.minutes_wheel.rotation_count(), 0);
        assert_eq!(wheel.hours_wheel.rotation_count(), 0);
        assert_eq!(wheel.days_wheel.rotation_count(), 0);
        assert_eq!(wheel.weeks_wheel.rotation_count(), 0);
        assert_eq!(wheel.months_wheel.rotation_count(), 0);
        assert_eq!(wheel.years_wheel.rotation_count(), 0);

        // Verify len of all wheels
        assert_eq!(wheel.seconds_wheel().len(), SECONDS);
        assert_eq!(wheel.minutes_wheel().len(), MINUTES);
        assert_eq!(wheel.hours_wheel().len(), HOURS);
        assert_eq!(wheel.days_wheel().len(), DAYS);
        assert_eq!(wheel.weeks_wheel().len(), WEEKS);
        assert_eq!(wheel.months_wheel().len(), MONTHS);
        assert_eq!(wheel.years_wheel().len(), YEARS);

        assert!(wheel.is_full());
        assert!(!wheel.is_empty());
        assert!(wheel.landmark().is_none());
    }

    #[cfg(feature = "drill_down")]
    #[test]
    fn drill_down_test() {
        use crate::agg_wheel::DrillCut;

        use super::aggregator::U64SumAggregator;
        let mut time = 0;
        let mut wheel = Wheel::<U64SumAggregator>::with_drill_down(time);

        let days_as_secs = time::Duration::days((DAYS + 1) as i64).whole_seconds();

        for _ in 0..days_as_secs {
            let entry = Entry::new(1u64, time);
            wheel.insert(entry).unwrap();
            time += 1000; // increase by 1 second
            wheel.advance_to(time);
        }

        // can't drill down on seconds wheel as it is the first wheel
        assert!(wheel.seconds_wheel().drill_down(1).is_none());

        // Drill down on each wheel (e.g., minute, hours, days) and confirm summed results

        let slots = wheel.minutes_wheel().drill_down(1).unwrap();
        assert_eq!(slots.iter().sum::<u64>(), 60u64);

        let slots = wheel.hours_wheel().drill_down(1).unwrap();
        assert_eq!(slots.iter().sum::<u64>(), 60u64 * 60);

        let slots = wheel.days_wheel().drill_down(1).unwrap();
        assert_eq!(slots.iter().sum::<u64>(), 60u64 * 60 * 24);

        // drill down range of 3 and confirm combined aggregates
        let decoded = wheel.minutes_wheel().drill_down_range(..3).unwrap();
        assert_eq!(decoded[0], 3);
        assert_eq!(decoded[1], 3);
        assert_eq!(decoded[59], 3);

        // test cut of last 5 seconds of last 1 minute + first 10 aggregates of last 2 min
        let decoded = wheel
            .minutes_wheel()
            .drill_down_cut(
                DrillCut {
                    slot: 1,
                    range: 55..,
                },
                DrillCut {
                    slot: 2,
                    range: ..10,
                },
            )
            .unwrap();
        assert_eq!(decoded.len(), 15);
        let sum = decoded.iter().sum::<u64>();
        assert_eq!(sum, 15u64);

        // drill down whole of minutes wheel
        let decoded = wheel.minutes_wheel().drill_down_range(..).unwrap();
        let sum = decoded.iter().sum::<u64>();
        assert_eq!(sum, 3600u64);
    }

    #[cfg(feature = "drill_down")]
    #[test]
    fn drill_down_holes_test() {
        let mut time = 0;
        let mut wheel = Wheel::<U32SumAggregator>::with_drill_down(time);

        for _ in 0..30 {
            let entry = Entry::new(1u32, time);
            wheel.insert(entry).unwrap();
            time += 2000; // increase by 2 seconds
            wheel.advance_to(time);
        }

        wheel.advance_to(time);

        // confirm there are "holes" as we bump time by 2 seconds above
        let decoded = wheel.minutes_wheel().drill_down(1).unwrap();
        assert_eq!(decoded[0], 1);
        assert_eq!(decoded[1], 0);
        assert_eq!(decoded[2], 1);
        assert_eq!(decoded[3], 0);

        assert_eq!(decoded[58], 1);
        assert_eq!(decoded[59], 0);
    }

    #[test]
    fn merge_test() {
        let time = 0;
        let mut wheel = Wheel::<U32SumAggregator>::new(time);

        let entry = Entry::new(1u32, 5000);
        wheel.insert(entry).unwrap();

        wheel.advance(7.days());

        let fresh_wheel_time = 0;
        let mut fresh_wheel = Wheel::new(fresh_wheel_time);
        fresh_wheel.merge(&mut wheel);

        assert_eq!(fresh_wheel.watermark(), wheel.watermark());
        assert_eq!(fresh_wheel.landmark(), wheel.landmark());
        assert_eq!(fresh_wheel.remaining_ticks(), wheel.remaining_ticks());
    }

    #[cfg(feature = "drill_down")]
    #[test]
    fn merge_drill_down_test() {
        let mut time = 0;
        let mut wheel = Wheel::<U32SumAggregator>::with_drill_down(time);

        for _ in 0..30 {
            let entry = Entry::new(1u32, time);
            wheel.insert(entry).unwrap();
            time += 2000; // increase by 2 seconds
            wheel.advance_to(time);
        }

        wheel.advance_to(time);

        let mut time = 0;
        let mut other_wheel = Wheel::<U32SumAggregator>::with_drill_down(time);

        for _ in 0..30 {
            let entry = Entry::new(1u32, time);
            other_wheel.insert(entry).unwrap();
            time += 2000; // increase by 2 seconds
            other_wheel.advance_to(time);
        }

        other_wheel.advance_to(time);

        // merge other_wheel into ´wheel´
        wheel.merge(&mut other_wheel);

        // same as drill_down_holes test but confirm that drill down slots have be merged between wheels
        let decoded = wheel.minutes_wheel().drill_down(1).unwrap();
        assert_eq!(decoded[0], 2);
        assert_eq!(decoded[1], 0);
        assert_eq!(decoded[2], 2);
        assert_eq!(decoded[3], 0);

        assert_eq!(decoded[58], 2);
        assert_eq!(decoded[59], 0);
    }

    #[cfg(all(feature = "rkyv", feature = "std"))]
    #[test]
    fn serde_test() {
        let aggregator = U32SumAggregator;
        let mut time = 1000;
        let wheel: Wheel<U32SumAggregator> = Wheel::new(time);

        let mut raw_wheel = wheel.as_bytes();

        for _ in 0..3 {
            let mut wheel = Wheel::<U32SumAggregator>::from_bytes(&raw_wheel).unwrap();
            wheel.insert(Entry::new(1u32, time + 100)).unwrap();
            raw_wheel = wheel.as_bytes();
        }

        let mut wheel = Wheel::from_bytes(&raw_wheel).unwrap();

        time += 1000;
        wheel.advance_to(time);

        assert_eq!(
            wheel
                .seconds_wheel()
                .combine_and_lower_range(.., &aggregator),
            Some(3u32)
        );

        let raw_wheel = wheel.as_bytes();

        // deserialize seconds wheel only and confirm same query works
        let seconds_wheel = Wheel::seconds_wheel_from_bytes(&raw_wheel);

        assert_eq!(
            seconds_wheel.combine_and_lower_range(.., &aggregator),
            Some(3u32)
        );
    }
}
