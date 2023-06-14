use core::{
    fmt::Debug,
    iter::IntoIterator,
    option::{
        Option,
        Option::{None, Some},
    },
    result::{
        Result,
        Result::{Err, Ok},
    },
    time::Duration,
};

#[cfg(feature = "rkyv")]
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
    ser::Serializer,
    AlignedVec,
};
#[cfg(feature = "rkyv")]
use rkyv::{with::Skip, Archive, Deserialize, Infallible, Serialize};

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

use super::{Entry, Error};
use crate::{agg_wheel::AggregationWheel, aggregator::Aggregator, time, waw::Waw};

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
pub const WEEKS: usize = 52;

#[cfg(feature = "years_size_10")]
pub const YEARS: usize = 10;
#[cfg(feature = "years_size_100")]
pub const YEARS: usize = 100;

// Default Wheel Capacities (power of two)
const SECONDS_CAP: usize = 64;
const MINUTES_CAP: usize = 64;
const HOURS_CAP: usize = 32;
const DAYS_CAP: usize = 8;
const WEEKS_CAP: usize = 64;
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
/// Type alias for an AggregationWheel representing years
pub type YearsWheel<A> = AggregationWheel<YEARS_CAP, A>;

const WRITE_AHEAD_SLOTS: usize = 64;

cfg_rkyv! {
    // Alias for an aggregators [`PartialAggregate`] type
    pub type PartialAggregate<A> = <A as Aggregator>::PartialAggregate;
    // Alias for an aggregators [`PartialAggregate`] archived type
    type Archived<A> = <<A as Aggregator>::PartialAggregate as Archive>::Archived;
    // Alias for the default serializer used by the crate
    pub type DefaultSerializer = CompositeSerializer<
        AlignedSerializer<AlignedVec>,
        FallbackScratch<HeapScratch<4096>, AllocScratch>,
        SharedSerializeMap,
    >;
}

/// Hierarchical Aggregation Wheel (HAW)
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Clone, Debug)]
pub struct Wheel<A>
where
    A: Aggregator,
{
    #[cfg_attr(feature = "rkyv", with(Skip))]
    aggregator: A,
    watermark: u64,
    #[cfg_attr(feature = "rkyv", with(Skip), omit_bounds)]
    waw: Waw<WRITE_AHEAD_SLOTS, A>,
    #[cfg_attr(feature = "rkyv", omit_bounds)]
    seconds_wheel: MaybeWheel<SECONDS_CAP, A>,
    #[cfg_attr(feature = "rkyv", omit_bounds)]
    minutes_wheel: MaybeWheel<MINUTES_CAP, A>,
    hours_wheel: MaybeWheel<HOURS_CAP, A>,
    days_wheel: MaybeWheel<DAYS_CAP, A>,
    weeks_wheel: MaybeWheel<WEEKS_CAP, A>,
    years_wheel: MaybeWheel<YEARS_CAP, A>,
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
    const YEAR_AS_SECS: u64 = Self::WEEK_AS_SECS * WEEKS as u64;
    const CYCLE_LENGTH_SECS: u64 = Self::CYCLE_LENGTH.whole_seconds() as u64;

    const TOTAL_SECS_IN_WHEEL: u64 = Self::YEAR_AS_SECS * YEARS as u64;
    pub const CYCLE_LENGTH: time::Duration =
        time::Duration::seconds((Self::YEAR_AS_SECS * (YEARS as u64 + 1)) as i64); // need 1 extra to force full cycle rotation
    pub const TOTAL_WHEEL_SLOTS: usize = SECONDS + MINUTES + HOURS + DAYS + WEEKS + YEARS;
    pub const MAX_WRITE_AHEAD_SLOTS: usize = WRITE_AHEAD_SLOTS;

    /// Creates a new Wheel starting from the given time with drill-down capabilities
    ///
    /// Time is represented as milliseconds
    pub fn with_drill_down(time: u64) -> Self {
        Self {
            aggregator: A::default(),
            watermark: time,
            waw: Waw::default(),
            seconds_wheel: MaybeWheel::with_capacity_and_drill_down(SECONDS),
            minutes_wheel: MaybeWheel::with_capacity_and_drill_down(MINUTES),
            hours_wheel: MaybeWheel::with_capacity_and_drill_down(HOURS),
            days_wheel: MaybeWheel::with_capacity_and_drill_down(DAYS),
            weeks_wheel: MaybeWheel::with_capacity_and_drill_down(WEEKS),
            years_wheel: MaybeWheel::with_capacity_and_drill_down(YEARS),
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
            waw: Waw::default(),
            seconds_wheel: MaybeWheel::with_capacity(SECONDS),
            minutes_wheel: MaybeWheel::with_capacity(MINUTES),
            hours_wheel: MaybeWheel::with_capacity(HOURS),
            days_wheel: MaybeWheel::with_capacity(DAYS),
            weeks_wheel: MaybeWheel::with_capacity(WEEKS),
            years_wheel: MaybeWheel::with_capacity(YEARS),
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
        self.waw.write_ahead_len()
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
        let year_secs = self.years_wheel.rotation_count() as u64 * Self::YEAR_AS_SECS;
        let cycle_time = secs + min_secs + hr_secs + day_secs + week_secs + year_secs;
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
            let rem_ticks = self.seconds_wheel.get_or_insert().ticks_remaining();
            tick_n(rem_ticks, self);
            ticks -= rem_ticks;

            // calculate how many fast_ticks we can perform
            let fast_ticks = ticks.saturating_div(SECONDS);

            // TODO: verify Write-ahead wheel logic
            if fast_ticks == 0 {
                // if fast ticks is 0, then tick normally
                tick_n(ticks, self);
            } else {
                // perform a number of fast ticks
                // NOTE: currently a fast tick is a full SECONDS rotation.
                let fast_tick_ms = (SECONDS - 1) as u64 * Self::SECOND_AS_MS;
                for _ in 0..fast_ticks {
                    self.seconds_wheel.get_or_insert().fast_skip_tick();
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
            if self.waw.can_write_ahead(seconds) {
                self.waw.write_ahead(seconds, entry.data, &self.aggregator);
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

    /// Return the current watermark as milliseconds for this wheel
    #[inline]
    pub fn watermark(&self) -> u64 {
        self.watermark
    }
    /// Returns a full aggregate in the given time interval
    pub fn interval(&self, dur: time::Duration) -> Option<A::PartialAggregate> {
        // closure that turns i64 to None if it is zero
        let to_option = |num: i64| {
            if num == 0 {
                None
            } else {
                Some(num as usize)
            }
        };
        let second = to_option(dur.whole_seconds() % SECONDS as i64);
        let minute = to_option(dur.whole_minutes() % MINUTES as i64);
        let hour = to_option(dur.whole_hours() % HOURS as i64);
        let day = to_option(dur.whole_days() % DAYS as i64);
        dbg!((second, minute, hour, day));
        // TODO: integrate week and year.
        //let week = to_option(dur.whole_weeks() % WEEKS as i64);
        //let year = to_option((dur.whole_weeks() / ?) % YEARS as i64);
        //dbg!((second, minute, hour, day, week, year));
        self.combine_time(second, minute, hour, day)
    }

    #[inline]
    pub fn combine_time(
        &self,
        second: Option<usize>,
        minute: Option<usize>,
        hour: Option<usize>,
        day: Option<usize>,
    ) -> Option<A::PartialAggregate> {
        // Single-Wheel Query => time must be lower than wheel.len() otherwise it wil panic as range out of bound
        // Multi-Wheel Query => Need to make sure we don't duplicate data across granularities.
        let aggregator = &self.aggregator;
        use core::cmp;

        // TODO: fix type conversions
        match (day, hour, minute, second) {
            // dhms
            (Some(day), Some(hour), Some(minute), Some(second)) => {
                // Do not query below rotation count
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);

                let sec = self.seconds_wheel.interval(second);
                let min = self.minutes_wheel.interval(minute);
                let hr = self.hours_wheel.interval(hour);
                let day = self.days_wheel.interval(day);

                Self::reduce([sec, min, hr, day], aggregator)
            }
            // dhm
            (Some(day), Some(hour), Some(minute), None) => {
                // Do not query below rotation count
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);

                let min = self.minutes_wheel.interval(minute);
                let hr = self.hours_wheel.interval(hour);
                let day = self.days_wheel.interval(day);
                Self::reduce([min, hr, day], aggregator)
            }
            // dh
            (Some(day), Some(hour), None, None) => {
                let hour = cmp::min(self.hours_wheel.rotation_count(), hour);
                let hr = self.hours_wheel.interval(hour);
                let day = self.days_wheel.interval(day);
                Self::reduce([hr, day], aggregator)
            }
            // d
            (Some(day), None, None, None) => {
                let day = self.days_wheel.interval(day);
                Self::reduce([day], aggregator)
            }
            // hms
            (None, Some(hour), Some(minute), Some(second)) => {
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);

                let sec = self.seconds_wheel.interval(second);
                let min = self.minutes_wheel.interval(minute);
                let hr = self.hours_wheel.interval(hour);
                Self::reduce([sec, min, hr], aggregator)
            }
            // hm
            (None, Some(hour), Some(minute), None) => {
                let minute = cmp::min(self.minutes_wheel.rotation_count(), minute);
                let min = self.minutes_wheel.interval(minute);

                let hr = self.hours_wheel.interval(hour);
                Self::reduce([min, hr], aggregator)
            }
            // h
            (None, Some(hour), None, None) => {
                let hr = self.hours_wheel.interval(hour);
                Self::reduce([hr], aggregator)
            }
            // ms
            (None, None, Some(minute), Some(second)) => {
                let second = cmp::min(self.seconds_wheel.rotation_count(), second);
                let sec = self.seconds_wheel.interval(second);
                let min = self.minutes_wheel.interval(minute);
                Self::reduce([min, sec], aggregator)
            }
            // m
            (None, None, Some(minute), None) => self.minutes_wheel.interval(minute),
            // s
            (None, None, None, Some(second)) => self.seconds_wheel.interval(second),
            (_, _, _, _) => {
                panic!("combine_time was given invalid Time arguments");
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
        self.watermark += Self::SECOND_AS_MS;

        let seconds = self.seconds_wheel.get_or_insert();

        // Tick the Write-ahead wheel, if new entry insert into head of seconds wheel
        if let Some(window) = self.waw.tick() {
            let partial_agg = self.aggregator.lift(window);
            seconds.insert_head(partial_agg, &self.aggregator)
        }

        // full rotation of seconds wheel
        if let Some(rot_data) = seconds.tick() {
            // insert 60 seconds worth of partial aggregates into minute wheel and then tick it
            let minutes = self.minutes_wheel.get_or_insert();

            minutes.insert_rotation_data(rot_data);

            // full rotation of minutes wheel
            if let Some(rot_data) = minutes.tick() {
                // insert 60 minutes worth of partial aggregates into hours wheel and then tick it
                let hours = self.hours_wheel.get_or_insert();

                hours.insert_rotation_data(rot_data);

                // full rotation of hours wheel
                if let Some(rot_data) = hours.tick() {
                    // insert 24 hours worth of partial aggregates into days wheel and then tick it
                    let days = self.days_wheel.get_or_insert();
                    days.insert_rotation_data(rot_data);

                    // full rotation of days wheel
                    if let Some(rot_data) = days.tick() {
                        // insert 7 days worth of partial aggregates into weeks wheel and then tick it
                        let weeks = self.weeks_wheel.get_or_insert();

                        weeks.insert_rotation_data(rot_data);

                        // full rotation of weeks wheel
                        if let Some(rot_data) = weeks.tick() {
                            // insert 1 years worth of partial aggregates into year wheel and then tick it
                            let years = self.years_wheel.get_or_insert();
                            years.insert_rotation_data(rot_data);

                            // tick but ignore full rotations as this is the last hierarchy
                            let _ = years.tick();
                        }
                    }
                }
            }
        }
    }

    /// Returns a reference to the seconds wheel
    pub fn seconds(&self) -> Option<&SecondsWheel<A>> {
        self.seconds_wheel.as_deref()
    }
    pub fn seconds_unchecked(&self) -> &SecondsWheel<A> {
        self.seconds_wheel.as_deref().unwrap()
    }

    /// Returns a reference to the minutes wheel
    pub fn minutes(&self) -> Option<&MinutesWheel<A>> {
        self.minutes_wheel.as_deref()
    }
    pub fn minutes_unchecked(&self) -> &MinutesWheel<A> {
        self.minutes_wheel.as_deref().unwrap()
    }
    /// Returns a reference to the hours wheel
    pub fn hours(&self) -> Option<&HoursWheel<A>> {
        self.hours_wheel.as_deref()
    }
    pub fn hours_unchecked(&self) -> &HoursWheel<A> {
        self.hours_wheel.as_deref().unwrap()
    }
    /// Returns a reference to the days wheel
    pub fn days(&self) -> Option<&DaysWheel<A>> {
        self.days_wheel.as_deref()
    }
    pub fn days_unchecked(&self) -> &DaysWheel<A> {
        self.days_wheel.as_deref().unwrap()
    }

    /// Returns a reference to the weeks wheel
    pub fn weeks(&self) -> Option<&WeeksWheel<A>> {
        self.weeks_wheel.as_deref()
    }
    pub fn weeks_unchecked(&self) -> &WeeksWheel<A> {
        self.weeks_wheel.as_deref().unwrap()
    }

    /// Returns a reference to the years wheel
    pub fn years(&self) -> Option<&YearsWheel<A>> {
        self.years_wheel.as_deref()
    }
    pub fn years_unchecked(&self) -> &YearsWheel<A> {
        self.years_wheel.as_deref().unwrap()
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
        self.seconds_wheel.merge(&other.seconds_wheel);
        self.minutes_wheel.merge(&other.minutes_wheel);
        self.hours_wheel.merge(&other.hours_wheel);
        self.days_wheel.merge(&other.days_wheel);
        self.weeks_wheel.merge(&other.weeks_wheel);
        self.years_wheel.merge(&other.years_wheel);
    }

    #[cfg(feature = "rkyv")]
    /// Converts the wheel to bytes using the default serializer
    pub fn as_bytes(&self) -> AlignedVec
    where
        PartialAggregate<A>: Serialize<DefaultSerializer>,
    {
        let mut serializer = DefaultSerializer::default();
        serializer.serialize_value(self).unwrap();
        serializer.into_serializer().into_inner()
    }

    #[cfg(feature = "rkyv")]
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
    pub fn seconds_wheel_from_bytes(bytes: &[u8]) -> Option<Box<SecondsWheel<A>>>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived = unsafe { rkyv::archived_root::<Self>(bytes) };
        let mut maybe: MaybeWheel<SECONDS_CAP, A> =
            archived.seconds_wheel.deserialize(&mut Infallible).unwrap();
        maybe.wheel.take()
    }

    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a minutes wheel
    ///
    /// For read-only operations that only target the minutes wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn minutes_wheel_from_bytes(bytes: &[u8]) -> Option<Box<MinutesWheel<A>>>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived = unsafe { rkyv::archived_root::<Self>(bytes) };
        let mut maybe: MaybeWheel<MINUTES_CAP, A> =
            archived.minutes_wheel.deserialize(&mut Infallible).unwrap();
        maybe.wheel.take()
    }

    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a hours wheel
    ///
    /// For read-only operations that only target the hours wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn hours_wheel_from_bytes(bytes: &[u8]) -> Option<Box<HoursWheel<A>>>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived = unsafe { rkyv::archived_root::<Self>(bytes) };
        let mut maybe: MaybeWheel<HOURS_CAP, A> =
            archived.hours_wheel.deserialize(&mut Infallible).unwrap();
        maybe.wheel.take()
    }

    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a days wheel
    ///
    /// For read-only operations that only target the days wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn days_wheel_from_bytes(bytes: &[u8]) -> Option<Box<DaysWheel<A>>>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived = unsafe { rkyv::archived_root::<Self>(bytes) };
        let mut maybe: MaybeWheel<DAYS_CAP, A> =
            archived.days_wheel.deserialize(&mut Infallible).unwrap();
        maybe.wheel.take()
    }

    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a weeks wheel
    ///
    /// For read-only operations that only target the weeks wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn weeks_wheel_from_bytes(bytes: &[u8]) -> Option<Box<WeeksWheel<A>>>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived = unsafe { rkyv::archived_root::<Self>(bytes) };
        let mut maybe: MaybeWheel<WEEKS_CAP, A> =
            archived.weeks_wheel.deserialize(&mut Infallible).unwrap();
        maybe.wheel.take()
    }

    #[cfg(feature = "rkyv")]
    /// Deserialise given bytes into a years wheel
    ///
    /// For read-only operations that only target the years wheel,
    /// this function is more efficient as it will only deserialize a single wheel.
    pub fn years_wheel_from_bytes(bytes: &[u8]) -> Option<Box<YearsWheel<A>>>
    where
        PartialAggregate<A>: Archive,
        Archived<A>: Deserialize<PartialAggregate<A>, Infallible>,
    {
        let archived = unsafe { rkyv::archived_root::<Self>(bytes) };
        let mut maybe: MaybeWheel<YEARS_CAP, A> =
            archived.years_wheel.deserialize(&mut Infallible).unwrap();
        maybe.wheel.take()
    }
}

// An internal wrapper Struct that containing a possible AggregationWheel
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Clone)]
pub struct MaybeWheel<const CAP: usize, A: Aggregator> {
    slots: usize,
    drill_down: bool,
    wheel: Option<Box<AggregationWheel<CAP, A>>>,
}
impl<const CAP: usize, A: Aggregator> MaybeWheel<CAP, A> {
    fn with_capacity_and_drill_down(slots: usize) -> Self {
        Self {
            slots,
            drill_down: true,
            wheel: None,
        }
    }
    fn with_capacity(slots: usize) -> Self {
        Self {
            slots,
            drill_down: false,
            wheel: None,
        }
    }
    fn clear(&mut self) {
        if let Some(ref mut wheel) = self.wheel {
            wheel.clear();
        }
    }
    fn merge(&mut self, other: &Self) {
        if let Some(ref mut wheel) = self.wheel {
            wheel.merge(other.as_deref().unwrap());
        }
    }
    #[inline]
    fn interval(&self, interval: usize) -> Option<A::PartialAggregate> {
        if let Some(w) = self.wheel.as_ref() {
            w.interval(interval)
        } else {
            None
        }
    }
    #[inline]
    fn total(&self) -> Option<A::PartialAggregate> {
        if let Some(w) = self.wheel.as_ref() {
            w.total()
        } else {
            None
        }
    }
    #[inline]
    fn as_deref(&self) -> Option<&AggregationWheel<CAP, A>> {
        self.wheel.as_deref()
    }
    #[inline]
    fn rotation_count(&self) -> usize {
        self.wheel.as_ref().map(|w| w.rotation_count()).unwrap_or(0)
    }
    #[inline]
    fn len(&self) -> usize {
        self.wheel.as_ref().map(|w| w.len()).unwrap_or(0)
    }
    #[inline]
    fn get_or_insert(&mut self) -> &mut AggregationWheel<CAP, A> {
        if self.wheel.is_none() {
            let agg_wheel = {
                if self.drill_down {
                    AggregationWheel::with_capacity_and_drill_down(self.slots)
                } else {
                    AggregationWheel::with_capacity(self.slots)
                }
            };

            self.wheel = Some(Box::new(agg_wheel));
        }
        self.wheel.as_mut().unwrap().as_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{aggregator::U32SumAggregator, time::*, *};

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

        let seconds_wheel = wheel.seconds_unchecked();
        let expected: &[_] = &[&None, &Some(1u32), &None, &None, &None, &Some(5)];
        let res: Vec<&Option<u32>> = seconds_wheel.iter().collect();
        assert_eq!(&res[..], expected);

        assert_eq!(seconds_wheel.interval(5), Some(6u32));
        assert_eq!(seconds_wheel.interval(1), Some(5u32));

        time = 12000;
        wheel.advance_to(time);

        assert!(wheel.insert(Entry::new(100u32, 61000)).is_ok());
        assert!(wheel.insert(Entry::new(100u32, 63000)).is_ok());
        assert!(wheel.insert(Entry::new(100u32, 67000)).is_ok());

        // go pass seconds wheel
        time = 65000;
        wheel.advance_to(time);

        //assert_eq!(wheel.interval(63.seconds()), Some(117u32));
        //assert_eq!(wheel.interval(64.seconds()), Some(217u32));
        // TODO: this does not work properly.
        // assert_eq!(wheel.interval(120.seconds()), Some(217u32));
        //assert_eq!(wheel.interval(2.days()), None);
    }

    #[test]
    fn mixed_timestamp_insertions_test() {
        let mut time = 1000;
        let mut wheel = Wheel::<U32SumAggregator>::new(time);
        wheel.advance_to(time);

        assert!(wheel.insert(Entry::new(1u32, 1000)).is_ok());
        assert!(wheel.insert(Entry::new(5u32, 5000)).is_ok());
        assert!(wheel.insert(Entry::new(11u32, 11000)).is_ok());

        time = 6000; // new watermark
        wheel.advance_to(time);

        assert_eq!(wheel.seconds_unchecked().total(), Some(6u32));
        // check we get the same result by combining the range of last 6 seconds
        assert_eq!(
            wheel.seconds_unchecked().combine_and_lower_range(0..5),
            Some(6u32)
        );
    }

    #[test]
    fn write_ahead_test() {
        let mut time = 0;
        let mut wheel = Wheel::<U32SumAggregator>::new(time);

        time += 58000; // 58 seconds
        wheel.advance_to(time);
        // head: 58
        // tail:0
        assert_eq!(wheel.write_ahead_len(), 64);

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
        assert_eq!(wheel.years_wheel.rotation_count(), YEARS - 1);

        // force full cycle clear
        wheel.advance(1.seconds());

        // rotation count of all wheels should be zero
        assert_eq!(wheel.seconds_wheel.rotation_count(), 0);
        assert_eq!(wheel.minutes_wheel.rotation_count(), 0);
        assert_eq!(wheel.hours_wheel.rotation_count(), 0);
        assert_eq!(wheel.days_wheel.rotation_count(), 0);
        assert_eq!(wheel.weeks_wheel.rotation_count(), 0);
        assert_eq!(wheel.years_wheel.rotation_count(), 0);

        // Verify len of all wheels
        assert_eq!(wheel.seconds_unchecked().len(), SECONDS);
        assert_eq!(wheel.minutes_unchecked().len(), MINUTES);
        assert_eq!(wheel.hours_unchecked().len(), HOURS);
        assert_eq!(wheel.days_unchecked().len(), DAYS);
        assert_eq!(wheel.weeks_unchecked().len(), WEEKS);
        assert_eq!(wheel.years_unchecked().len(), YEARS);

        assert!(wheel.is_full());
        assert!(!wheel.is_empty());
        assert!(wheel.landmark().is_none());
    }

    #[test]
    fn drill_down_test() {
        use crate::{agg_wheel::DrillCut, aggregator::U64SumAggregator};

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
        assert!(wheel.seconds_unchecked().drill_down(1).is_none());

        // Drill down on each wheel (e.g., minute, hours, days) and confirm summed results

        let slots = wheel.minutes_unchecked().drill_down(1).unwrap();
        assert_eq!(slots.iter().sum::<u64>(), 60u64);

        let slots = wheel.hours_unchecked().drill_down(1).unwrap();
        assert_eq!(slots.iter().sum::<u64>(), 60u64 * 60);

        let slots = wheel.days_unchecked().drill_down(1).unwrap();
        assert_eq!(slots.iter().sum::<u64>(), 60u64 * 60 * 24);

        // drill down range of 3 and confirm combined aggregates
        let decoded = wheel.minutes_unchecked().combine_drill_down_range(..3);
        assert_eq!(decoded[0], 3);
        assert_eq!(decoded[1], 3);
        assert_eq!(decoded[59], 3);

        // test cut of last 5 seconds of last 1 minute + first 10 aggregates of last 2 min
        let decoded = wheel
            .minutes_unchecked()
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
        let decoded = wheel.minutes_unchecked().combine_drill_down_range(..);
        let sum = decoded.iter().sum::<u64>();
        assert_eq!(sum, 3600u64);
    }

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
        let decoded = wheel.minutes_unchecked().drill_down(1).unwrap();
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
        let decoded = wheel.minutes_unchecked().drill_down(1).unwrap();
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
        let time = 1000;
        let wheel: Wheel<U32SumAggregator> = Wheel::new(time);

        let mut raw_wheel = wheel.as_bytes();

        for _ in 0..3 {
            let mut wheel = Wheel::<U32SumAggregator>::from_bytes(&raw_wheel).unwrap();
            wheel.insert(Entry::new(1u32, time + 100)).unwrap();
            raw_wheel = wheel.as_bytes();
        }

        assert!(Wheel::<U32SumAggregator>::from_bytes(&raw_wheel).is_ok());

        // TODO: fix WaW serialization
        /*
        time += 1000;
        wheel.advance_to(time);

        assert_eq!(
            wheel.seconds_unchecked().combine_and_lower_range(..),
            Some(3u32)
        );

        let raw_wheel = wheel.as_bytes();

        // deserialize seconds wheel only and confirm same query works
        let seconds_wheel =
            Wheel::<U32SumAggregator>::seconds_wheel_from_bytes(&raw_wheel).unwrap();

        assert_eq!(seconds_wheel.combine_and_lower_range(..), Some(3u32));
        */
    }
}
