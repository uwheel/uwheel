use core::{
    cmp,
    fmt::{self, Display},
};
use time::OffsetDateTime;

use super::{
    super::write::WriterWheel,
    aggregation::{
        conf::{DataLayout, RetentionPolicy, WheelMode},
        maybe::MaybeWheel,
        Wheel,
    },
    plan::{ExecutionPlan, WheelAggregation, WheelRanges},
};

use crate::{
    aggregator::Aggregator,
    delta::DeltaState,
    wheels::read::{
        aggregation::combine_or_insert,
        plan::{CombinedAggregation, WheelAggregations},
    },
    window::{WindowAggregate, WindowManager},
    Duration,
    Window,
};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "profiler")]
use super::stats::Stats;
#[cfg(feature = "profiler")]
use uwheel_stats::profile_scope;

crate::cfg_timer! {
    #[cfg(not(feature = "std"))]
    use alloc::{boxed::Box, rc::Rc};
    use crate::wheels::timer::{RawTimerWheel, TimerWheel, TimerError, TimerAction};
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

/// Configuration for a Hierarchical Aggregate Wheel
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
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
    /// Optimizer configuration
    pub optimizer: Optimizer,
    /// Flag indicating whether to maintain deltas within the wheel
    pub generate_deltas: bool,
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
            optimizer: Default::default(),
            generate_deltas: false,
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
    /// Configures all wheels to use the same `WheelMode`
    ///
    /// # Safety
    ///
    /// If using ``WheelMode::Index`` in combination with explicit SIMD support,
    /// then make sure to convert wheels to support SIMD (see ``Haw::to_simd_wheels``).
    pub fn with_mode(mut self, mode: WheelMode) -> Self {
        self.seconds.set_mode(mode);
        self.minutes.set_mode(mode);
        self.hours.set_mode(mode);
        self.days.set_mode(mode);
        self.weeks.set_mode(mode);
        self.years.set_mode(mode);

        self
    }

    /// Configures all wheels with prefix-sum enabled
    pub fn with_prefix_sum(mut self) -> Self {
        self.seconds.set_data_layout(DataLayout::Prefix);
        self.minutes.set_data_layout(DataLayout::Prefix);
        self.hours.set_data_layout(DataLayout::Prefix);
        self.days.set_data_layout(DataLayout::Prefix);
        self.weeks.set_data_layout(DataLayout::Prefix);
        self.years.set_data_layout(DataLayout::Prefix);

        self
    }

    /// Configures a global retention policy across all granularities
    pub fn with_retention_policy(mut self, policy: RetentionPolicy) -> Self {
        self.seconds.set_retention_policy(policy);
        self.minutes.set_retention_policy(policy);
        self.hours.set_retention_policy(policy);
        self.days.set_retention_policy(policy);
        self.weeks.set_retention_policy(policy);
        self.years.set_retention_policy(policy);

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
    /// Configures the weeks granularity
    pub fn with_weeks(mut self, weeks: WheelConf) -> Self {
        self.weeks = weeks;
        self
    }
    /// Configures the years granularity
    pub fn with_years(mut self, years: WheelConf) -> Self {
        self.years = years;
        self
    }

    /// Configures the wheel to generate and maintain deltas
    pub fn with_deltas(mut self) -> Self {
        self.generate_deltas = true;
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

/// A type containing error variants that
#[derive(Debug, Copy, Clone)]
pub enum RangeError {
    /// Range start is an invalid unix timestamp
    InvalidStart {
        /// Invalid start unix timestamp
        start_ms: u64,
    },
    /// Range end is an invalid unix timestamp
    InvalidEnd {
        /// Invalid end unix timestamp
        end_ms: u64,
    },
}
impl Display for RangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeError::InvalidStart { start_ms } => {
                write!(f, "{start_ms} is not a valid unix timestamp")
            }
            RangeError::InvalidEnd { end_ms } => {
                write!(f, "{end_ms} is not a valid unix timestamp")
            }
        }
    }
}

/// A Wheel time range representing a closed-open interval of [start, end)
///
/// # Example
/// ```
/// use uwheel::WheelRange;
///
/// let start = 1699488000000;
/// let end = 1699491600000;
/// assert!(WheelRange::new(start, end).is_ok());
/// ```
#[derive(Debug, Copy, Hash, PartialEq, Eq, Clone)]
pub struct WheelRange {
    pub(crate) start: OffsetDateTime,
    pub(crate) end: OffsetDateTime,
}
impl WheelRange {
    /// Creates a WheelRange using a start and end timestamp as unix timestamps in milliseconds
    pub fn new(start_ms: u64, end_ms: u64) -> Result<Self, RangeError> {
        // NOTE: internally we have to convert it to seconds for `OffsetDateTime`
        let start = OffsetDateTime::from_unix_timestamp(start_ms as i64 / 1000)
            .map_err(|_| RangeError::InvalidStart { start_ms })?;

        let end = OffsetDateTime::from_unix_timestamp(end_ms as i64 / 1000)
            .map_err(|_| RangeError::InvalidEnd { end_ms })?;

        Ok(Self { start, end })
    }
    /// Creates an unchecked WheelRange using start and end timestamps as unix timestamps in milliseconds
    ///
    /// # Panics
    ///
    /// This function panics if given an invalid unix timestamp (See [Self::new] for a safe version)
    pub fn new_unchecked(start_ms: u64, end_ms: u64) -> Self {
        Self::new(start_ms, end_ms).unwrap()
    }
    #[doc(hidden)]
    pub fn from(start: OffsetDateTime, end: OffsetDateTime) -> Self {
        Self { start, end }
    }
}

impl WheelRange {
    /// Returns the lowest granularity of the Wheel range
    pub(crate) fn lowest_granularity(&self) -> Granularity {
        let is_seconds = self.start.second() != 0 || self.end.second() != 0;
        let is_minutes = self.start.minute() != 0 || self.end.minute() != 0;
        let is_hours = self.start.hour() != 0 || self.end.hour() != 0;
        let is_days = self.start.day() != 0 || self.end.day() != 0;
        if is_seconds {
            Granularity::Second
        } else if is_minutes {
            Granularity::Minute
        } else if is_hours {
            Granularity::Hour
        } else if is_days {
            Granularity::Day
        } else {
            unimplemented!("Weeks and years not supported");
        }
    }
    /// Returns an estimation of the number of scans
    pub fn scan_estimation(&self) -> i64 {
        let dur = self.duration();
        match self.lowest_granularity() {
            Granularity::Second => dur.whole_seconds(),
            Granularity::Minute => dur.whole_minutes(),
            Granularity::Hour => dur.whole_hours(),
            Granularity::Day => dur.whole_days(),
        }
    }

    /// Returns a Duration object for the range
    #[inline]
    pub fn duration(&self) -> Duration {
        Duration::seconds((self.end - self.start).whole_seconds())
    }
}

#[derive(Debug, Copy, PartialEq, Eq, Clone)]
#[repr(usize)]
pub(crate) enum Granularity {
    Second,
    Minute,
    Hour,
    Day,
}

/// Default threshold for SIMD-based Wheel Aggregations
pub const DEFAULT_SIMD_THRESHOLD: usize = 15000;

/// Optimizer Heuristics
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone, Copy)]
pub struct Heuristics {
    simd_threshold: usize,
}

impl Default for Heuristics {
    fn default() -> Self {
        Self {
            simd_threshold: DEFAULT_SIMD_THRESHOLD,
        }
    }
}

/// HAW Query optimization configuration
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Default, Debug, Clone, Copy)]
pub struct Optimizer {
    /// Defines whether the optimizer should use framework provided hints
    use_hints: bool,
    /// A set of heuristics that the optimizer takes into context
    heuristics: Heuristics,
}
impl Optimizer {
    /// Sets the use hints flag
    pub fn use_hints(&mut self, use_hints: bool) {
        self.use_hints = use_hints;
    }
}

/// Hierarchical Aggregate Wheel
///
/// This data structure can be used in standalone fashion and may be updated through [Haw::delta_advance].
///
/// # How it works
///
/// Similarly to Hierarchical Wheel Timers, HAW exploits the hierarchical nature of time and utilise several aggregation wheels,
/// each with a different time granularity. This enables a compact representation of aggregates across time
/// with a low memory footprint and makes it highly compressible and efficient to store on disk.
/// HAWs are event-time driven and indexed by low watermarking which means that no timestamps are stored as they are implicit in the wheel slots.
/// It is up to the user of the wheel to advance the low watermark and thus roll up aggregates continously up the time hierarchy.
/// For instance, to store aggregates with second granularity up to 10 years, we would need the following aggregation wheels:
///
/// * Seconds wheel with 60 slots
/// * Minutes wheel with 60 slots
/// * Hours wheel with 24 slots
/// * Days wheel with 7 slots
/// * Weeks wheel with 52 slots
/// * Years wheel with 10 slots
///
/// The above scheme results in a total of 213 wheel slots. This is the minimum number of slots
/// required to support rolling up aggregates across 10 years with second granularity.
///
/// # Query Optimizer
///
/// HAW is equipped with a wheel-based query optimizer whose cost function minimizes the number of aggregate operations for a given time range query.
/// The Optimizer takes advantage of framework-provided hints such as SIMD compatibility and algebraic properties (invertibility) to execute more efficient plans.
///
/// # Data Retention
///
/// HAW has built-in data retention capabilities meaning it can store more wheel slots than the specified aggregate scheme.
/// For instance, if you know that queries at the minute granularity will be common then the wheel may be configured to retain all minutes.
///
/// # Example
///
/// ```
/// use uwheel::{aggregator::sum::U32SumAggregator, NumericalDuration, Haw};
///
/// // Creates a Haw with default configuration
/// let mut haw: Haw<U32SumAggregator> = Haw::default();
/// ```
#[repr(C)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
pub struct Haw<A>
where
    A: Aggregator,
{
    /// The current low watermark for this wheel
    watermark: u64,
    /// A seconds wheel which may or may not be initialized
    seconds_wheel: MaybeWheel<A>,
    /// A minutes wheel which may or may not be initialized
    minutes_wheel: MaybeWheel<A>,
    /// A hours wheel which may or may not be initialized
    hours_wheel: MaybeWheel<A>,
    /// A days wheel which may or may not be initialized
    days_wheel: MaybeWheel<A>,
    /// A weeks  wheel which may or may not be initialized
    weeks_wheel: MaybeWheel<A>,
    /// A years  wheel which may or may not be initialized
    years_wheel: MaybeWheel<A>,
    /// An optional window manager that manages windows if configured
    window_manager: Option<WindowManager<A>>,
    /// Defines the configuration of the Hierarchical Aggregate Wheel
    conf: HawConf,
    /// Maintains deltas if the wheel has been configured to do so
    delta: DeltaState<A::PartialAggregate>,
    #[cfg(feature = "timer")]
    #[cfg_attr(feature = "serde", serde(skip))]
    /// A hierarchical timing wheel for scheduling user-defined functions
    timer: TimerWheel<A>,
    #[cfg(feature = "profiler")]
    /// A profiler that records latencies of various Haw operations
    stats: Stats,
}

impl<A: Aggregator> Default for Haw<A> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<A> Haw<A>
where
    A: Aggregator,
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

    /// Creates a new Wheel from the given configuration
    pub fn new(conf: HawConf) -> Self {
        Self {
            watermark: conf.watermark,
            seconds_wheel: MaybeWheel::new(conf.seconds),
            minutes_wheel: MaybeWheel::new(conf.minutes),
            hours_wheel: MaybeWheel::new(conf.hours),
            days_wheel: MaybeWheel::new(conf.days),
            weeks_wheel: MaybeWheel::new(conf.weeks),
            years_wheel: MaybeWheel::new(conf.years),
            conf,
            delta: DeltaState::new(conf.watermark, Vec::new()),
            window_manager: None,
            #[cfg(feature = "timer")]
            timer: TimerWheel::new(RawTimerWheel::default()),
            #[cfg(feature = "profiler")]
            stats: Stats::default(),
        }
    }

    #[doc(hidden)]
    pub fn set_optimizer_hints(&mut self, hints: bool) {
        self.conf.optimizer.use_hints = hints;
    }

    /// Returns the current DeltaState object
    pub fn delta_state(&self) -> DeltaState<A::PartialAggregate> {
        self.delta.clone()
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
    // Returns the current low watermark as [OffsetDateTime]
    #[inline]
    fn now(&self) -> OffsetDateTime {
        Self::to_offset_date(self.watermark)
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
    pub fn current_time_in_cycle(&self) -> Duration {
        let secs = self.seconds_wheel.rotation_count() as u64;
        let min_secs = self.minutes_wheel.rotation_count() as u64 * Self::MINUTES_AS_SECS;
        let hr_secs = self.hours_wheel.rotation_count() as u64 * Self::HOURS_AS_SECS;
        let day_secs = self.days_wheel.rotation_count() as u64 * Self::DAYS_AS_SECS;
        let week_secs = self.weeks_wheel.rotation_count() as u64 * Self::WEEK_AS_SECS;
        let year_secs = self.years_wheel.rotation_count() as u64 * Self::YEAR_AS_SECS;
        let cycle_time = secs + min_secs + hr_secs + day_secs + week_secs + year_secs;
        Duration::seconds(cycle_time as i64)
    }

    #[inline]
    const fn to_ms(ts: u64) -> u64 {
        ts.saturating_mul(1000)
    }
    #[inline]
    fn to_offset_date(ts: u64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(ts as i64 / 1000).unwrap()
    }

    /// Converts all wheels to be prefix-enabled
    ///
    /// Prefix-enabled wheels require double the space but runs any range-sum query in O(1) complexity.
    ///
    /// If wheels are already prefix-enabled then this function does nothing.
    ///
    /// # Panics
    ///
    /// The function panics if the [Aggregator] does not implement ``combine_inverse`` since
    /// it is not an invertible aggregator that supports prefix-sum range queries.
    pub fn to_prefix_wheels(&mut self) {
        assert!(
            A::invertible(),
            "Aggregator is not invertible, must implement combine_inverse."
        );

        if let Some(seconds) = self.seconds_wheel.as_mut() {
            seconds.to_prefix();

            if let Some(minutes) = self.minutes_wheel.as_mut() {
                minutes.to_prefix();

                if let Some(hours) = self.hours_wheel.as_mut() {
                    hours.to_prefix();

                    if let Some(days) = self.days_wheel.as_mut() {
                        days.to_prefix();
                    }
                }
            }
        }
    }

    /// Converts all wheels to support explicit SIMD execution.
    ///
    /// This operation assumes that the underlying wheels are configured with the default `Deque` data layout.
    ///
    /// # Safety
    /// If you have built a wheel in index mode and configured with explicit SIMD, then
    /// this function must be called once the index process is complete.
    pub fn to_simd_wheels(&mut self) {
        if let Some(seconds) = self.seconds_wheel.as_mut() {
            seconds.to_simd();

            if let Some(minutes) = self.minutes_wheel.as_mut() {
                minutes.to_simd();

                if let Some(hours) = self.hours_wheel.as_mut() {
                    hours.to_simd();

                    if let Some(days) = self.days_wheel.as_mut() {
                        days.to_simd();
                    }
                }
            }
        }
    }

    // for benching purposes right now
    #[doc(hidden)]
    pub fn convert_all_to_array(&mut self) {
        self.seconds_wheel.as_mut().unwrap().to_deque();
        self.minutes_wheel.as_mut().unwrap().to_deque();
        self.hours_wheel.as_mut().unwrap().to_deque();
        self.days_wheel.as_mut().unwrap().to_deque();
    }

    /// Installs a periodic window aggregation query
    pub fn window(&mut self, window: Window) {
        self.window_manager = Some(WindowManager::new(self.watermark, window));
    }

    /// Advances the time of the wheel aligned by the lowest unit (Second)
    #[inline]
    pub(crate) fn advance_to(
        &mut self,
        watermark: u64,
        waw: &mut WriterWheel<A>,
    ) -> Vec<WindowAggregate<A::PartialAggregate>> {
        let diff = watermark.saturating_sub(self.watermark());
        self.advance(Duration::milliseconds(diff as i64), waw)
    }

    /// Advances the wheel by applying a set of partial aggregate deltas where each delta represents the lowest unit of time
    ///
    /// Note that deltas are processed in the order of the iterator. If you have the following deltas
    /// [Some(10),  Some(20)], it will first insert Some(10) into the wheel and then Some(20).
    ///
    /// Returns possible window aggregates if there is a window installed.
    ///
    /// # Example
    /// ```
    /// use uwheel::{Haw, aggregator::sum::U64SumAggregator, NumericalDuration};
    ///
    /// let mut haw: Haw<U64SumAggregator> = Haw::default();
    /// // oldest to newest
    /// let deltas = vec![Some(10), None, Some(50), None];
    /// // Visualization:
    /// // 10
    /// // 0, 10
    /// // 50, 0, 10
    /// // 0, 50, 0, 10
    /// haw.delta_advance(deltas);
    /// assert_eq!(haw.interval(1.seconds()), Some(0));
    /// assert_eq!(haw.interval(2.seconds()), Some(50));
    /// assert_eq!(haw.interval(3.seconds()), Some(50));
    /// assert_eq!(haw.interval(4.seconds()), Some(60));
    /// ```
    #[inline]
    pub fn delta_advance(
        &mut self,
        deltas: impl IntoIterator<Item = Option<A::PartialAggregate>>,
    ) -> Vec<WindowAggregate<A::PartialAggregate>> {
        let mut windows = Vec::new();
        for delta in deltas {
            self.tick(delta);

            // Store delta if configured to
            if self.conf.generate_deltas {
                self.delta.push(delta);
            }
            // maybe handle window if there is any configured
            self.handle_window_maybe(delta, &mut windows);
        }
        windows
    }

    /// Advance the watermark of the wheel by the given [Duration]
    #[inline(always)]
    pub fn advance(
        &mut self,
        duration: Duration,
        waw: &mut WriterWheel<A>,
    ) -> Vec<WindowAggregate<A::PartialAggregate>> {
        let ticks: usize = duration.whole_seconds() as usize;
        let mut windows = Vec::new();

        if ticks <= Self::CYCLE_LENGTH_SECS as usize {
            for _ in 0..ticks {
                // tick the write wheel and freeze mutable aggregate
                let delta = waw.tick().map(A::freeze);

                // Store delta if configured to
                if self.conf.generate_deltas {
                    self.delta.push(delta);
                }

                // Tick the HAW
                self.tick(delta);

                // maybe handle window if there is any configured
                self.handle_window_maybe(delta, &mut windows);
            }
        } else {
            // Exceeds full cycle length, clear all!
            self.clear();
        }
        windows
    }

    // internal function to handle installed window queries
    fn handle_window_maybe(
        &mut self,
        delta: Option<A::PartialAggregate>,
        windows: &mut Vec<WindowAggregate<A::PartialAggregate>>,
    ) {
        match self.window_manager.as_ref().map(|wm| wm.window) {
            Some(Window::Session { .. }) => self.handle_session_window(delta, windows),
            Some(Window::Sliding { .. }) => self.handle_slicing_window(windows),
            Some(Window::Tumbling { .. }) => self.handle_slicing_window(windows),
            None => {
                // do nothing, no window installed...
            }
        }
    }

    fn handle_session_window(
        &mut self,
        delta: Option<A::PartialAggregate>,
        windows: &mut Vec<WindowAggregate<A::PartialAggregate>>,
    ) {
        // SAFETY: safe to unwrap since we confirm that there is a window manager before calling this function
        let manager = self.window_manager.as_mut().unwrap();
        let (ref mut state, ref mut aggregator) = manager.aggregator.session_as_mut();
        if let Some(partial) = delta {
            if !state.has_active_session() {
                state.activate_session(self.watermark.saturating_sub(1000)); // subtract watermark by 1 tick to align correctly.
            }
            // Aggregate the partial into the session
            aggregator.aggregate_session(partial);
            // Reset the period of the session
            state.reset_inactive_period();
        } else {
            // only do something with empty `None` delta if there is an active session
            if state.has_active_session() {
                // Bump session period by 1 tick (i.e. SECOND)
                state.bump_inactive_period(Duration::SECOND);

                // if we have reached session gap of inactivity, we can close the session
                if state.is_inactive() {
                    let session_aggregate = aggregator.get_and_reset();

                    // SAFETY: safe to unwrap since we checked if there is an active session
                    let window_start_ms = state.reset().unwrap();
                    let window_end_ms = self.watermark.saturating_sub(1000); // subtract watermark by 1 tick to align correctly.

                    windows.push(WindowAggregate {
                        window_start_ms,
                        window_end_ms,
                        aggregate: session_aggregate,
                    });
                }
            }
        }
    }

    fn handle_slicing_window(&mut self, windows: &mut Vec<WindowAggregate<A::PartialAggregate>>) {
        // NOTE: accessing window manager twice since we need to borrow mutably and immutably (self.combine_range)
        if let Some((pairs_remaining, current_pair_len)) = self.window_manager.as_mut().map(|wm| {
            let (ref mut state, _) = wm.aggregator.slicing_as_mut();
            state.pair_ticks_remaining -= 1;
            (state.pair_ticks_remaining, state.current_pair_len)
        }) {
            // if we have no more pairs to process
            if pairs_remaining == 0 {
                // query the pair range
                let from = Self::to_offset_date(self.watermark - current_pair_len as u64);
                let to = Self::to_offset_date(self.watermark);
                let pair = self.combine_range(WheelRange {
                    start: from,
                    end: to,
                });

                // SAFETY: safe to unwrap at this point
                let manager = self.window_manager.as_mut().unwrap();
                let (ref mut state, ref mut aggregator) = manager.aggregator.slicing_as_mut();

                // insert pair into window aggregator
                aggregator.push(pair.unwrap_or(A::IDENTITY));

                // Update pair metadata
                state.update_state(self.watermark);

                // handle the window itself
                if self.watermark == state.next_window_end {
                    let window_start_ms = self.watermark.saturating_sub(state.range as u64);
                    let window_end_ms = self.watermark;
                    windows.push(WindowAggregate {
                        window_start_ms,
                        window_end_ms,
                        aggregate: aggregator.query(),
                    });

                    // clean up pairs
                    for _i in 0..state.total_pairs() {
                        aggregator.pop();
                    }

                    state.next_window_end += state.slide as u64;
                }
            }
        }
    }

    /// Clears the state of all wheels
    ///
    /// Use with caution as this operation cannot be reversed.
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
    #[doc(hidden)]
    pub fn downsample(
        &self,
        _range: impl Into<WheelRange>,
        _sample_interval: Duration,
    ) -> Option<Vec<(u64, A::PartialAggregate)>> {
        todo!("https://github.com/Max-Meldrum/uwheel/issues/87");
    }
    /// Returns partial aggregates within the given date range [start, end) using the lowest granularity
    ///
    /// Returns `None` if the range cannot be answered by the wheel
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Haw, WheelRange, aggregator::sum::U32SumAggregator};
    ///
    /// // Init a HAW with time 0
    /// let mut haw: Haw<U32SumAggregator> = Haw::default();
    /// let deltas = vec![Some(10), None, Some(50), None];
    /// haw.delta_advance(deltas);
    ///
    /// assert_eq!(haw.watermark(), 4000);
    /// let range = WheelRange::new_unchecked(0, 4000);
    /// assert_eq!(haw.range(range), Some(vec![(0, 10), (1000, 0), (2000, 50), (3000, 0)]));
    /// ```
    #[inline]
    pub fn range(&self, range: impl Into<WheelRange>) -> Option<Vec<(u64, A::PartialAggregate)>> {
        let range = range.into();
        let start = range.start;
        let end = range.end;

        match range.lowest_granularity() {
            Granularity::Second => {
                let seconds = (end - start).whole_seconds() as usize;
                self.seconds_wheel
                    .range(start, seconds, Granularity::Second)
            }
            Granularity::Minute => {
                let minutes = (end - start).whole_minutes() as usize;
                self.minutes_wheel
                    .range(start, minutes, Granularity::Minute)
            }
            Granularity::Hour => {
                let hours = (end - start).whole_hours() as usize;
                self.hours_wheel.range(start, hours, Granularity::Hour)
            }
            Granularity::Day => {
                let days = (end - start).whole_days() as usize;
                self.days_wheel.range(start, days, Granularity::Day)
            }
        }
    }

    /// Returns aggregates within the given date range [start, end) using the lowest granularity
    ///
    /// Returns `None` if the range cannot be answered by the wheel
    ///
    /// See [Self::range] for an example.
    #[inline]
    pub fn range_and_lower(
        &self,
        range: impl Into<WheelRange>,
    ) -> Option<Vec<(u64, A::Aggregate)>> {
        self.range(range).map(|partials| {
            partials
                .into_iter()
                .map(|(ts, pa)| (ts, A::lower(pa)))
                .collect()
        })
    }

    /// Returns the execution plan for a given combine range query
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Haw, WheelRange, wheels::read::ExecutionPlan, aggregator::sum::U32SumAggregator};
    ///
    /// // Init a HAW with time 0
    /// let mut haw: Haw<U32SumAggregator> = Haw::default();
    /// let deltas = vec![Some(10), None, Some(50), None];
    /// haw.delta_advance(deltas);
    ///
    /// let range = WheelRange::new_unchecked(0, 4000);
    /// let exec_plan = haw.explain_combine_range(range).unwrap();
    /// assert_eq!(exec_plan, ExecutionPlan::LandmarkAggregation);
    /// ```
    #[inline]
    pub fn explain_combine_range(&self, range: impl Into<WheelRange>) -> Option<ExecutionPlan> {
        self.create_exec_plan(range.into())
    }

    /// Combines partial aggregates within the given date range and lowers it to a final aggregate
    ///
    /// Returns `None` if the range cannot be answered by the wheel
    ///
    /// See [Self::combine_range] for an example.
    #[inline]
    pub fn combine_range_and_lower(&self, range: impl Into<WheelRange>) -> Option<A::Aggregate> {
        self.combine_range(range).map(A::lower)
    }

    /// Combines partial aggregates within the given date range [start, end) into a final partial aggregate
    ///
    /// Returns `None` if the range cannot be answered by the wheel
    ///
    /// # Query Optimizer
    ///
    /// HAW use a query optimizer internally to reduce execution time using a hybrid cost and heuristics based approach.
    ///
    /// The following flow chart illustrates how the query optimizer chooses an execution plan.
    ///
    /// ![](https://raw.githubusercontent.com/uwheel/uwheel/a63799ca63b0d50a25565b150120570603b6d4cf/assets/query_optimizer_flow_chart.svg)
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Haw, WheelRange, aggregator::sum::U32SumAggregator};
    ///
    /// // Init a HAW with time 0
    /// let mut haw: Haw<U32SumAggregator> = Haw::default();
    /// let deltas = vec![Some(10), None, Some(50), None];
    /// haw.delta_advance(deltas);
    ///
    /// assert_eq!(haw.watermark(), 4000);
    /// let range = WheelRange::new_unchecked(0, 4000);
    /// assert_eq!(haw.combine_range(range), Some(60));
    /// ```
    #[inline]
    pub fn combine_range(&self, range: impl Into<WheelRange>) -> Option<A::PartialAggregate> {
        self.combine_range_inner(range).0
    }

    /// Executes a combine range query and returns the result + cost (combine ops) of executing it
    ///
    /// Returns `None` if the range cannot be answered by the wheel
    #[inline]
    pub fn analyze_combine_range(
        &self,
        range: impl Into<WheelRange>,
    ) -> (Option<A::PartialAggregate>, usize) {
        self.combine_range_inner(range)
    }

    /// Combines partial aggregates within the given date range [start, end) into a final partial aggregate
    #[inline]
    fn combine_range_inner(
        &self,
        range: impl Into<WheelRange>,
    ) -> (Option<A::PartialAggregate>, usize) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.combine_range);

        let range = range.into();
        let WheelRange { start, end } = range;

        assert!(end > start, "End date needs to be larger than start date");

        // create the best possible execution plan and run it
        match self.create_exec_plan(range) {
            Some(ExecutionPlan::WheelAggregation(wheel_agg)) => {
                (self.wheel_aggregation(wheel_agg), wheel_agg.cost())
            }
            Some(ExecutionPlan::CombinedAggregation(combined)) => {
                self.combined_aggregation(combined)
            }
            Some(ExecutionPlan::LandmarkAggregation) => self.analyze_landmark(),
            Some(ExecutionPlan::InverseLandmarkAggregation(wheel_aggs)) => {
                let (result, cost) = self.inverse_landmark_aggregation(wheel_aggs);
                (Some(result), cost)
            }
            None => (None, 0), // No execution plan possible
        }
    }
    /// Returns the best possible execution plan for a given wheel range
    ///
    /// Returns `None` if no plan can be established.
    #[inline]
    fn create_exec_plan(&self, mut range: WheelRange) -> Option<ExecutionPlan> {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.exec_plan);

        let mut best_plan: Option<ExecutionPlan> = None;

        // helper fn for maybe updating to a new optimal plan or inserting if there is none.
        let maybe_update_plan_or_insert =
            |plan: ExecutionPlan, current: &mut Option<ExecutionPlan>| {
                *current = match current.take() {
                    Some(p) => Some(cmp::min(p, plan)),
                    None => Some(plan),
                }
            };

        let wheel_start = self
            .watermark()
            .saturating_sub(self.current_time_in_cycle().whole_milliseconds() as u64);

        // SAFETY: ensure start range is not lower than the start of the wheel time
        range.start = cmp::max(range.start, Self::to_offset_date(wheel_start));

        if let Some(plan) = self.wheel_aggregation_plan(range) {
            best_plan = Some(ExecutionPlan::WheelAggregation(plan));
        }

        let end_ms = Self::to_ms(range.end.unix_timestamp() as u64);
        let start_ms = Self::to_ms(range.start.unix_timestamp() as u64);

        // Landmark optimization: landmark covers the whole range
        if start_ms <= wheel_start && end_ms >= self.watermark() {
            best_plan = Some(ExecutionPlan::LandmarkAggregation);
        }

        // Check for possible early return: Going further may just have more overhead
        if let Some(plan) = &best_plan {
            // If the plan supports single-wheel prefix scan or landmark aggregation (both O(1) complexity).
            // Or if the cost is low. For now we assume under 60 since values above are not usually quantizable to wheel intervals.
            if plan.is_prefix_or_landmark() || plan.cost() < SECONDS {
                return best_plan;
            }
        }

        // check if aggregator is invertible
        if A::invertible() {
            let mut aggregations = WheelAggregations::default();

            // always include: [wheel_start, start)
            let p1 = self.wheel_aggregation_plan(WheelRange {
                start: Self::to_offset_date(wheel_start),
                end: range.start,
            });

            // NOTE: works but refactor
            if let Some(p1_plan) = p1 {
                let mut valid = true;
                aggregations.push(p1_plan);

                // if range.end != current watermark also include [end, watermark_now]
                if end_ms < self.watermark() {
                    let p2 = self.wheel_aggregation_plan(WheelRange {
                        start: range.end,
                        end: Self::to_offset_date(self.watermark()),
                    });
                    if let Some(p2_plan) = p2 {
                        aggregations.push(p2_plan);
                    } else {
                        valid = false;
                    }
                }

                // only try to update plan if its deemed valid
                // that is, no out of bound aggregation.
                if valid {
                    maybe_update_plan_or_insert(
                        ExecutionPlan::InverseLandmarkAggregation(aggregations),
                        &mut best_plan,
                    );
                }
            }
        }

        // Check whether it is worth to create a combined plan as it comes with some overhead
        let combined_aggregation = {
            let scan_estimation = best_plan.as_ref().map(|p| p.cost()).unwrap_or(0);

            if self.conf.optimizer.use_hints && A::simd_support() {
                scan_estimation > self.conf.optimizer.heuristics.simd_threshold
            } else {
                true // always generate a combined aggregation plan
            }
        };

        // if the range can be split into multiple non-overlapping ranges
        if combined_aggregation {
            // NOTE: could create multiple combinations of combined aggregations to check
            if let Some(combined_plan) =
                self.combined_aggregation_plan(Self::split_wheel_ranges(range))
            {
                maybe_update_plan_or_insert(
                    ExecutionPlan::CombinedAggregation(combined_plan),
                    &mut best_plan,
                );
            }
        }

        best_plan
    }

    /// Given a [start, end) interval, returns the cost (number of ⊕ operations) for a given wheel aggregation
    ///
    /// Returns `None` if the wheel aggregation cannot be executed because of uninitialized wheel or out of bounds aggregation
    #[inline]
    fn wheel_aggregation_plan(&self, range: WheelRange) -> Option<WheelAggregation> {
        let start = range.start;
        let end = range.end;

        match range.lowest_granularity() {
            Granularity::Second => {
                let seconds = (end - start).whole_seconds() as usize;
                self.seconds_wheel
                    .plan(start, seconds, range, Granularity::Second)
            }
            Granularity::Minute => {
                let minutes = (end - start).whole_minutes() as usize;
                self.minutes_wheel
                    .plan(start, minutes, range, Granularity::Minute)
            }
            Granularity::Hour => {
                let hours = (end - start).whole_hours() as usize;
                self.hours_wheel
                    .plan(start, hours, range, Granularity::Hour)
            }
            Granularity::Day => {
                let days = (end - start).whole_days() as usize;
                self.days_wheel.plan(start, days, range, Granularity::Day)
            }
        }
    }

    /// Logically splits the wheel range into multiple non-overlapping ranges to execute using Combined Aggregation
    #[inline]
    #[doc(hidden)]
    pub fn split_wheel_ranges(range: WheelRange) -> WheelRanges {
        let mut ranges = WheelRanges::default();
        let WheelRange { start, end } = range;

        // initial starting point
        let mut current_start = start;

        // while we have not reached the end date, keep building wheel rangess.
        while current_start < end {
            let current_end = Self::new_curr_end(current_start, end);
            let range = WheelRange {
                start: current_start,
                end: current_end,
            };
            ranges.push(range);
            current_start = current_end;
        }
        ranges
    }

    // helper fn to calculate the next aligned end date
    #[inline]
    fn new_curr_end(current_start: OffsetDateTime, end: OffsetDateTime) -> OffsetDateTime {
        let second = current_start.second();
        let minute = current_start.minute();
        let hour = current_start.hour();
        let day = current_start.day();

        // based on the lowest granularity figure out the duration to next alignment point
        let next = if second > 0 {
            time::Duration::seconds(SECONDS as i64 - second as i64)
        } else if minute > 0 {
            time::Duration::minutes(MINUTES as i64 - minute as i64)
        } else if hour > 0 {
            time::Duration::hours(HOURS as i64 - hour as i64)
        } else if day > 0 {
            let days_to_add = if (end - current_start).whole_days() > 0 {
                let mut date = current_start.date();
                // iterate until there is no next day for the current month
                while let Some(next) = date.next_day() {
                    date = next;
                }
                date.day() as i64
            } else {
                1
            };
            time::Duration::days(days_to_add)
        } else {
            unimplemented!("Weeks and Years not supported yet");
        };
        // TODO: time::checked_add is somewhat expensive.
        // let s = current_start.unix_timestamp() * 1000 + next.whole_milliseconds() as i64;
        // let next_aligned = OffsetDateTime::from_unix_timestamp(s / 1000).unwrap();
        let next_aligned = current_start + next;

        if next_aligned > end {
            // if jumping too far: check the remaining duration between current_start and end
            let rem = end - current_start;
            let rem_secs = rem.whole_seconds();
            let rem_mins = rem.whole_minutes();
            let rem_hours = rem.whole_hours();
            let rem_days = rem.whole_days();

            let remains = [
                (rem_secs, time::Duration::seconds(rem_secs)),
                (rem_mins, time::Duration::minutes(rem_mins)),
                (rem_hours, time::Duration::hours(rem_hours)),
                (rem_days, time::Duration::days(rem_days)),
            ];

            // Jump with lowest remaining duration > 0
            let idx = remains
                .iter()
                .rposition(|&(rem, _)| rem > 0)
                .unwrap_or(remains.len());

            current_start + remains[idx].1
        } else {
            next_aligned
        }
    }

    /// Takes a set of wheel ranges and creates a CombinedAggregation plan
    #[doc(hidden)]
    #[inline]
    pub fn combined_aggregation_plan(&self, ranges: WheelRanges) -> Option<CombinedAggregation> {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.combined_aggregation_plan);

        let mut aggregations = WheelAggregations::default();

        for range in ranges.into_iter() {
            match self.wheel_aggregation_plan(range) {
                Some(plan) => aggregations.push(plan),
                None => return None, // early return if a single aggregation cannot be executed
            }
        }

        // function that returns a score of the wheel range which is used during sorting
        let granularity_score = |start: &OffsetDateTime, end: &OffsetDateTime| {
            let (sh, sm, ss) = start.time().as_hms();
            let (eh, em, es) = end.time().as_hms();
            if ss > 0 || es > 0 {
                // second rank
                0
            } else if sm > 0 || em > 0 {
                // minute rank
                1
            } else if sh > 0 || eh > 0 {
                // hour rank
                2
            } else {
                // day rank (00:00:00 - 00:00:00)
                3
            }
        };

        // Sort ranges based on lowest granularity in order to execute ranges in order of granularity
        // so that the execution visits wheels sequentially and not randomly.
        aggregations.sort_unstable_by(|a, b| {
            let a_score = granularity_score(&a.range.start, &a.range.end);
            let b_score = granularity_score(&b.range.start, &b.range.end);
            a_score.cmp(&b_score)
        });

        Some(CombinedAggregation::from(aggregations))
    }

    // Performs a inverse landmark aggregation
    fn inverse_landmark_aggregation(
        &self,
        wheel_aggregations: WheelAggregations,
    ) -> (A::PartialAggregate, usize) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.inverse_landmark);

        // get landmark partial
        let (landmark, lcost) = self.analyze_landmark();
        let combine_inverse = A::combine_inverse().unwrap(); // assumed to be safe as it has been verified by the plan generation

        wheel_aggregations.into_iter().fold(
            (landmark.unwrap_or(A::IDENTITY), lcost),
            |mut acc, plan| {
                let cost = plan.cost();
                let agg = self.wheel_aggregation(plan).unwrap_or(A::IDENTITY);
                acc.0 = combine_inverse(acc.0, agg);
                acc.1 += cost;
                acc
            },
        )
    }

    /// Performs a given Combined Aggregation and returns the partial aggregate and the cost of the operation
    #[inline]
    fn combined_aggregation(
        &self,
        combined: CombinedAggregation,
    ) -> (Option<A::PartialAggregate>, usize) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.combined_aggregation);

        let cost = combined.cost();
        let agg = combined
            .aggregations
            .into_iter()
            .fold(None, |mut acc, wheel_agg| {
                match self.wheel_aggregation(wheel_agg) {
                    Some(agg) => {
                        combine_or_insert::<A>(&mut acc, agg);
                        acc
                    }
                    None => acc,
                }
            });
        (agg, cost)
    }
    /// Combines partial aggregates within [start, end) using the lowest time granularity wheel
    ///
    /// Note that start date is inclusive while end date is exclusive.
    /// For instance, the range [16:00:50, 16:01:00) refers to the 10 remaining seconds of the minute
    ///
    /// Returns `None` if the range cannot be answered by the wheel
    fn wheel_aggregation(&self, agg: WheelAggregation) -> Option<A::PartialAggregate> {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.wheel_aggregation);

        let (start, end) = agg.slots;
        let slot_range = start..end;
        match agg.granularity {
            Granularity::Second => self.seconds_wheel.combine_range(slot_range),
            Granularity::Minute => self.minutes_wheel.combine_range(slot_range),
            Granularity::Hour => self.hours_wheel.combine_range(slot_range),
            Granularity::Day => self.days_wheel.combine_range(slot_range),
        }
    }

    /// Returns the partial aggregate in the given time interval [(watermark - `duration`), watermark)
    ///
    /// Internally the [Self::combine_range] function is used to produce the result
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Haw, NumericalDuration, aggregator::sum::U32SumAggregator};
    ///
    /// // Init a HAW with time 0
    /// let mut haw: Haw<U32SumAggregator> = Haw::default();
    /// let deltas = vec![Some(10), None, Some(50), None];
    /// haw.delta_advance(deltas);
    ///
    /// assert_eq!(haw.interval(1.seconds()), Some(0));
    /// assert_eq!(haw.interval(2.seconds()), Some(50));
    /// assert_eq!(haw.interval(3.seconds()), Some(50));
    /// assert_eq!(haw.interval(4.seconds()), Some(60));
    /// ```
    pub fn interval(&self, dur: Duration) -> Option<A::PartialAggregate> {
        self.interval_with_stats(dur).0
    }

    /// Returns the partial aggregate in the given time interval and lowers the result
    ///
    /// Internally the [Self::combine_range] function is used to produce the result
    ///
    /// See [Self::interval] for example.
    pub fn interval_and_lower(&self, dur: Duration) -> Option<A::Aggregate> {
        self.interval(dur).map(|partial| A::lower(partial))
    }

    /// Returns the partial aggregate in the given time interval
    #[inline]
    pub fn interval_with_stats(&self, dur: Duration) -> (Option<A::PartialAggregate>, usize) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.interval);

        let to = self.now();
        let from = to.saturating_sub(time::Duration::seconds(dur.whole_seconds()));

        self.analyze_combine_range(WheelRange {
            start: from,
            end: to,
        })
    }

    /// Executes a Landmark Window that combines total partial aggregates across all wheels into a full-wheel result
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Haw, NumericalDuration, aggregator::sum::U32SumAggregator};
    ///
    /// // Init a HAW with time 0
    /// let mut haw: Haw<U32SumAggregator> = Haw::default();
    /// // Advance HAW with 1800 (total 30min) deltas with value 1
    /// let deltas = (0..1800).map(Some).collect::<Vec<_>>();
    /// haw.delta_advance(deltas);
    ///
    /// // get the full result between time [0, watermark).
    /// assert_eq!(haw.landmark(), Some(1619100));
    /// ```
    #[inline]
    pub fn landmark(&self) -> Option<A::PartialAggregate> {
        self.analyze_landmark().0
    }

    /// Executes a Landmark Window that combines total partial aggregates across all wheels and lowers the result
    ///
    /// See [Self::landmark] for an example.
    #[inline]
    pub fn landmark_and_lower(&self) -> Option<A::Aggregate> {
        self.landmark().map(|partial| A::lower(partial))
    }

    /// Executes a Landmark Window that combines total partial aggregates across all wheels and returns the aggregate cost
    #[inline]
    pub(crate) fn analyze_landmark(&self) -> (Option<A::PartialAggregate>, usize) {
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

    #[inline]
    fn reduce(
        partial_aggs: impl IntoIterator<Item = Option<A::PartialAggregate>>,
    ) -> (Option<A::PartialAggregate>, usize) {
        let mut combines = 0;
        let agg = partial_aggs
            .into_iter()
            .reduce(|acc, b| match (acc, b) {
                (Some(curr), Some(agg)) => {
                    combines += 1;
                    Some(A::combine(curr, agg))
                }
                (None, Some(_)) => b,
                _ => acc,
            })
            .flatten();
        (agg, combines)
    }
    /// Schedules a timer to fire once the HAW has reached the specified time.
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Haw, NumericalDuration, aggregator::sum::U32SumAggregator};
    ///
    /// // Init a HAW with time 0
    /// let mut haw: Haw<U32SumAggregator> = Haw::default();
    /// let _ = haw.schedule_once(5000, move |haw: &Haw<_>| {
    ///    println!("{:?}", haw.interval(5.seconds()));
    /// });
    ///
    /// ```
    #[cfg(feature = "timer")]
    pub fn schedule_once(
        &self,
        time: u64,
        f: impl Fn(&Haw<A>) + 'static,
    ) -> Result<(), TimerError<TimerAction<A>>> {
        self.timer
            .write()
            .schedule_at(time, TimerAction::Oneshot(Box::new(f)))
    }
    /// Schedules a timer to fire repeatedly
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Haw, NumericalDuration, aggregator::sum::U32SumAggregator};
    ///
    /// // Init a HAW with time 0
    /// let mut haw: Haw<U32SumAggregator> = Haw::default();
    /// let _ = haw.schedule_repeat(5000, 5.seconds(), move |haw: &Haw<_>| {
    ///    println!("{:?}", haw.interval(5.seconds()));
    /// });
    ///
    /// ```
    #[cfg(feature = "timer")]
    pub fn schedule_repeat(
        &self,
        at: u64,
        interval: Duration,
        f: impl Fn(&Haw<A>) + 'static,
    ) -> Result<(), TimerError<TimerAction<A>>> {
        self.timer
            .write()
            .schedule_at(at, TimerAction::Repeat((at, interval, Box::new(f))))
    }

    /// Tick the wheel by a single unit (second)
    ///
    /// In the worst case, a tick may cause a rotation of all the wheels in the hierarchy.
    #[inline]
    fn tick(&mut self, partial_opt: Option<A::PartialAggregate>) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.tick);

        self.watermark += Self::SECOND_AS_MS;

        // if 'None', insert the Identity value
        let partial = partial_opt.unwrap_or(A::IDENTITY);
        let seconds = self.seconds_wheel.get_or_insert();

        seconds.insert_head(partial);

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

        // Fire any outgoing timers
        #[cfg(feature = "timer")]
        {
            let mut timer = self.timer.write();

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

    /// Returns a reference to the seconds wheel
    pub fn seconds(&self) -> Option<&Wheel<A>> {
        self.seconds_wheel.as_ref()
    }
    /// Returns an unchecked reference to the seconds wheel
    pub fn seconds_unchecked(&self) -> &Wheel<A> {
        self.seconds_wheel.as_ref().unwrap()
    }

    /// Returns a reference to the minutes wheel
    pub fn minutes(&self) -> Option<&Wheel<A>> {
        self.minutes_wheel.as_ref()
    }
    /// Returns an unchecked reference to the minutes wheel
    pub fn minutes_unchecked(&self) -> &Wheel<A> {
        self.minutes_wheel.as_ref().unwrap()
    }
    /// Returns a reference to the hours wheel
    pub fn hours(&self) -> Option<&Wheel<A>> {
        self.hours_wheel.as_ref()
    }
    /// Returns an unchecked reference to the hours wheel
    pub fn hours_unchecked(&self) -> &Wheel<A> {
        self.hours_wheel.as_ref().unwrap()
    }
    /// Returns a reference to the days wheel
    pub fn days(&self) -> Option<&Wheel<A>> {
        self.days_wheel.as_ref()
    }
    /// Returns an unchecked reference to the days wheel
    pub fn days_unchecked(&self) -> &Wheel<A> {
        self.days_wheel.as_ref().unwrap()
    }

    /// Returns a reference to the weeks wheel
    pub fn weeks(&self) -> Option<&Wheel<A>> {
        self.weeks_wheel.as_ref()
    }

    /// Returns an unchecked reference to the weeks wheel
    pub fn weeks_unchecked(&self) -> &Wheel<A> {
        self.weeks_wheel.as_ref().unwrap()
    }

    /// Returns a reference to the years wheel
    pub fn years(&self) -> Option<&Wheel<A>> {
        self.years_wheel.as_ref()
    }
    /// Returns a reference to the years wheel
    pub fn years_unchecked(&self) -> &Wheel<A> {
        self.years_wheel.as_ref().unwrap()
    }

    /// Merges two wheels
    ///
    /// Note that the time in `other` may be advanced and thus change state
    ///
    /// # Panics
    ///
    /// The function currently panics if another data layout than Array is used.
    pub(crate) fn merge(&mut self, other: &mut Self) {
        let other_watermark = other.watermark();

        // make sure both wheels are aligned by time
        if self.watermark() > other_watermark {
            other.advance_to(self.watermark(), &mut WriterWheel::default());
        } else {
            self.advance_to(other_watermark, &mut WriterWheel::default());
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
    /// Returns a reference to the stats of the [Haw]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        aggregator::sum::U64SumAggregator,
        duration::NumericalDuration,
        wheels::read::plan::Aggregation,
    };
    use time::macros::datetime;

    use super::*;

    #[test]
    fn delta_advance_test() {
        let mut haw: Haw<U64SumAggregator> = Haw::default();
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
    fn wheel_range_test() {
        let start = datetime!(2023 - 11 - 09 00:00:00 UTC);
        let end = datetime!(2023 - 11 - 12 00:00:00 UTC);
        assert!(
            WheelRange::new(start.unix_timestamp() as u64, end.unix_timestamp() as u64).is_ok()
        );
    }
    #[test]
    fn out_of_bounds_aggregation() {
        // 2023-11-09 00:00:00
        let watermark = 1699488000000;
        let conf = HawConf::default().with_watermark(watermark);

        let mut haw: Haw<U64SumAggregator> = Haw::new(conf);

        let days_as_secs = 3600 * 24;
        let days = days_as_secs * 3;
        let deltas: Vec<Option<u64>> = (0..days).map(|_| Some(1)).collect();
        // advance wheel by 3 days
        haw.delta_advance(deltas);

        let start = datetime!(2023 - 11 - 09 15:50:50 UTC);
        let end = datetime!(2023 - 11 - 11 12:30:45 UTC);
        // as we do not retain slots for any granularity this call must produce `None`
        assert_eq!(haw.combine_range(WheelRange { start, end }), None);
    }

    #[test]
    fn range_query_sec_test() {
        // 2023-11-09 00:00:00
        let watermark = 1699488000000;
        let conf = HawConf::default().with_watermark(watermark);
        let mut haw: Haw<U64SumAggregator> = Haw::new(conf);
        // oldest to newest
        let deltas = vec![Some(10), None, Some(50), None];

        haw.delta_advance(deltas);

        assert_eq!(haw.interval(1.seconds()), Some(0));
        assert_eq!(haw.interval(2.seconds()), Some(50));
        assert_eq!(haw.interval(3.seconds()), Some(50));
        assert_eq!(haw.interval(4.seconds()), Some(60));

        let start = datetime!(2023-11-09 00:00:00 UTC);
        let end = datetime!(2023-11-09 00:00:02 UTC);

        let agg = haw.combine_range(WheelRange { start, end });
        assert_eq!(agg, Some(10));
        assert_eq!(
            haw.range(WheelRange { start, end }),
            Some(vec![(1699488000000, 10), (1699488001000, 0)])
        );

        let start = datetime!(2023-11-09 00:00:00 UTC);
        let end = datetime!(2023-11-09 00:00:04 UTC);
        let agg = haw.combine_range(WheelRange { start, end });
        assert_eq!(agg, Some(60));

        assert_eq!(
            haw.range(WheelRange { start, end }),
            Some(vec![
                (1699488000000, 10),
                (1699488001000, 0),
                (1699488002000, 50),
                (1699488003000, 0),
            ])
        );
    }

    #[test]
    fn range_query_min_test() {
        // 2023-11-09 00:00:00
        let watermark = 1699488000000;
        let conf = HawConf::default().with_watermark(watermark);
        let mut haw: Haw<U64SumAggregator> = Haw::new(conf);

        let deltas: Vec<Option<u64>> = (0..180).map(|_| Some(1)).collect();
        haw.delta_advance(deltas);
        assert_eq!(haw.watermark(), 1699488180000);

        // time is 2023-11-09 00:03:00
        // queryable minutes are 00:00 - 00:03
        let start = datetime!(2023-11-09 00:00 UTC);
        let end = datetime!(2023-11-09 00:03 UTC);

        let agg = haw.combine_range(WheelRange { start, end });
        // 1 for each second rolled-up over 3 minutes > 180
        assert_eq!(agg, Some(180));
    }

    #[test]
    fn range_query_hour_test() {
        // 2023-11-09 00:00:00
        let watermark = 1699488000000;
        let conf = HawConf::default().with_watermark(watermark);
        let mut haw: Haw<U64SumAggregator> = Haw::new(conf);

        let hours_as_secs = 3600;
        let hours = hours_as_secs * 3;
        let deltas: Vec<Option<u64>> = (0..hours).map(|_| Some(1)).collect();
        // advance wheel by 3 hours
        haw.delta_advance(deltas);

        let start = datetime!(2023-11-09 00:00 UTC);
        let end = datetime!(2023-11-09 01:00 UTC);

        assert_eq!(haw.combine_range(WheelRange { start, end }), Some(3600));
        assert_eq!(
            haw.range(WheelRange { start, end }),
            Some(vec![(1699488000000, 3600)])
        );

        let start = datetime!(2023-11-09 00:00 UTC);
        let end = datetime!(2023-11-09 03:00 UTC);

        assert_eq!(haw.combine_range(WheelRange { start, end }), Some(10800));
        assert_eq!(
            haw.range(WheelRange { start, end }),
            Some(vec![
                (1699488000000, 3600),
                (1699491600000, 3600),
                (1699495200000, 3600),
            ])
        );
    }

    #[test]
    fn range_query_day_test() {
        // 2023-11-09 00:00:00
        let watermark = 1699488000000;
        let conf = HawConf::default()
            .with_watermark(watermark)
            .with_retention_policy(RetentionPolicy::Keep);

        let mut haw: Haw<U64SumAggregator> = Haw::new(conf);

        let days_as_secs = 3600 * 24;
        let days = days_as_secs * 3;
        let deltas: Vec<Option<u64>> = (0..days).map(|_| Some(1)).collect();
        // advance wheel by 3 days
        haw.delta_advance(deltas);

        let start = datetime!(2023 - 11 - 09 00:00:00 UTC);
        let end = datetime!(2023 - 11 - 10 00:00:00 UTC);

        assert_eq!(
            haw.combine_range(WheelRange { start, end }),
            Some(3600 * 24)
        );

        let start = datetime!(2023 - 11 - 09 00:00:00 UTC);
        let end = datetime!(2023 - 11 - 12 00:00:00 UTC);

        assert_eq!(haw.combine_range(WheelRange { start, end }), Some(259200));

        let start = datetime!(2023 - 11 - 09 15:50:50 UTC);
        let end = datetime!(2023 - 11 - 11 12:30:45 UTC);

        // verify that both wheel aggregation using seconds and combine range using multiple wheel aggregations end up the same
        // assert_eq!(haw.wheel_aggregation((start, end)), Some(160795));
        assert_eq!(haw.combine_range(WheelRange { start, end }), Some(160795));

        let plan = haw
            .explain_combine_range(WheelRange { start, end })
            .unwrap();
        dbg!(&plan);
        let combined_plan = match plan {
            ExecutionPlan::CombinedAggregation(plan) => plan,
            _ => panic!("not supposed to happen"),
        };

        let mut expected = WheelAggregations::default();

        expected.push(WheelAggregation {
            range: WheelRange {
                start: datetime!(2023 - 11 - 09 15:50:50 UTC),
                end: datetime!(2023 - 11 - 09 15:51:00 UTC),
            },
            plan: Aggregation::Scan(10),
            slots: (202140, 202150),
            granularity: Granularity::Second,
        });

        expected.push(WheelAggregation {
            range: WheelRange {
                start: datetime!(2023 - 11 - 11 12:30:00 UTC),
                end: datetime!(2023 - 11 - 11 12:30:45 UTC),
            },
            plan: Aggregation::Scan(45),
            slots: (41355, 41400),
            granularity: Granularity::Second,
        });

        expected.push(WheelAggregation {
            range: WheelRange {
                start: datetime!(2023 - 11 - 09 15:51:00 UTC),
                end: datetime!(2023 - 11 - 09 16:00:00 UTC),
            },
            plan: Aggregation::Scan(9),
            slots: (3360, 3369),
            granularity: Granularity::Minute,
        });

        expected.push(WheelAggregation {
            range: WheelRange {
                start: datetime!(2023 - 11 - 11 12:00:00 UTC),
                end: datetime!(2023 - 11 - 11 12:30:00 UTC),
            },
            plan: Aggregation::Scan(30),
            slots: (690, 720),
            granularity: Granularity::Minute,
        });

        expected.push(WheelAggregation {
            range: WheelRange {
                start: datetime!(2023 - 11 - 09 16:00:00 UTC),
                end: datetime!(2023 - 11 - 10 00:00:00 UTC),
            },
            plan: Aggregation::Scan(8),
            slots: (48, 56),
            granularity: Granularity::Hour,
        });
        expected.push(WheelAggregation {
            range: WheelRange {
                start: datetime!(2023 - 11 - 11 00:00:00 UTC),
                end: datetime!(2023 - 11 - 11 12:00:00 UTC),
            },
            plan: Aggregation::Scan(12),
            slots: (12, 24),
            granularity: Granularity::Hour,
        });
        expected.push(WheelAggregation {
            range: WheelRange {
                start: datetime!(2023 - 11 - 10 00:00:00 UTC),
                end: datetime!(2023 - 11 - 11 00:00:00 UTC),
            },
            plan: Aggregation::Scan(1),
            slots: (1, 2),
            granularity: Granularity::Day,
        });

        assert_eq!(combined_plan.aggregations, expected);

        let start = datetime!(2023 - 11 - 09 05:00:00 UTC);
        let end = datetime!(2023 - 11 - 12 00:00:00 UTC);

        // Runs a Inverse Landmark Execution
        let result = haw.combine_range(WheelRange { start, end });
        let plan = haw.create_exec_plan(WheelRange { start, end });
        assert!(matches!(
            plan,
            Some(ExecutionPlan::InverseLandmarkAggregation(_))
        ));
        assert_eq!(result, Some(241200));
    }

    #[test]
    fn month_cross_test() {
        // 2018-08-31 00:00:00
        let watermark = 1535673600000;

        let conf = HawConf::default()
            .with_watermark(watermark)
            .with_retention_policy(RetentionPolicy::Keep);
        let mut haw: Haw<U64SumAggregator> = Haw::new(conf);

        let days_as_secs = 3600 * 24;
        let days = days_as_secs * 3;
        let deltas: Vec<Option<u64>> = (0..days).map(|_| Some(1)).collect();
        // advance wheel by 3 days
        haw.delta_advance(deltas);

        let start = datetime!(2018 - 08 - 31 00:00:00 UTC);
        let end = datetime!(2018 - 08 - 31 00:05:00 UTC);

        assert_eq!(haw.combine_range(WheelRange { start, end }), Some(300));

        let start = datetime!(2018 - 08 - 31 00:00:00 UTC);
        let end = datetime!(2018 - 09 - 01 12:00:00 UTC);

        assert_eq!(haw.combine_range(WheelRange { start, end }), Some(129600));

        let start = datetime!(2018 - 08 - 31 00:00:00 UTC);
        let end = datetime!(2018 - 09 - 02 00:00:00 UTC);

        assert_eq!(haw.combine_range(WheelRange { start, end }), Some(172800));
    }

    #[test]
    fn overlapping_ranges_test() {
        let watermark = 1699488000000; // 2023-11-09 00:00:00
        let conf = HawConf::default().with_watermark(watermark);
        let mut haw: Haw<U64SumAggregator> = Haw::new(conf);

        let deltas = vec![Some(10), Some(20), Some(30), Some(40)];
        haw.delta_advance(deltas);

        // Test overlapping ranges
        let range1 = WheelRange {
            start: datetime!(2023-11-09 00:00:00 UTC),
            end: datetime!(2023-11-09 00:00:02 UTC),
        };
        let range2 = WheelRange {
            start: datetime!(2023-11-09 00:00:01 UTC),
            end: datetime!(2023-11-09 00:00:03 UTC),
        };

        let range3 = WheelRange {
            start: datetime!(2023-11-09 00:00:02 UTC),
            end: datetime!(2023-11-09 00:00:04 UTC),
        };

        assert_eq!(haw.combine_range(range1), Some(30));
        assert_eq!(haw.combine_range(range2), Some(50));
        assert_eq!(haw.combine_range(range3), Some(70));
    }

    #[test]
    fn sparse_data_test() {
        let watermark = 1699488000000; // 2023-11-09 00:00:00
        let conf = HawConf::default().with_watermark(watermark);
        let mut haw: Haw<U64SumAggregator> = Haw::new(conf);

        // Add sparse data: values at 0s, 5s, and 10s
        let deltas = vec![
            Some(10),
            None,
            None,
            None,
            None,
            Some(20),
            None,
            None,
            None,
            None,
            Some(30),
        ];
        haw.delta_advance(deltas);

        let start = datetime!(2023-11-09 00:00:00 UTC);
        let end = datetime!(2023-11-09 00:00:11 UTC);
        assert_eq!(haw.combine_range(WheelRange { start, end }), Some(60));
        assert_eq!(
            haw.range(WheelRange { start, end }),
            Some(vec![
                (1699488000000, 10),
                (1699488001000, 0),
                (1699488002000, 0),
                (1699488003000, 0),
                (1699488004000, 0),
                (1699488005000, 20),
                (1699488006000, 0),
                (1699488007000, 0),
                (1699488008000, 0),
                (1699488009000, 0),
                (1699488010000, 30),
            ])
        );
    }

    #[test]
    fn empty_haw_test() {
        let watermark = 1699488000000; // 2023-11-09 00:00:00
        let conf = HawConf::default().with_watermark(watermark);
        let haw: Haw<U64SumAggregator> = Haw::new(conf);

        assert_eq!(haw.interval(1.seconds()), None);

        let start = datetime!(2023-11-09 00:00:00 UTC);
        let end = datetime!(2023-11-09 00:00:02 UTC);
        assert_eq!(haw.combine_range(WheelRange { start, end }), None);
        assert_eq!(haw.range(WheelRange { start, end }), None);
    }

    #[test]
    fn out_of_bounds_query_test() {
        let watermark = 1699488000000; // 2023-11-09 00:00:00
        let conf = HawConf::default().with_watermark(watermark);
        let mut haw: Haw<U64SumAggregator> = Haw::new(conf);

        let deltas = vec![Some(10), Some(20), Some(30), Some(40)];
        haw.delta_advance(deltas);

        // Query before the initial watermark + few seconds ahead
        let before_and_in_range = WheelRange {
            start: datetime!(2023-11-08 23:59:58 UTC),
            end: datetime!(2023-11-09 00:00:01 UTC),
        };
        assert_eq!(haw.combine_range(before_and_in_range), Some(10));

        // Query range ahead of the watemark
        let outside_range = WheelRange {
            start: datetime!(2023-11-09 00:00:05 UTC),
            end: datetime!(2023-11-09 00:00:08 UTC),
        };
        assert_eq!(haw.combine_range(outside_range), None);
    }
}
