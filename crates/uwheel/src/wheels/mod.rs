/// Reader Wheel
///
/// Single reader or multi-reader with the ``sync`` feature enabled.
pub mod read;
/// Extension trait for implementing a custom wheel
pub mod wheel_ext;
/// Writer Wheel
///
/// Optimised for a single writer
pub mod write;

#[cfg(feature = "profiler")]
mod stats;

/// Hierarchical Wheel Timer
#[allow(dead_code)]
mod timer;

use crate::{aggregator::Aggregator, duration::Duration, window::WindowAggregate, Entry};
use core::fmt::Debug;
use write::DEFAULT_WRITE_AHEAD_SLOTS;

pub use read::{DAYS, HOURS, MINUTES, SECONDS, WEEKS, YEARS};
pub use wheel_ext::WheelExt;
pub use write::WriterWheel;

use self::read::{hierarchical::HawConf, ReaderWheel};

use crate::window::Window;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "profiler")]
use uwheel_stats::profile_scope;

/// A Reader-Writer aggregation wheel with decoupled read and write paths.
///
/// # How it works
/// The design of µWheel is centered around [low-watermarking](http://www.vldb.org/pvldb/vol14/p3135-begoli.pdf), a concept found in modern streaming systems (e.g., Apache Flink, RisingWave, Arroyo).
/// Wheels in µWheel are low-watermark indexed which enables fast writes and lookups. Additionally, it lets us avoid storing explicit timestamps since they are implicit in the wheels.
///
/// A low watermark `w` indicates that all records with timestamps `t` where `t <= w` have been ingested.
/// µWheel exploits this property and seperates the write and read paths. Writes (timestamps above `w`) are handled by a Writer Wheel which is optimized for single-threaded ingestion of out-of-order aggregates.
/// Reads (queries with `time < w`) are managed by a hierarchically indexed Reader Wheel that employs a wheel-based query optimizer whose cost function
/// minimizes the number of aggregate operations for a query.
///
/// µWheel adopts a Lazy Synchronization aggregation approach. Aggregates are only shifted over from the `WriterWheel` to the `ReaderWheel`
/// once the internal low watermark has been advanced.
///
/// ![](https://raw.githubusercontent.com/uwheel/uwheel/a63799ca63b0d50a25565b150120570603b6d4cf/assets/overview.svg)
///
/// ## Queries
///
/// [RwWheel] supports both streaming window aggregation queries and temporal adhoc queries over arbitrary time ranges.
/// A window may be installed through [RwWheel::window] and queries can be executed through its ReaderWheel which is accessible through [RwWheel::read].
///
/// ## Example
///
/// Here is an example showing the use of a U32 SUM aggregator
///
/// ```
/// use uwheel::{aggregator::sum::U32SumAggregator, NumericalDuration, Entry, RwWheel};
///
/// let time = 0;
/// // initialize the wheel with a low watermark
/// let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(time);
/// // Insert an entry into the wheel
/// wheel.insert(Entry::new(100, time));
/// // advance the wheel to 1000
/// wheel.advance_to(1000);
/// // verify the new low watermark
/// assert_eq!(wheel.watermark(), 1000);
/// // query the last second of aggregates
/// assert_eq!(wheel.read().interval(1.seconds()), Some(100));
/// ```
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
pub struct RwWheel<A>
where
    A: Aggregator,
{
    /// A single-writer wheel designed for high-throughput ingestion of stream aggregates
    writer: WriterWheel<A>,
    /// A multiple-reader wheel designed for efficient querying of aggregate across arbitrary time ranges
    reader: ReaderWheel<A>,
    #[cfg(feature = "profiler")]
    stats: stats::Stats,
}

impl<A: Aggregator> Default for RwWheel<A> {
    fn default() -> Self {
        Self::with_conf(Default::default())
    }
}

impl<A> RwWheel<A>
where
    A: Aggregator,
{
    /// Creates a new Wheel starting from the given time
    ///
    /// Time is represented as milliseconds since unix timestamp
    ///
    /// See [`RwWheel::with_conf`] for more detailed configuration possibilities
    pub fn new(time: u64) -> Self {
        let conf = Conf::default().with_haw_conf(HawConf::default().with_watermark(time));
        Self::with_conf(conf)
    }
    /// Creates a new wheel using the specified configuration
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{RwWheel, Conf, aggregator::sum::U32SumAggregator, HawConf};
    /// let conf = Conf::default().with_haw_conf(HawConf::default().with_watermark(10000));
    /// let wheel: RwWheel<U32SumAggregator> = RwWheel::with_conf(conf);
    /// ```
    pub fn with_conf(conf: Conf) -> Self {
        Self {
            writer: WriterWheel::with_capacity_and_watermark(
                conf.writer_conf.write_ahead_capacity,
                conf.reader_conf.haw_conf.watermark,
            ),
            reader: ReaderWheel::with_conf(conf.reader_conf.haw_conf),
            #[cfg(feature = "profiler")]
            stats: stats::Stats::default(),
        }
    }
    /// Installs a periodic window aggregation query
    ///
    /// Results of the window are returned when advancing the wheel with [Self::advance_to] or [Self::advance].
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Window, aggregator::sum::U32SumAggregator, RwWheel, NumericalDuration};
    ///
    /// // Define a window query
    /// let window = Window::sliding(10.seconds(), 3.seconds());
    /// // Initialize a Reader-Writer Wheel and install the window
    /// let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
    /// wheel.window(window);
    /// ```
    pub fn window(&mut self, window: impl Into<Window>) {
        self.reader.window(window.into());
    }

    /// Inserts an entry into the wheel
    ///
    /// # Safety
    ///
    /// Entries with timestamps below the current low watermark ([Self::watermark]) are dropped.
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{aggregator::sum::U32SumAggregator, RwWheel, Entry};
    ///
    /// let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
    /// let data = 10;
    /// let timestamp = 1000;
    /// wheel.insert(Entry::new(data, timestamp));
    /// ```
    #[inline]
    pub fn insert(&mut self, e: impl Into<Entry<A::Input>>) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.insert);

        self.writer.insert(e);
    }

    /// Returns a reference to the writer wheel
    pub fn write(&self) -> &WriterWheel<A> {
        &self.writer
    }
    /// Returns a reference to the underlying reader wheel
    pub fn read(&self) -> &ReaderWheel<A> {
        &self.reader
    }
    /// Merges another read wheel with same size into this one
    pub fn merge_read_wheel(&self, other: &ReaderWheel<A>) {
        self.read().merge(other);
    }
    /// Returns the current watermark of this wheel
    pub fn watermark(&self) -> u64 {
        self.writer.watermark()
    }
    /// Advance the watermark of the wheel by the given [Duration]
    ///
    /// May return possible window aggregates if any window is installed (see [RwWheel::window]).
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{aggregator::sum::U32SumAggregator, RwWheel, NumericalDuration};
    ///
    /// let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
    /// wheel.advance(5.seconds());
    /// assert_eq!(wheel.watermark(), 5000);
    /// ```
    #[inline]
    pub fn advance(&mut self, duration: Duration) -> Vec<WindowAggregate<A::PartialAggregate>> {
        let to = self.watermark() + duration.whole_milliseconds() as u64;
        self.advance_to(to)
    }

    /// Advances the time of the wheel to the specified watermark.
    ///
    /// May return possible window aggregates if any window is installed (see [RwWheel::window]).
    ///
    /// # Safety
    ///
    /// This function assumes advancement to occur in atomic units (e.g., 5 not 4.5 seconds)
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{aggregator::sum::U32SumAggregator, RwWheel, NumericalDuration};
    ///
    /// let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
    /// wheel.advance_to(5000);
    /// assert_eq!(wheel.watermark(), 5000);
    /// ```
    #[inline]
    pub fn advance_to(&mut self, watermark: u64) -> Vec<WindowAggregate<A::PartialAggregate>> {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.advance);

        self.reader.advance_to(watermark, &mut self.writer)
    }

    /// Returns an estimation of bytes used by the wheel
    pub fn size_bytes(&self) -> usize {
        let read = self.reader.as_ref().size_bytes();
        let write = self.writer.size_bytes().unwrap();
        read + write
    }

    #[cfg(feature = "profiler")]
    /// Prints the stats of the [RwWheel]
    pub fn print_stats(&self) {
        use prettytable::{row, Table};
        use uwheel_stats::Sketch;
        let mut table = Table::new();
        table.add_row(row![
            "name", "count", "min", "p50", "p99", "p99.9", "p99.99", "p99.999", "max",
        ]);
        // helper fn to format percentile
        let percentile_fmt = |p: f64| -> String { format!("{:.2}ns", p) };

        // helper fn to add row to the table
        let add_row = |id: &str, table: &mut Table, sketch: &Sketch| {
            let percentiles = sketch.percentiles();
            table.add_row(row![
                id,
                percentiles.count,
                percentiles.min,
                percentile_fmt(percentiles.p50),
                percentile_fmt(percentiles.p99),
                percentile_fmt(percentiles.p99_9),
                percentile_fmt(percentiles.p99_99),
                percentile_fmt(percentiles.p99_999),
                percentiles.min,
            ]);
        };

        add_row("insert", &mut table, &self.stats.insert);
        add_row("advance", &mut table, &self.stats.advance);

        let read = self.reader.as_ref();
        add_row("tick", &mut table, &read.stats().tick);
        add_row("interval", &mut table, &read.stats().interval);
        add_row("landmark", &mut table, &read.stats().landmark);
        add_row("combine range", &mut table, &read.stats().combine_range);
        add_row(
            "combine range plan",
            &mut table,
            &read.stats().combine_range_plan,
        );
        add_row(
            "combined aggregation",
            &mut table,
            &read.stats().combined_aggregation,
        );

        add_row("execution plan", &mut table, &read.stats().exec_plan);

        add_row(
            "combined aggregation plan",
            &mut table,
            &read.stats().combined_aggregation_plan,
        );
        add_row(
            "wheel aggregation",
            &mut table,
            &read.stats().wheel_aggregation,
        );

        println!("====RwWheel Profiler Dump====");
        table.printstd();
    }
}

impl<A> Drop for RwWheel<A>
where
    A: Aggregator,
{
    fn drop(&mut self) {
        #[cfg(feature = "profiler")]
        self.print_stats();
    }
}

/// [`WriterWheel`] Configuration
#[derive(Debug, Copy, Clone)]
pub struct WriterConf {
    /// Defines the capacity of write-ahead slots
    write_ahead_capacity: usize,
}
impl Default for WriterConf {
    fn default() -> Self {
        Self {
            write_ahead_capacity: DEFAULT_WRITE_AHEAD_SLOTS,
        }
    }
}

/// [`ReaderWheel`] Configuration
#[derive(Debug, Default, Copy, Clone)]
pub struct ReaderConf {
    /// Hierarchical Aggregation Wheel configuration
    haw_conf: HawConf,
}

/// Reader-Writer Wheel Configuration
#[derive(Debug, Default, Copy, Clone)]
pub struct Conf {
    /// Writer Wheel Configuration
    writer_conf: WriterConf,
    /// Reader Wheel Configuration
    reader_conf: ReaderConf,
}

impl Conf {
    /// Configure the number of write-ahead slots
    ///
    /// The default value is [DEFAULT_WRITE_AHEAD_SLOTS]
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::Conf;
    ///
    /// // Configure a write-ahead capacity of 128 (seconds)
    /// let rw_conf = Conf::default().with_write_ahead(128);
    /// ```
    pub fn with_write_ahead(mut self, capacity: usize) -> Self {
        self.writer_conf.write_ahead_capacity = capacity;
        self
    }
    /// Configures the reader wheel to use the given [HawConf]
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{HawConf, Conf, RetentionPolicy};
    ///
    /// // Configure all wheels in the HAW to maintain data
    /// let haw_conf = HawConf::default().with_retention_policy(RetentionPolicy::Keep);
    /// let rw_conf = Conf::default().with_haw_conf(haw_conf);
    /// ```
    pub fn with_haw_conf(mut self, conf: HawConf) -> Self {
        self.reader_conf.haw_conf = conf;
        self
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "timer")]
    use core::cell::RefCell;
    #[cfg(feature = "timer")]
    use std::rc::Rc;

    use super::*;
    use crate::{aggregator::sum::U32SumAggregator, duration::*, *};

    #[test]
    fn delta_generate_test() {
        let haw_conf = HawConf::default().with_deltas();
        let conf = Conf::default().with_haw_conf(haw_conf);

        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::with_conf(conf);
        rw_wheel.insert(Entry::new(250, 1000));
        rw_wheel.insert(Entry::new(250, 2000));
        rw_wheel.insert(Entry::new(250, 3000));
        rw_wheel.insert(Entry::new(250, 4000));

        rw_wheel.advance_to(5000);
        let delta_state = rw_wheel.read().delta_state();

        assert_eq!(delta_state.oldest_ts, 0);
        assert_eq!(
            delta_state.deltas,
            vec![None, Some(250), Some(250), Some(250), Some(250)]
        );

        assert_eq!(rw_wheel.read().interval(4.seconds()), Some(1000));

        // create a new read wheel from deltas
        let read: ReaderWheel<U32SumAggregator> = ReaderWheel::from_delta_state(delta_state);
        // verify the watermark
        assert_eq!(read.watermark(), 5000);
        // verify the the results
        assert_eq!(read.interval(4.seconds()), Some(1000));
    }

    #[test]
    fn insert_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::default();
        rw_wheel.insert(Entry::new(250, 1000));
        rw_wheel.insert(Entry::new(250, 2000));
        rw_wheel.insert(Entry::new(250, 3000));
        rw_wheel.insert(Entry::new(250, 4000));

        // should be inserted into the timer wheel
        rw_wheel.insert(Entry::new(250, 150000));

        rw_wheel.advance_to(5000);

        // should trigger insert
        rw_wheel.advance_to(86000);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn rw_wheel_serde_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
        rw_wheel.insert(Entry::new(250, 1000));
        rw_wheel.insert(Entry::new(250, 2000));
        rw_wheel.insert(Entry::new(250, 3000));
        rw_wheel.insert(Entry::new(250, 4000));

        rw_wheel.insert(Entry::new(250, 100000)); // insert entry that will go into overflow wheel

        let serialized = bincode::serialize(&rw_wheel).unwrap();

        let mut deserialized_wheel =
            bincode::deserialize::<RwWheel<U32SumAggregator>>(&serialized).unwrap();

        assert_eq!(deserialized_wheel.watermark(), 0);
        deserialized_wheel.advance_to(5000);
        assert_eq!(deserialized_wheel.read().interval(4.seconds()), Some(1000));

        // advance passed the overflow wheel entry and make sure its still there
        deserialized_wheel.advance_to(101000);
        assert_eq!(deserialized_wheel.read().interval(1.seconds()), Some(250));
    }

    #[cfg(feature = "timer")]
    #[test]
    fn timer_once_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::default();
        let gate = Rc::new(RefCell::new(false));
        let inner_gate = gate.clone();

        let _ = rw_wheel.read().schedule_once(5000, move |read| {
            if let Some(last_five) = read.interval(5.seconds()) {
                *inner_gate.borrow_mut() = true;
                assert_eq!(last_five, 1000);
            }
        });
        rw_wheel.insert(Entry::new(250, 1000));
        rw_wheel.insert(Entry::new(250, 2000));
        rw_wheel.insert(Entry::new(250, 3000));
        rw_wheel.insert(Entry::new(250, 4000));

        rw_wheel.advance_to(5000);

        // assert that the timer action was triggered
        assert!(*gate.borrow());
    }

    #[cfg(feature = "timer")]
    #[test]
    fn timer_repeat_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::default();
        let sum = Rc::new(RefCell::new(0));
        let inner_sum = sum.clone();

        // schedule a repeat action
        let _ = rw_wheel
            .read()
            .schedule_repeat(5000, 5.seconds(), move |read| {
                if let Some(last_five) = read.interval(5.seconds()) {
                    *inner_sum.borrow_mut() += last_five;
                }
            });

        rw_wheel.insert(Entry::new(250, 1000));
        rw_wheel.insert(Entry::new(250, 2000));
        rw_wheel.insert(Entry::new(250, 3000));
        rw_wheel.insert(Entry::new(250, 4000));

        // trigger first timer to add sum of last 5 seconds
        rw_wheel.advance_to(5000);
        assert_eq!(*sum.borrow(), 1000);

        rw_wheel.insert(Entry::new(250, 5000));
        rw_wheel.insert(Entry::new(250, 6000));
        rw_wheel.insert(Entry::new(250, 7000));

        // trigger second timer to add sum of last 5 seconds
        rw_wheel.advance_to(10000);
        assert_eq!(*sum.borrow(), 1750);
    }

    #[cfg(feature = "sync")]
    #[test]
    fn read_wheel_move_thread_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::default();
        rw_wheel.insert(Entry::new(1, 999));
        rw_wheel.advance(1.seconds());

        let read = rw_wheel.read().clone();

        let handle = std::thread::spawn(move || {
            assert_eq!(read.interval(1.seconds()), Some(1));
        });

        handle.join().expect("Failed to join the thread.");
    }

    #[test]
    fn interval_test() {
        let mut time = 0;
        let mut wheel = RwWheel::<U32SumAggregator>::new(time);
        wheel.advance(1.seconds());

        wheel.insert(Entry::new(1u32, 1000));
        wheel.insert(Entry::new(5u32, 5000));
        wheel.insert(Entry::new(11u32, 11000));

        wheel.advance(5.seconds());
        assert_eq!(wheel.watermark(), 6000);

        // let expected: &[_] = &[&0, &1u32, &0, &0, &0, &5];
        // assert_eq!(
        //     &wheel
        //         .read()
        //         .as_ref()
        //         .seconds_unchecked()
        //         .iter()
        //         .collect::<Vec<&u32>>(),
        //     expected
        // );

        assert_eq!(
            wheel.read().as_ref().seconds_unchecked().interval(5).0,
            Some(6u32)
        );
        assert_eq!(
            wheel.read().as_ref().seconds_unchecked().interval(1).0,
            Some(5u32)
        );

        time = 12000;
        wheel.advance_to(time);

        wheel.insert(Entry::new(100u32, 61000));
        wheel.insert(Entry::new(100u32, 63000));
        wheel.insert(Entry::new(100u32, 67000));

        // go pass seconds wheel
        time = 65000;
        wheel.advance_to(time);
    }

    #[test]
    fn mixed_timestamp_insertions_test() {
        let mut time = 1000;
        let mut wheel = RwWheel::<U32SumAggregator>::new(time);
        wheel.advance_to(time);

        wheel.insert(Entry::new(1u32, 1000));
        wheel.insert(Entry::new(5u32, 5000));
        wheel.insert(Entry::new(11u32, 11000));

        time = 6000; // new watermark
        wheel.advance_to(time);

        assert_eq!(
            wheel.read().as_ref().seconds_unchecked().total(),
            Some(6u32)
        );
        // check we get the same result by combining the range of last 6 seconds
        assert_eq!(
            wheel
                .read()
                .as_ref()
                .seconds_unchecked()
                .combine_range_and_lower(0..5),
            Some(6u32)
        );
    }

    // #[test]
    // fn full_cycle_test() {
    //     let mut wheel = RwWheel::<U32SumAggregator>::new(0);

    //     let ticks = wheel.read().remaining_ticks() - 1;
    //     wheel.advance(Duration::seconds(ticks as i64));

    //     // one tick away from full cycle clear
    //     assert_eq!(
    //         wheel.read().as_ref().seconds_unchecked().rotation_count(),
    //         SECONDS - 1
    //     );
    //     assert_eq!(
    //         wheel.read().as_ref().minutes_unchecked().rotation_count(),
    //         MINUTES - 1
    //     );
    //     assert_eq!(
    //         wheel.read().as_ref().hours_unchecked().rotation_count(),
    //         HOURS - 1
    //     );
    //     assert_eq!(
    //         wheel.read().as_ref().days_unchecked().rotation_count(),
    //         DAYS - 1
    //     );
    //     assert_eq!(
    //         wheel.read().as_ref().weeks_unchecked().rotation_count(),
    //         WEEKS - 1
    //     );
    //     assert_eq!(
    //         wheel.read().as_ref().years_unchecked().rotation_count(),
    //         YEARS - 1
    //     );

    //     // force full cycle clear
    //     wheel.advance(1.seconds());

    //     // rotation count of all wheels should be zero
    //     assert_eq!(
    //         wheel.read().as_ref().seconds_unchecked().rotation_count(),
    //         0,
    //     );
    //     assert_eq!(
    //         wheel.read().as_ref().minutes_unchecked().rotation_count(),
    //         0,
    //     );
    //     assert_eq!(wheel.read().as_ref().hours_unchecked().rotation_count(), 0,);
    //     assert_eq!(wheel.read().as_ref().days_unchecked().rotation_count(), 0,);
    //     assert_eq!(wheel.read().as_ref().weeks_unchecked().rotation_count(), 0,);
    //     assert_eq!(wheel.read().as_ref().years_unchecked().rotation_count(), 0,);

    //     // Verify len of all wheels
    //     assert_eq!(wheel.read().as_ref().seconds_unchecked().len(), SECONDS);
    //     assert_eq!(wheel.read().as_ref().minutes_unchecked().len(), MINUTES);
    //     assert_eq!(wheel.read().as_ref().hours_unchecked().len(), HOURS);
    //     assert_eq!(wheel.read().as_ref().days_unchecked().len(), DAYS);
    //     assert_eq!(wheel.read().as_ref().weeks_unchecked().len(), WEEKS);
    //     assert_eq!(wheel.read().as_ref().years_unchecked().len(), YEARS);

    //     assert!(wheel.read().is_full());
    //     assert!(!wheel.read().is_empty());
    //     assert!(wheel.read().landmark().is_none());
    // }

    #[test]
    fn merge_test() {
        let time = 0;
        let mut wheel = RwWheel::<U32SumAggregator>::new(time);

        let entry = Entry::new(1u32, 5000);
        wheel.insert(entry);

        wheel.advance(60.minutes());

        let fresh_wheel_time = 0;
        let fresh_wheel = RwWheel::<U32SumAggregator>::new(fresh_wheel_time);
        fresh_wheel.read().merge(wheel.read());

        assert_eq!(fresh_wheel.read().watermark(), wheel.read().watermark());
        assert_eq!(fresh_wheel.read().landmark(), wheel.read().landmark());
        assert_eq!(
            fresh_wheel.read().remaining_ticks(),
            wheel.read().remaining_ticks()
        );
    }

    #[test]
    fn merge_test_low_to_high() {
        let time = 0;
        let mut wheel = RwWheel::<U32SumAggregator>::new(time);

        let entry = Entry::new(1u32, 5000);
        wheel.insert(entry);

        wheel.advance(10.seconds());

        let mut fresh_wheel = RwWheel::<U32SumAggregator>::new(time);
        fresh_wheel.insert(Entry::new(5u32, 8000));
        fresh_wheel.advance(9.seconds());

        wheel.read().merge(fresh_wheel.read());

        assert_eq!(wheel.read().landmark(), Some(6));
        // assert_eq!(wheel.read().interval(2.seconds()), Some(5));
        assert_eq!(wheel.read().interval(10.seconds()), Some(6));
    }
}
