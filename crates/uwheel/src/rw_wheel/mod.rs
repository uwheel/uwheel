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

use crate::{aggregator::Aggregator, duration::Duration, Entry, Error};
use core::fmt::Debug;
use write::{WriterWheel, DEFAULT_WRITE_AHEAD_SLOTS};

pub use read::{aggregation::DrillCut, DAYS, HOURS, MINUTES, SECONDS, WEEKS, YEARS};
pub use wheel_ext::WheelExt;

use self::read::{
    hierarchical::{HawConf, Window},
    window::WindowBuilder,
    ReaderWheel,
};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "profiler")]
use uwheel_stats::profile_scope;

/// A Reader-Writer aggregation wheel with decoupled read and write paths.
///
/// # How it works
///
/// ## Indexing
///
/// The wheel adopts a low watermark clock which is used for indexing. The watermark
/// is used to the decouple write and read paths.
///
/// ## Writes
///
/// Writes are handled by a [WriterWheel] which manages aggregates above the current watermark.
///
/// ## Reads
///
/// Reads are handled by a [ReaderWheel] which hierarchically organizes aggregates across multiple time dimensions.
/// The reader wheel internally consists of a Hierarchical Aggregate Wheel that is equipped with a wheel-based query optimizer.
///
/// ## Aggregate Synchronization
///
/// Aggregates are lazily "synchronized" between the writer and reader wheels once the low watermark has been advanced.
/// The synchronization only occurs once the user decides to advance time either through [Self::advance] or [Self::advance_to].
///
/// ## Example
///
///
/// ```
/// use uwheel::{aggregator::sum::U32SumAggregator, NumericalDuration, Entry, RwWheel};
///
/// let time = 0;
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
    /// Configures a periodic window aggregation with range ``range`` and slide ``slide``
    ///
    /// Results of the window are returned when advancing the wheel [Self::advance_to]
    pub fn window(&mut self, window: impl Into<WindowBuilder>) {
        self.reader.window(window.into());
    }

    /// Inserts an entry into the wheel
    #[inline]
    pub fn insert(&mut self, e: impl Into<Entry<A::Input>>) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.insert);
        if let Err(Error::Late { .. }) = self.writer.insert(e) {
            // For now do nothing but should be returned to the user..
        }
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
    /// Advance the watermark of the wheel by the given [time::Duration]
    #[inline]
    pub fn advance(&mut self, duration: Duration) -> Vec<Window<A::PartialAggregate>> {
        let to = self.watermark() + duration.whole_milliseconds() as u64;
        self.advance_to(to)
    }
    /// Advances the time of the wheel aligned by the lowest unit (Second)
    #[inline]
    pub fn advance_to(&mut self, watermark: u64) -> Vec<Window<A::PartialAggregate>> {
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
    pub fn with_write_ahead(mut self, capacity: usize) -> Self {
        self.writer_conf.write_ahead_capacity = capacity;
        self
    }
    /// Configures the reader wheel to use the given [HawConf]
    pub fn with_haw_conf(mut self, conf: HawConf) -> Self {
        self.reader_conf.haw_conf = conf;
        self
    }
}

#[cfg(test)]
mod tests {
    #[cfg(all(feature = "timer", not(feature = "serde")))]
    use core::cell::RefCell;
    #[cfg(all(feature = "timer", not(feature = "serde")))]
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

    #[cfg(all(feature = "timer", not(feature = "serde")))]
    #[test]
    fn timer_once_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::default();
        let gate = Rc::new(RefCell::new(false));
        let inner_gate = gate.clone();

        let _ = rw_wheel.read().schdule_once(5000, move |read| {
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

    #[cfg(all(feature = "timer", not(feature = "serde")))]
    #[test]
    fn timer_repeat_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::default();
        let sum = Rc::new(RefCell::new(0));
        let inner_sum = sum.clone();

        // schedule a repeat action
        let _ = rw_wheel
            .read()
            .schdule_repeat(5000, 5.seconds(), move |read| {
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
                .aggregate_and_lower(0..5),
            Some(6u32)
        );
    }

    // #[test]
    // fn full_cycle_test() {
    //     let mut wheel = RwWheel::<U32SumAggregator>::new(0);

    //     let ticks = wheel.read().remaining_ticks() - 1;
    //     wheel.advance(time::Duration::seconds(ticks as i64));

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

    #[should_panic]
    #[test]
    fn drill_down_test() {
        use crate::aggregator::sum::U64SumAggregator;
        let mut time = 0;
        let mut haw_conf = HawConf::default();
        haw_conf.seconds.set_drill_down(true);

        let seconds = haw_conf.seconds.with_drill_down(true);

        let minutes = haw_conf
            .minutes
            .with_retention_policy(read::aggregation::conf::RetentionPolicy::Keep)
            .with_drill_down(true);

        let hours = haw_conf
            .hours
            .with_retention_policy(read::aggregation::conf::RetentionPolicy::Keep)
            .with_drill_down(true);

        let days = haw_conf
            .days
            .with_retention_policy(read::aggregation::conf::RetentionPolicy::Keep)
            .with_drill_down(true);

        let haw_conf = haw_conf
            .with_seconds(seconds)
            .with_minutes(minutes)
            .with_hours(hours)
            .with_days(days);

        let conf = Conf::default().with_haw_conf(haw_conf);

        let mut wheel = RwWheel::<U64SumAggregator>::with_conf(conf);

        let days_as_secs = time::Duration::days((DAYS + 1) as i64).whole_seconds();

        for _ in 0..days_as_secs {
            let entry = Entry::new(1u64, time);
            wheel.insert(entry);
            time += 1000; // increase by 1 second
            wheel.advance_to(time);
        }

        // can't drill down on seconds wheel as it is the first wheel
        assert!(wheel
            .read()
            .as_ref()
            .seconds_unchecked()
            .drill_down(0)
            .is_none());

        // Drill down on each wheel (e.g., minute, hours, days) and confirm summed results

        assert_eq!(
            wheel
                .read()
                .as_ref()
                .minutes_unchecked()
                .drill_down(0)
                .unwrap()
                .iter()
                .sum::<u64>(),
            60u64
        );

        assert_eq!(
            wheel
                .read()
                .as_ref()
                .hours_unchecked()
                .drill_down(0)
                .unwrap()
                .iter()
                .sum::<u64>(),
            60u64 * 60
        );

        assert_eq!(
            wheel
                .read()
                .as_ref()
                .days_unchecked()
                .drill_down(0)
                .unwrap()
                .iter()
                .sum::<u64>(),
            60u64 * 60 * 24
        );
    }

    #[should_panic]
    #[test]
    fn drill_down_holes_test() {
        let mut time = 0;
        let mut haw_conf = HawConf::default();
        haw_conf.seconds.set_drill_down(true);
        let minutes = haw_conf
            .minutes
            .with_drill_down(true)
            .with_retention_policy(read::aggregation::conf::RetentionPolicy::Keep);
        let haw_conf = haw_conf.with_minutes(minutes);
        let conf = Conf::default().with_haw_conf(haw_conf);

        let mut wheel = RwWheel::<U32SumAggregator>::with_conf(conf);

        for _ in 0..30 {
            let entry = Entry::new(1u32, time);
            wheel.insert(entry);
            time += 2000; // increase by 2 seconds
            wheel.advance_to(time);
        }

        wheel.advance_to(time);

        // confirm there are "holes" as we bump time by 2 seconds above
        let decoded = wheel
            .read()
            .as_ref()
            .minutes_unchecked()
            .drill_down(0)
            .unwrap()
            .to_vec();
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
