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

use crate::{aggregator::Aggregator, delta::DeltaState, time_internal, Entry, Error};
use core::fmt::Debug;
use read::ReadWheel;
use write::{WriteAheadWheel, DEFAULT_WRITE_AHEAD_SLOTS};

pub use read::{aggregation::DrillCut, DAYS, HOURS, MINUTES, SECONDS, WEEKS, YEARS};
pub use wheel_ext::WheelExt;

use self::{
    read::{hierarchical::HawConf, Kind, Lazy},
    timer::RawTimerWheel,
};

#[cfg(feature = "profiler")]
use awheel_stats::profile_scope;

/// A Reader-Writer aggregation wheel with decoupled read and write paths.
///
/// ## Indexing
///
/// The wheel adopts a low watermark clock which is used for indexing. The watermark
/// is used to the decouple write and read paths.
///
/// ## Writes
///
/// Writes are handled by a write-ahead wheel which contain aggregates above the current watermark.
/// The write-ahead wheel has a fixed-sized capacity meaning it only supports N number of seconds above the
/// watermark. Writes that do not fit within this capacity is scheduled into a Overflow wheel (Hierarchical Timing Wheel)
/// to be inserted once time has passed enough.
///
/// ## Reads
///
/// Aggregates once moved to below the watermark become immutable and are inserted into
/// a hierarchical aggregation wheel [ReadWheel]. A data structure which materializes aggregates
/// across time.
pub struct RwWheel<A, K = Lazy>
where
    A: Aggregator,
    K: Kind,
{
    overflow: RawTimerWheel<Entry<A::Input>>,
    write: WriteAheadWheel<A>,
    read: ReadWheel<A, K>,
    #[cfg(feature = "profiler")]
    stats: stats::Stats,
}
impl<A, K> RwWheel<A, K>
where
    A: Aggregator,
    K: Kind,
{
    /// Creates a new Wheel starting from the given time and enables drill-down on all granularities
    ///
    /// Time is represented as milliseconds
    pub fn with_drill_down(time: u64) -> Self {
        let mut haw_conf = HawConf::default();
        haw_conf.seconds.set_drill_down(true);
        haw_conf.minutes.set_drill_down(true);
        haw_conf.hours.set_drill_down(true);
        haw_conf.days.set_drill_down(true);
        haw_conf.weeks.set_drill_down(true);
        haw_conf.years.set_drill_down(true);

        let options = Options::default().with_haw_conf(haw_conf);

        Self {
            overflow: RawTimerWheel::new(time),
            write: WriteAheadWheel::with_watermark(time),
            read: ReadWheel::with_conf(time, options.haw_conf),
            #[cfg(feature = "profiler")]
            stats: stats::Stats::default(),
        }
    }
    /// Creates a new Wheel starting from the given time
    ///
    /// Time is represented as milliseconds
    pub fn new(time: u64) -> Self {
        Self {
            overflow: RawTimerWheel::new(time),
            write: WriteAheadWheel::with_watermark(time),
            read: ReadWheel::new(time),
            #[cfg(feature = "profiler")]
            stats: stats::Stats::default(),
        }
    }
    /// Creates a new wheel starting from the given time and the specified [Options]
    pub fn with_options(time: u64, opts: Options) -> Self {
        let write: WriteAheadWheel<A> =
            WriteAheadWheel::with_capacity_and_watermark(opts.write_ahead_capacity, time);
        let read = ReadWheel::with_conf(time, opts.haw_conf);
        Self {
            overflow: RawTimerWheel::new(time),
            write,
            read,
            #[cfg(feature = "profiler")]
            stats: stats::Stats::default(),
        }
    }
    /// Inserts an entry into the wheel
    #[inline]
    pub fn insert(&mut self, e: impl Into<Entry<A::Input>>) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.insert);

        // If entry does not fit within the write-ahead wheel then schedule it to be inserted in the future
        if let Err(Error::Overflow {
            entry,
            max_write_ahead_ts: _,
        }) = self.write.insert(e)
        {
            #[cfg(feature = "profiler")]
            profile_scope!(&self.stats.overflow_schedule);
            // TODO: cluster the entry with other timestamps around the same write-ahead range
            let write_ahead_ms =
                time_internal::Duration::seconds(self.write.write_ahead_len() as i64)
                    .whole_milliseconds() as u64;
            let timestamp = entry.timestamp - write_ahead_ms / 2; // for now
            self.overflow.schedule_at(timestamp, entry).unwrap();
        }
    }

    /// Returns a reference to the underlying Write-ahead Wheel
    pub fn write(&self) -> &WriteAheadWheel<A> {
        &self.write
    }
    /// Returns a reference to the underlying ReadWheel
    pub fn read(&self) -> &ReadWheel<A, K> {
        &self.read
    }
    /// Merges another read wheel with same size into this one
    pub fn merge_read_wheel(&self, other: &ReadWheel<A, K>) {
        self.read().merge(other);
    }
    /// Returns the current watermark of this wheel
    pub fn watermark(&self) -> u64 {
        self.write.watermark()
    }
    /// Advance the watermark of the wheel by the given [time::Duration]
    #[inline]
    pub fn advance(&mut self, duration: time_internal::Duration) {
        let to = self.watermark() + duration.whole_milliseconds() as u64;
        self.advance_to(to);
    }
    /// Advance the watermark of the wheel by the given [time::Duration] and returns deltas
    #[inline]
    pub fn advance_and_emit_deltas(
        &mut self,
        duration: time_internal::Duration,
    ) -> DeltaState<A::PartialAggregate> {
        let to = self.watermark() + duration.whole_milliseconds() as u64;
        self.advance_to_and_emit_deltas(to)
    }

    /// Advances the time of the wheel aligned by the lowest unit (Second)
    #[inline]
    pub fn advance_to(&mut self, watermark: u64) {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.advance);

        // Advance the read wheel
        self.read.advance_to(watermark, &mut self.write);
        debug_assert_eq!(self.write.watermark(), self.read.watermark());

        // Check if there are entries that can be inserted into the write-ahead wheel
        for entry in self.overflow.advance_to(watermark) {
            self.write.insert(entry).unwrap(); // this is assumed to be safe if it was scheduled correctly
        }
    }
    /// Advances the time of the wheel aligned by the lowest unit (Second) and emits deltas
    #[inline]
    pub fn advance_to_and_emit_deltas(
        &mut self,
        watermark: u64,
    ) -> DeltaState<A::PartialAggregate> {
        #[cfg(feature = "profiler")]
        profile_scope!(&self.stats.advance);

        // Advance the read wheel
        let delta_state = self.read.advance_and_emit_deltas(
            time_internal::Duration::milliseconds((watermark - self.watermark()) as i64),
            &mut self.write,
        );
        debug_assert_eq!(self.write.watermark(), self.read.watermark());

        // Check if there are entries that can be inserted into the write-ahead wheel
        for entry in self.overflow.advance_to(watermark) {
            self.write.insert(entry).unwrap(); // this is assumed to be safe if it was scheduled correctly
        }
        delta_state
    }
    /// Returns an estimation of bytes used by the wheel
    pub fn size_bytes(&self) -> usize {
        let read = self.read.as_ref().size_bytes();
        let write = self.write.size_bytes().unwrap();
        read + write
    }
    #[cfg(feature = "profiler")]
    /// Prints the stats of the [RwWheel]
    pub fn print_stats(&self) {
        use awheel_stats::Sketch;
        use prettytable::{row, Table};
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
        add_row(
            "overflow schedule",
            &mut table,
            &self.stats.overflow_schedule,
        );

        let read = self.read.as_ref();
        add_row("tick", &mut table, &read.stats().tick);
        add_row("interval", &mut table, &read.stats().interval);
        add_row("landmark", &mut table, &read.stats().landmark);

        println!("====RwWheel Profiler Dump====");
        table.printstd();
    }
}

impl<A, K> Drop for RwWheel<A, K>
where
    A: Aggregator,
    K: Kind,
{
    fn drop(&mut self) {
        #[cfg(feature = "profiler")]
        self.print_stats();
    }
}

/// Options to customise a [RwWheel]
#[derive(Debug, Copy, Clone)]
pub struct Options {
    /// Defines the capacity of write-ahead slots
    write_ahead_capacity: usize,
    /// Hierarchical Aggregation Wheel scheme
    haw_conf: HawConf,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            write_ahead_capacity: DEFAULT_WRITE_AHEAD_SLOTS,
            haw_conf: Default::default(),
        }
    }
}
impl Options {
    /// Configure the number of write-ahead slots
    ///
    /// The default value is [DEFAULT_WRITE_AHEAD_SLOTS]
    pub fn with_write_ahead(mut self, capacity: usize) -> Self {
        self.write_ahead_capacity = capacity;
        self
    }
    /// Configures the wheel to use the given [HawConf]
    pub fn with_haw_conf(mut self, conf: HawConf) -> Self {
        self.haw_conf = conf;
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
    use crate::{aggregator::sum::U32SumAggregator, time_internal::*, *};

    #[test]
    fn delta_emit_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
        rw_wheel.insert(Entry::new(250, 1000));
        rw_wheel.insert(Entry::new(250, 2000));
        rw_wheel.insert(Entry::new(250, 3000));
        rw_wheel.insert(Entry::new(250, 4000));

        let delta_state = rw_wheel.advance_to_and_emit_deltas(5000);
        assert_eq!(delta_state.oldest_ts, 0);
        assert_eq!(
            delta_state.deltas,
            vec![None, Some(250), Some(250), Some(250), Some(250)]
        );

        assert_eq!(rw_wheel.read().interval(4.seconds()), Some(1000));

        // create a new read wheel from deltas
        let read: ReadWheel<U32SumAggregator> = ReadWheel::from_delta_state(delta_state);
        // verify the watermark
        assert_eq!(read.watermark(), 5000);
        // verify the the results
        assert_eq!(read.interval(4.seconds()), Some(1000));
    }

    #[test]
    fn insert_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
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
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
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
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
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
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
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
            wheel.read().as_ref().seconds_unchecked().interval(5),
            Some(6u32)
        );
        assert_eq!(
            wheel.read().as_ref().seconds_unchecked().interval(1),
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
                .combine_and_lower_range(0..5),
            Some(6u32)
        );
    }

    // #[test]
    // fn eager_wheel_test() {
    //     let mut wheel = RwWheel::<U32SumAggregator, Eager>::new(0);

    //     wheel.advance(60.seconds());
    //     let watermark = wheel.watermark();

    //     wheel.insert(Entry::new(100, watermark));
    //     wheel.insert(Entry::new(10, watermark + 1000));

    //     wheel.advance(1.seconds());

    //     // both last 1 second and 1 minute should have a sum of 100
    //     // as aggregation is done eagerly across both wheels
    //     assert_eq!(wheel.read().interval(1.seconds()), Some(100));
    //     assert_eq!(wheel.read().interval(1.minutes()), Some(100));

    //     wheel.insert(Entry::new(10, watermark + 1000));

    //     wheel.advance(60.seconds());
    //     assert_eq!(wheel.read().interval(2.minutes()), Some(120));
    // }

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

        let options = Options::default().with_haw_conf(haw_conf);

        let mut wheel = RwWheel::<U64SumAggregator>::with_options(time, options);

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
            .table()
            .drill_down(0)
            .is_none());

        // Drill down on each wheel (e.g., minute, hours, days) and confirm summed results

        assert_eq!(
            wheel
                .read()
                .as_ref()
                .minutes_unchecked()
                .table()
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
                .table()
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
                .table()
                .drill_down(0)
                .unwrap()
                .iter()
                .sum::<u64>(),
            60u64 * 60 * 24
        );

        // test cut of last 5 seconds of last 1 minute + first 10 aggregates of last 2 min
        let decoded = wheel
            .read()
            .as_ref()
            .minutes_unchecked()
            .table()
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
    }

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
        let options = Options::default().with_haw_conf(haw_conf);

        let mut wheel = RwWheel::<U32SumAggregator>::with_options(time, options);

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
            .table()
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

    #[test]
    fn merge_drill_down_test() {
        // let mut time = 0;

        // let mut haw_conf = HawConf::default();
        // haw_conf.seconds.set_drill_down(true);
        // let minutes = haw_conf
        //     .minutes
        //     .with_drill_down(true)
        //     .with_retention_policy(read::aggregation::conf::RetentionPolicy::Keep);
        // let haw_conf = haw_conf.with_minutes(minutes);

        // let options = Options::default().with_haw_conf(haw_conf);

        // let mut wheel = RwWheel::<U32SumAggregator>::with_options(time, options);
        // for _ in 0..30 {
        //     let entry = Entry::new(1u32, time);
        //     wheel.insert(entry);
        //     time += 2000; // increase by 2 seconds
        //     wheel.advance_to(time);
        // }

        // wheel.advance_to(time);

        // let mut time = 0;
        // let mut other_wheel = RwWheel::<U32SumAggregator>::with_options(time, options);

        // for _ in 0..30 {
        //     let entry = Entry::new(1u32, time);
        //     other_wheel.insert(entry);
        //     time += 2000; // increase by 2 seconds
        //     other_wheel.advance_to(time);
        // }

        // other_wheel.advance_to(time);

        // // merge other_wheel into ´wheel´
        // wheel.read().merge(other_wheel.read());

        // // same as drill_down_holes test but confirm that drill down slots have be merged between wheels
        // let decoded = wheel
        //     .read()
        //     .as_ref()
        //     .minutes_unchecked()
        //     .table()
        //     .drill_down(0)
        //     .unwrap()
        //     .to_vec();
        // assert_eq!(decoded[0], 2);
        // assert_eq!(decoded[1], 0);
        // assert_eq!(decoded[2], 2);
        // assert_eq!(decoded[3], 0);

        // assert_eq!(decoded[58], 2);
        // assert_eq!(decoded[59], 0);
    }
}
