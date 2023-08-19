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

/// Hierarchical Wheel Timer
#[allow(dead_code)]
mod timer;

use crate::{aggregator::Aggregator, time, Entry, Error};
use core::fmt::Debug;
use read::ReadWheel;
use write::{WriteAheadWheel, DEFAULT_WRITE_AHEAD_SLOTS};

pub use read::{aggregation::DrillCut, DAYS, HOURS, MINUTES, SECONDS, WEEKS, YEARS};
pub use wheel_ext::WheelExt;

use self::timer::RawTimerWheel;
#[cfg(not(feature = "serde"))]
use self::timer::{timer_wheel::TimerAction, timer_wheel::TimerWheel};

/// A Reader-Writer aggregation wheel with decoupled read and write paths.
///
/// Writes are handled by a Write-ahead wheel which contain aggregates above the current watermark.
/// Aggregates are moved to the read wheel once time has been ticked enough.
///
/// The ``ReadWheel`` is backed by interior mutability and by default supports a single reader. For multiple readers,
/// the ``sync`` feature must be enabled.
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
//#[derive(Clone)]
pub struct RwWheel<A: Aggregator> {
    entry_timer: RawTimerWheel<Entry<A::Input>>,
    write: WriteAheadWheel<A>,
    read: ReadWheel<A>,
    #[cfg(not(feature = "serde"))]
    timer: TimerWheel<A>,
}
impl<A: Aggregator> RwWheel<A> {
    /// Creates a new Wheel starting from the given time
    ///
    /// Time is represented as milliseconds
    pub fn new(time: u64) -> Self {
        Self {
            entry_timer: RawTimerWheel::new(time),
            write: WriteAheadWheel::with_watermark(time),
            read: ReadWheel::new(time),
            #[cfg(not(feature = "serde"))]
            timer: TimerWheel::new(time),
        }
    }
    /// Creates a new Wheel starting from the given time with drill down enabled
    ///
    /// Time is represented as milliseconds
    pub fn with_drill_down(time: u64) -> Self {
        Self {
            entry_timer: RawTimerWheel::new(time),
            write: WriteAheadWheel::with_watermark(time),
            read: ReadWheel::with_drill_down(time),
            #[cfg(not(feature = "serde"))]
            timer: TimerWheel::new(time),
        }
    }
    /// Creates a new wheel starting from the given time and the specified [Options]
    pub fn with_options(time: u64, opts: Options) -> Self {
        let write: WriteAheadWheel<A> =
            WriteAheadWheel::with_capacity_and_watermark(opts.write_ahead_capacity, time);
        let read: ReadWheel<A> = if opts.drill_down {
            ReadWheel::with_drill_down(time)
        } else {
            ReadWheel::new(time)
        };
        Self {
            entry_timer: RawTimerWheel::new(time),
            write,
            read,
            #[cfg(not(feature = "serde"))]
            timer: TimerWheel::new(time),
        }
    }
    /// Inserts an entry into the wheel
    #[inline]
    pub fn insert(&mut self, e: impl Into<Entry<A::Input>>) {
        if let Err(Error::Overflow {
            entry,
            max_write_ahead_ts: _,
        }) = self.write.insert(e)
        {
            // If entry does not fit within the write-ahead wheel then schedule it to be inserted in the future
            // TODO: cluster the entry with other timestamps around the same write-ahead range
            let write_ahead_ms = time::Duration::seconds(self.write.write_ahead_len() as i64)
                .whole_milliseconds() as u64;
            let timestamp = entry.timestamp - write_ahead_ms / 2; // for now
            self.entry_timer.schedule_at(timestamp, entry).unwrap();
        }
    }

    /// Returns a reference to the underlying Write-ahead Wheel
    pub fn write(&self) -> &WriteAheadWheel<A> {
        &self.write
    }
    /// Returns a reference to the underlying ReadWheel
    pub fn read(&self) -> &ReadWheel<A> {
        &self.read
    }
    /// Returns a reference to the TimerWheel
    #[cfg(not(feature = "serde"))]
    pub fn timer(&self) -> &TimerWheel<A> {
        &self.timer
    }
    /// Merges another read wheel with same size into this one
    pub fn merge_read_wheel(&self, other: &ReadWheel<A>) {
        self.read().merge(other);
    }
    /// Returns the current watermark of this wheel
    pub fn watermark(&self) -> u64 {
        self.write.watermark()
    }
    /// Advance the watermark of the wheel by the given [time::Duration]
    #[inline]
    pub fn advance(&mut self, duration: time::Duration) {
        let to = self.watermark() + duration.whole_milliseconds() as u64;
        self.advance_to(to);
    }

    /// Advances the time of the wheel aligned by the lowest unit (Second)
    #[inline]
    pub fn advance_to(&mut self, watermark: u64) {
        // Advance the read wheel
        self.read.advance_to(watermark, &mut self.write);
        debug_assert_eq!(self.write.watermark(), self.read.watermark());

        // Check if there are entries that can be inserted into the write-ahead wheel
        for entry in self.entry_timer.advance_to(watermark) {
            self.write.insert(entry).unwrap(); // this is assumed to be safe if it was scheduled correctly
        }
        #[cfg(not(feature = "serde"))]
        {
            // Fire any outstanding timers
            for action in self.timer.advance_to(watermark) {
                match action {
                    TimerAction::Insert(entry) => {
                        // If this overflows then something is wrong..
                        //dbg!(entry);
                        self.insert(entry);
                    }
                    TimerAction::Oneshot(udf) => {
                        udf(&self.read);
                    }
                    TimerAction::Repeat((at, interval, udf)) => {
                        udf(&self.read);
                        let new_at = at + interval.whole_milliseconds() as u64;
                        let _ = self.timer.schdule_repeat(new_at, interval, udf);
                    }
                }
            }
        }
    }
    /// Returns an estimation of bytes used by the wheel
    pub fn size_bytes(&self) -> usize {
        let read = self.read.size_bytes();
        let write = self.write.size_bytes().unwrap();
        read + write
    }
}

/// Options to customise a [RwWheel]
#[derive(Debug, Copy, Clone)]
pub struct Options {
    /// Enables drill-down capabilities
    drill_down: bool,
    /// Defines the capacity of write-ahead slots
    write_ahead_capacity: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            drill_down: false,
            write_ahead_capacity: DEFAULT_WRITE_AHEAD_SLOTS,
        }
    }
}
impl Options {
    /// Enable drill-down capabilities at the cost of more storage
    pub fn with_drill_down(mut self) -> Self {
        self.drill_down = true;
        self
    }
    /// Configure the number of write-ahead slots
    ///
    /// The default value is [DEFAULT_WRITE_AHEAD_SLOTS]
    pub fn with_write_ahead(mut self, capacity: usize) -> Self {
        self.write_ahead_capacity = capacity;
        self
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "serde"))]
    use core::cell::RefCell;
    #[cfg(not(feature = "serde"))]
    use std::rc::Rc;

    use super::{WheelExt, *};
    use crate::{aggregator::sum::U32SumAggregator, time::*, *};

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

    #[cfg(not(feature = "serde"))]
    #[test]
    fn timer_once_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
        let gate = Rc::new(RefCell::new(false));
        let inner_gate = gate.clone();

        let _ = rw_wheel.timer().schdule_once(5000, move |read| {
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

    #[cfg(not(feature = "serde"))]
    #[test]
    fn timer_repeat_test() {
        let mut rw_wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
        let sum = Rc::new(RefCell::new(0));
        let inner_sum = sum.clone();

        // schedule a repeat action
        let _ = rw_wheel
            .timer()
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

        let expected: &[_] = &[&None, &Some(1u32), &None, &None, &None, &Some(5)];
        assert_eq!(
            &wheel
                .read()
                .seconds()
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<&Option<u32>>>(),
            expected
        );

        assert_eq!(
            wheel.read().seconds().as_ref().unwrap().interval(5),
            Some(6u32)
        );
        assert_eq!(
            wheel.read().seconds().as_ref().unwrap().interval(1),
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

        assert_eq!(wheel.read().seconds().as_ref().unwrap().total(), Some(6u32));
        // check we get the same result by combining the range of last 6 seconds
        assert_eq!(
            wheel
                .read()
                .seconds()
                .as_ref()
                .unwrap()
                .combine_and_lower_range(0..5),
            Some(6u32)
        );
    }

    #[test]
    fn full_cycle_test() {
        let mut wheel = RwWheel::<U32SumAggregator>::new(0);

        let ticks = wheel.read().remaining_ticks() - 1;
        wheel.advance(time::Duration::seconds(ticks as i64));

        // one tick away from full cycle clear
        assert_eq!(
            wheel.read().seconds().as_ref().unwrap().rotation_count(),
            SECONDS - 1
        );
        assert_eq!(
            wheel.read().minutes().as_ref().unwrap().rotation_count(),
            MINUTES - 1
        );
        assert_eq!(
            wheel.read().hours().as_ref().unwrap().rotation_count(),
            HOURS - 1
        );
        assert_eq!(
            wheel.read().days().as_ref().unwrap().rotation_count(),
            DAYS - 1
        );
        assert_eq!(
            wheel.read().weeks().as_ref().unwrap().rotation_count(),
            WEEKS - 1
        );
        assert_eq!(
            wheel.read().years().as_ref().unwrap().rotation_count(),
            YEARS - 1
        );

        // force full cycle clear
        wheel.advance(1.seconds());

        // rotation count of all wheels should be zero
        assert_eq!(wheel.read().seconds().as_ref().unwrap().rotation_count(), 0,);
        assert_eq!(wheel.read().minutes().as_ref().unwrap().rotation_count(), 0,);
        assert_eq!(wheel.read().hours().as_ref().unwrap().rotation_count(), 0,);
        assert_eq!(wheel.read().days().as_ref().unwrap().rotation_count(), 0,);
        assert_eq!(wheel.read().weeks().as_ref().unwrap().rotation_count(), 0,);
        assert_eq!(wheel.read().years().as_ref().unwrap().rotation_count(), 0,);

        // Verify len of all wheels
        assert_eq!(wheel.read().seconds().as_ref().unwrap().len(), SECONDS);
        assert_eq!(wheel.read().minutes().as_ref().unwrap().len(), MINUTES);
        assert_eq!(wheel.read().hours().as_ref().unwrap().len(), HOURS);
        assert_eq!(wheel.read().days().as_ref().unwrap().len(), DAYS);
        assert_eq!(wheel.read().weeks().as_ref().unwrap().len(), WEEKS);
        assert_eq!(wheel.read().years().as_ref().unwrap().len(), YEARS);

        assert!(wheel.read().is_full());
        assert!(!wheel.read().is_empty());
        assert!(wheel.read().landmark().is_none());
    }

    #[test]
    fn drill_down_test() {
        use crate::aggregator::sum::U64SumAggregator;

        let mut time = 0;
        let mut wheel = RwWheel::<U64SumAggregator>::with_drill_down(time);

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
            .seconds()
            .as_ref()
            .unwrap()
            .drill_down(1)
            .is_none());

        // Drill down on each wheel (e.g., minute, hours, days) and confirm summed results

        assert_eq!(
            wheel
                .read()
                .minutes()
                .as_ref()
                .unwrap()
                .drill_down(1)
                .unwrap()
                .iter()
                .sum::<u64>(),
            60u64
        );

        assert_eq!(
            wheel
                .read()
                .hours()
                .as_ref()
                .unwrap()
                .drill_down(1)
                .unwrap()
                .iter()
                .sum::<u64>(),
            60u64 * 60
        );

        assert_eq!(
            wheel
                .read()
                .days()
                .as_ref()
                .unwrap()
                .drill_down(1)
                .unwrap()
                .iter()
                .sum::<u64>(),
            60u64 * 60 * 24
        );

        // drill down range of 3 and confirm combined aggregates
        let decoded = wheel
            .read()
            .minutes()
            .as_ref()
            .unwrap()
            .combine_drill_down_range(..3);
        assert_eq!(decoded[0], 3);
        assert_eq!(decoded[1], 3);
        assert_eq!(decoded[59], 3);

        // test cut of last 5 seconds of last 1 minute + first 10 aggregates of last 2 min
        let decoded = wheel
            .read()
            .minutes()
            .as_ref()
            .unwrap()
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
        let decoded = wheel
            .read()
            .minutes()
            .as_ref()
            .unwrap()
            .combine_drill_down_range(..);
        let sum = decoded.iter().sum::<u64>();
        assert_eq!(sum, 3600u64);
    }

    #[test]
    fn drill_down_holes_test() {
        let mut time = 0;
        let mut wheel = RwWheel::<U32SumAggregator>::with_drill_down(time);

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
            .minutes()
            .as_ref()
            .unwrap()
            .drill_down(1)
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
    fn merge_drill_down_test() {
        let mut time = 0;
        let mut wheel = RwWheel::<U32SumAggregator>::with_drill_down(time);

        for _ in 0..30 {
            let entry = Entry::new(1u32, time);
            wheel.insert(entry);
            time += 2000; // increase by 2 seconds
            wheel.advance_to(time);
        }

        wheel.advance_to(time);

        let mut time = 0;
        let mut other_wheel = RwWheel::<U32SumAggregator>::with_drill_down(time);

        for _ in 0..30 {
            let entry = Entry::new(1u32, time);
            other_wheel.insert(entry);
            time += 2000; // increase by 2 seconds
            other_wheel.advance_to(time);
        }

        other_wheel.advance_to(time);

        // merge other_wheel into ´wheel´
        wheel.read().merge(other_wheel.read());

        // same as drill_down_holes test but confirm that drill down slots have be merged between wheels
        let decoded = wheel
            .read()
            .minutes()
            .as_ref()
            .unwrap()
            .drill_down(1)
            .unwrap()
            .to_vec();
        assert_eq!(decoded[0], 2);
        assert_eq!(decoded[1], 0);
        assert_eq!(decoded[2], 2);
        assert_eq!(decoded[3], 0);

        assert_eq!(decoded[58], 2);
        assert_eq!(decoded[59], 0);
    }
}
