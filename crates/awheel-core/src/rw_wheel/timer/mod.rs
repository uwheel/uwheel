//! This module contains a Hierarchical Wheel Timer implementation
//!
//! It has been taken from the following [crate](https://github.com/Bathtor/rust-hash-wheel-timer/tree/master).
//! License: MIT

mod byte_wheel;
mod quad_wheel;

use self::{byte_wheel::Bounds, quad_wheel::QuadWheelWithOverflow};
use core::{fmt::Debug, hash::Hash, time::Duration};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Result of a [can_skip](quad_wheel::QuadWheelWithOverflow::can_skip) invocation
#[derive(PartialEq, Debug)]
pub enum Skip {
    /// The wheel is completely empty, so there's no point in skipping
    ///
    /// In fact, this may be a good opportunity to reset the wheel, if the
    /// time semantics allow for that.
    Empty,
    /// It's possible to skip up to the provided number of ticks (in ms)
    Millis(u32),
    /// Nothing can be skipped, as the next tick has expiring timers
    None,
}

impl Skip {
    /// Provide a skip instance from ms
    ///
    /// A `ms` value of `0` will result in a `Skip::None`.
    pub fn from_millis(ms: u32) -> Skip {
        if ms == 0 {
            Skip::None
        } else {
            Skip::Millis(ms)
        }
    }

    /// A skip instance for empty wheels
    pub fn empty() -> Skip {
        Skip::Empty
    }
}

/// A trait for timer entries that store their delay along the with the state
pub trait TimerEntryWithDelay: Debug {
    /// Returns the time until the timeout is supposed to be triggered
    fn delay(&self) -> Duration;
}

/// Errors encounted by a timer implementation
#[derive(Debug)]
pub enum TimerError<EntryType> {
    /// The timeout with the given id was not found
    NotFound,
    /// The timout has already expired
    Expired(EntryType),
}

/// A simple implementation of a timer entry that only stores its own unique id and the original delay
#[derive(Debug)]
pub struct IdOnlyTimerEntry<I> {
    /// The unique identifier part of the entry
    pub id: I,
    /// The delay that this entry is to be schedulled with (i.e., expire after)
    pub delay: Duration,
}
impl<I> IdOnlyTimerEntry<I> {
    /// Create a new timer entry from the id and the delay after which it should expire
    pub fn new(id: I, delay: Duration) -> Self {
        IdOnlyTimerEntry { id, delay }
    }
}

impl<I> TimerEntryWithDelay for IdOnlyTimerEntry<I>
where
    I: Hash + Clone + Eq + core::fmt::Debug,
{
    fn delay(&self) -> Duration {
        self.delay
    }
}

#[derive(Debug)]
pub struct TimerExpiredError<T: Debug> {
    /// Current event time
    pub current_time: u64,
    /// The scheduled time
    pub scheduled_time: u64,
    /// Timer Entry
    pub entry: T,
}
use core::{fmt, fmt::Display};
impl<T: Debug> Display for TimerExpiredError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Attempted to schedule timer entry {:?} at {} when time is {}",
            self.entry, self.scheduled_time, self.current_time
        )
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(
    feature = "serde",
    serde(bound = "A: serde::Serialize + for<'a> serde::Deserialize<'a>")
)]
pub struct RawTimerWheel<A: Bounds> {
    timer: QuadWheelWithOverflow<A>,
    time: u64,
}

impl<A: Bounds> RawTimerWheel<A> {
    pub fn new(time: u64) -> Self {
        Self {
            timer: QuadWheelWithOverflow::default(),
            time,
        }
    }
    #[inline(always)]
    pub fn schedule_at(&mut self, time: u64, entry: A) -> Result<(), TimerError<A>> {
        let curr_time = self.time();
        let delay = time - curr_time;

        self.timer
            .insert_with_delay(entry, Duration::from_millis(delay))
    }

    #[inline]
    pub(crate) fn time(&self) -> u64 {
        self.time
    }

    #[inline(always)]
    pub fn add_time(&mut self, by: u64) {
        self.time += by;
    }

    #[inline(always)]
    pub fn tick_and_collect(&mut self, res: &mut Vec<A>, mut time_left: u32) {
        while time_left > 0 {
            match self.timer.can_skip() {
                Skip::Empty => {
                    // Timer is empty, no point in ticking it
                    self.add_time(time_left as u64);
                    return;
                }
                Skip::Millis(skip_ms) => {
                    // Skip forward
                    if skip_ms >= time_left {
                        // No more ops to gather, skip the remaining time_left and return
                        self.timer.skip(time_left);
                        self.add_time(time_left as u64);
                        return;
                    } else {
                        // Skip lower than time-left:
                        self.timer.skip(skip_ms);
                        self.add_time(skip_ms as u64);
                        time_left -= skip_ms;
                    }
                }
                Skip::None => {
                    for entry in self.timer.tick() {
                        res.push(entry);
                    }
                    self.add_time(1u64);
                    time_left -= 1u32;
                }
            }
        }
    }

    #[inline]
    pub fn advance_to(&mut self, ts: u64) -> Vec<A> {
        let curr_time = self.time();
        if ts < curr_time {
            // advance_to called with lower timestamp than current time
            return Vec::new();
        }

        let mut res = Vec::new();
        let mut time_left = ts - curr_time;

        while time_left > u32::MAX as u64 {
            self.tick_and_collect(&mut res, u32::MAX);
            time_left -= u32::MAX as u64;
        }
        // this cast must be safe now
        self.tick_and_collect(&mut res, time_left as u32);

        res
    }
}

#[cfg(not(feature = "serde"))]
pub mod timer_wheel {
    use crate::{rw_wheel::timer::TimerError, time, Aggregator, Entry, ReadWheel};
    #[cfg(not(feature = "std"))]
    use alloc::{boxed::Box, vec::Vec};
    use inner_impl::Inner;

    pub type WheelFn<A> = Box<dyn Fn(&ReadWheel<A>)>;

    pub enum TimerAction<A: Aggregator> {
        Insert(Entry<A::Input>),
        Oneshot(WheelFn<A>),
        Repeat((u64, time::Duration, WheelFn<A>)),
    }

    #[derive(Clone)]
    pub struct TimerWheel<A: Aggregator> {
        inner: Inner<A>,
    }
    impl<A: Aggregator> TimerWheel<A> {
        pub fn new(time: u64) -> Self {
            Self {
                inner: Inner::new(time),
            }
        }
        pub fn schdule_once(
            &self,
            time: u64,
            f: impl Fn(&ReadWheel<A>) + 'static,
        ) -> Result<(), TimerError<TimerAction<A>>> {
            self.schedule_at(time, TimerAction::Oneshot(Box::new(f)))
        }
        pub fn schdule_repeat(
            &self,
            at: u64,
            interval: time::Duration,
            f: impl Fn(&ReadWheel<A>) + 'static,
        ) -> Result<(), TimerError<TimerAction<A>>> {
            self.schedule_at(at, TimerAction::Repeat((at, interval, Box::new(f))))
        }
        #[inline(always)]
        pub(crate) fn schedule_at(
            &self,
            time: u64,
            entry: TimerAction<A>,
        ) -> Result<(), TimerError<TimerAction<A>>> {
            self.inner.write().schedule_at(time, entry)
        }
        #[inline]
        pub(crate) fn advance_to(&mut self, ts: u64) -> Vec<TimerAction<A>> {
            self.inner.write().advance_to(ts)
        }
    }
    #[cfg(feature = "sync")]
    mod inner_impl {
        use super::{Aggregator, TimerAction};
        use crate::rw_wheel::timer::RawTimerWheel;
        use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock};
        use std::sync::Arc;

        /// The lock you get from [`RwLock::read`].
        pub type Ref<'a, T> = MappedRwLockReadGuard<'a, RawTimerWheel<TimerAction<T>>>;
        /// The lock you get from [`RwLock::write`].
        pub type RefMut<'a, T> = MappedRwLockWriteGuard<'a, RawTimerWheel<TimerAction<T>>>;

        /// A TimerWheel backed by interior mutability
        ///
        /// ``RefCell`` for single threded exuections and ``Arc<RwLock<_>>`` with the sync feature enabled
        //#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
        //#[cfg_attr(feature = "serde", serde(bound = "T: Default"))]
        #[derive(Clone)]
        pub struct Inner<T: Aggregator + Clone>(Arc<RwLock<RawTimerWheel<TimerAction<T>>>>);

        impl<T: Aggregator + Clone> Inner<T> {
            #[inline(always)]
            pub fn new(time: u64) -> Self {
                Self(Arc::new(RwLock::new(RawTimerWheel::new(time))))
            }

            #[inline(always)]
            pub fn read(&self) -> Ref<'_, T> {
                parking_lot::RwLockReadGuard::map(self.0.read(), |v| v)
            }

            #[inline(always)]
            pub fn write(&self) -> RefMut<'_, T> {
                parking_lot::RwLockWriteGuard::map(self.0.write(), |v| v)
            }
        }
    }

    #[cfg(not(feature = "sync"))]
    mod inner_impl {
        use super::{Aggregator, TimerAction};
        use crate::rw_wheel::timer::RawTimerWheel;
        #[cfg(not(feature = "std"))]
        use alloc::rc::Rc;
        use core::cell::RefCell;
        #[cfg(feature = "std")]
        use std::rc::Rc;

        /// An immutably borrowed Option<AggregationWheel<_>> from [`RefCell::borrow´]
        pub type Ref<'a, T> = core::cell::Ref<'a, RawTimerWheel<TimerAction<T>>>;
        /// An mutably borrowed Option<AggregationWheel<_>> from [`RefCell::borrow_mut´]
        pub type RefMut<'a, T> = core::cell::RefMut<'a, RawTimerWheel<TimerAction<T>>>;

        /// A TimerWheel backed by interior mutability
        ///
        /// ``RefCell`` for single threded exuections and ``Arc<RwLock<_>>`` with the sync feature enabled
        //#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
        //#[cfg_attr(feature = "serde", serde(bound = "T: Default"))]
        #[derive(Clone)]
        pub struct Inner<T: Aggregator + Clone>(Rc<RefCell<RawTimerWheel<TimerAction<T>>>>);

        impl<T: Aggregator + Clone> Inner<T> {
            #[inline(always)]
            pub fn new(time: u64) -> Self {
                Self(Rc::new(RefCell::new(RawTimerWheel::new(time))))
            }

            #[inline(always)]
            pub fn read(&self) -> Ref<'_, T> {
                self.0.borrow()
            }

            #[inline(always)]
            pub fn write(&self) -> RefMut<'_, T> {
                self.0.borrow_mut()
            }
        }
    }
}
