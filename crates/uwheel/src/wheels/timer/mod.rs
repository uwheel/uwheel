//! This module contains a Hierarchical Wheel Timer implementation
//!
//! It has been taken from the following [crate](https://github.com/Bathtor/rust-hash-wheel-timer/tree/master)
//! and has been modified to support ``no_std``.
//! License: MIT

mod byte_wheel;
mod quad_wheel;
pub(crate) mod raw_wheel;

use crate::{cfg_not_sync, cfg_sync, wheels::read::Haw};
use core::{fmt::Debug, hash::Hash, time::Duration};
pub(super) use raw_wheel::RawTimerWheel;

use crate::{Aggregator, duration};
use core::{fmt, fmt::Display};

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
    pub fn id(&self) -> &I {
        &self.id
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

impl<T: Debug> Display for TimerExpiredError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Attempted to schedule timer entry {:?} at {} when time is {}",
            self.entry, self.scheduled_time, self.current_time
        )
    }
}
#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

pub type WheelFn<A> = Box<dyn Fn(&Haw<A>)>;

pub enum TimerAction<A: Aggregator> {
    Oneshot(WheelFn<A>),
    Repeat((u64, duration::Duration, WheelFn<A>)),
}

// Two Timer Wheel implementations

cfg_not_sync! {
    #[cfg(not(feature = "std"))]
    use alloc::rc::Rc;
    use core::cell::RefCell;
    #[cfg(feature = "std")]
    use std::rc::Rc;

    /// An immutably borrowed Timer from [`RefCell::borrow´]
    pub type TimerRef<'a, T> = core::cell::Ref<'a, RawTimerWheel<TimerAction<T>>>;
    /// A mutably borrowed Timer from [`RefCell::borrow_mut´]
    pub type TimerRefMut<'a, T> = core::cell::RefMut<'a, RawTimerWheel<TimerAction<T>>>;

    /// An timer wheel impl for single-threaded executions
    #[derive(Clone, Default)]
    #[doc(hidden)]
    pub struct TimerWheel<T: Aggregator>(Rc<RefCell<RawTimerWheel<TimerAction<T>>>>);

    impl<T: Aggregator> TimerWheel<T> {
        #[inline(always)]
        pub fn new(val: RawTimerWheel<TimerAction<T>>) -> Self {
            Self(Rc::new(RefCell::new(val)))
        }

        #[inline(always)]
        pub fn read(&self) -> TimerRef<'_, T> {
            self.0.borrow()
        }

        #[inline(always)]
        pub fn write(&self) -> TimerRefMut<'_, T> {
            self.0.borrow_mut()
        }
    }

}

cfg_sync! {
    use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock};
    use std::sync::Arc;

    /// The lock you get from [`RwLock::read`].
    pub type TimerRef<'a, T> = MappedRwLockReadGuard<'a, RawTimerWheel<TimerAction<T>>>;
    /// The lock you get from [`RwLock::write`].
    pub type TimerRefMut<'a, T> = MappedRwLockWriteGuard<'a, RawTimerWheel<TimerAction<T>>>;

    /// An timer impl for multi-reader setups
    #[derive(Clone, Default)]
    #[doc(hidden)]
    pub struct TimerWheel<T: Aggregator>(Arc<RwLock<RawTimerWheel<TimerAction<T>>>>);

    impl<T: Aggregator> TimerWheel<T> {
        #[inline(always)]
        pub fn new(val: RawTimerWheel<TimerAction<T>>) -> Self {
            Self(Arc::new(RwLock::new(val)))
        }

        #[inline(always)]
        pub fn read(&self) -> TimerRef<'_, T> {
            parking_lot::RwLockReadGuard::map(self.0.read(), |v| v)
        }

        #[inline(always)]
        pub fn write(&self) -> TimerRefMut<'_, T> {
            parking_lot::RwLockWriteGuard::map(self.0.write(), |v| v)
        }
    }

}
