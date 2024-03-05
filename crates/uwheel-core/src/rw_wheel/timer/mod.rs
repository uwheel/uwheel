//! This module contains a Hierarchical Wheel Timer implementation

mod byte_wheel;
mod quad_wheel;
pub(crate) mod raw_wheel;

use crate::rw_wheel::read::Haw;
use core::{fmt::Debug, hash::Hash, time::Duration};
pub(super) use raw_wheel::RawTimerWheel;

use crate::{time_internal, Aggregator};
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
    Repeat((u64, time_internal::Duration, WheelFn<A>)),
}
