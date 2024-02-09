/// Aggregation Wheel based on a fixed-sized circular buffer
///
/// This is the core data structure that is reused between different hierarchies (e.g., seconds, minutes, hours, days)
pub mod aggregation;
/// Hierarchical Aggregation Wheel (HAW)
pub mod hierarchical;

mod plan;

#[cfg(feature = "cache")]
mod cache;

#[cfg(feature = "profiler")]
pub(crate) mod stats;
#[cfg(feature = "timer")]
use crate::rw_wheel::timer::{TimerAction, TimerError};

use crate::{cfg_not_sync, cfg_sync, delta::DeltaState, time_internal::Duration, WriteAheadWheel};
pub use hierarchical::{Haw, DAYS, HOURS, MINUTES, SECONDS, WEEKS, YEARS};

use crate::aggregator::Aggregator;

use self::hierarchical::HawConf;

/// A read wheel with hierarchical aggregation wheels backed by interior mutability.
///
/// By default allows a single reader using `RefCell`, and multiple-readers with the `sync` flag enabled using `parking_lot`
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone)]
pub struct ReadWheel<A, K = Lazy>
where
    A: Aggregator,
    K: Kind,
{
    inner: Inner<A, K>,
}
impl<A, K> ReadWheel<A, K>
where
    A: Aggregator,
    K: Kind,
{
    /// Creates a new Wheel starting from the given time with default configuration
    ///
    /// Time is represented as milliseconds
    pub fn new(time: u64) -> Self {
        Self {
            inner: Inner::new(Haw::new(time, Default::default())),
        }
    }
    /// Creates a new Wheel starting from the given time and configuration
    ///
    /// Time is represented as milliseconds
    pub fn with_conf(time: u64, conf: HawConf) -> Self {
        Self {
            inner: Inner::new(Haw::new(time, conf)),
        }
    }
    /// Creates a new Wheel from a set of deltas
    ///
    /// Time is represented as milliseconds
    pub fn from_delta_state(state: DeltaState<A::PartialAggregate>) -> Self {
        let rw = Self::new(state.oldest_ts);
        rw.delta_advance(state.deltas);
        rw
    }
    /// Returns the number of wheel slots used
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Returns true if the internal wheel time has never been advanced
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Returns true if all slots in the hierarchy are utilised
    pub fn is_full(&self) -> bool {
        self.inner.read().is_full()
    }

    /// Returns how many ticks (seconds) are left until the wheel is fully utilised
    pub fn remaining_ticks(&self) -> u64 {
        self.inner.read().remaining_ticks()
    }
    #[doc(hidden)]
    pub fn set_optimizer_hints(&self, hints: bool) {
        self.inner.write().set_optimizer_hints(hints);
    }

    #[doc(hidden)]
    pub fn convert_all_to_prefix(&self) {
        self.inner.write().convert_all_to_prefix();
    }

    #[doc(hidden)]
    pub fn convert_all_to_array(&self) {
        self.inner.write().convert_all_to_array();
    }

    /// Returns Duration that represents where the wheel currently is in its cycle
    #[inline]
    pub fn current_time_in_cycle(&self) -> Duration {
        self.inner.read().current_time_in_cycle()
    }
    /// Schedules a timer to fire once the given time has been reached
    #[cfg(feature = "timer")]
    pub fn schedule_once(
        &self,
        at: u64,
        f: impl Fn(&Haw<A, K>) + 'static,
    ) -> Result<(), TimerError<TimerAction<A, K>>> {
        self.inner.write().schedule_once(at, f)
    }
    /// Schedules a timer to fire repeatedly
    #[cfg(feature = "timer")]
    pub fn schedule_repeat(
        &self,
        at: u64,
        interval: Duration,
        f: impl Fn(&Haw<A, K>) + 'static,
    ) -> Result<(), TimerError<TimerAction<A, K>>> {
        self.inner.write().schedule_repeat(at, interval, f)
    }

    /// Advance the watermark of the wheel by the given [Duration]
    #[inline]
    #[doc(hidden)]
    pub fn advance(&self, duration: Duration, waw: &mut WriteAheadWheel<A>) {
        self.inner.write().advance(duration, waw);
    }
    #[inline]
    #[doc(hidden)]
    pub fn advance_and_emit_deltas(
        &self,
        duration: Duration,
        waw: &mut WriteAheadWheel<A>,
    ) -> DeltaState<A::PartialAggregate> {
        let current_wm = self.watermark();
        let deltas = self.inner.write().advance_and_emit_deltas(duration, waw);
        DeltaState::new(current_wm, deltas)
    }

    /// Advances the time of the wheel aligned by the lowest unit (Second)
    #[inline]
    pub(crate) fn advance_to(&self, watermark: u64, waw: &mut WriteAheadWheel<A>) {
        self.inner.write().advance_to(watermark, waw);
    }

    /// Advances the wheel by applying a set of deltas, each representing the lowest unit.
    #[inline]
    pub fn delta_advance(&self, deltas: impl IntoIterator<Item = Option<A::PartialAggregate>>) {
        self.inner.write().delta_advance(deltas);
    }

    /// Clears the state of all wheels
    pub fn clear(&self) {
        self.inner.write().clear();
    }

    /// Return the current watermark as milliseconds for this wheel
    #[inline]
    pub fn watermark(&self) -> u64 {
        self.inner.read().watermark()
    }
    /// Returns the aggregate in the given time interval
    pub fn interval_and_lower(&self, dur: Duration) -> Option<A::Aggregate> {
        self.interval(dur).map(|partial| A::lower(partial))
    }

    /// Returns the partial aggregate in the given time interval
    #[inline]
    pub fn interval(&self, dur: Duration) -> Option<A::PartialAggregate> {
        self.inner.read().interval(dur)
    }

    /// Returns the partial aggregate in the given time interval and the number of combine operations
    #[inline]
    pub fn interval_with_ops(&self, dur: Duration) -> (Option<A::PartialAggregate>, usize) {
        self.inner.read().interval_with_stats(dur)
    }

    /// Executes a Landmark Window that combines total partial aggregates across all wheels
    #[inline]
    pub fn landmark(&self) -> Option<A::PartialAggregate> {
        self.inner.read().landmark()
    }
    /// Merges another [ReadWheel] into this one
    pub fn merge(&self, other: &Self) {
        self.inner.write().merge(&mut other.inner.write());
    }
    /// Returns a reference to the internal [Haw] data structure
    pub fn as_ref(&self) -> HawRef<'_, A, K> {
        self.inner.read()
    }
}

// Two different Inner Read Wheel implementations below:

cfg_not_sync! {
    #[cfg(not(feature = "std"))]
    use alloc::rc::Rc;
    use core::cell::RefCell;
    #[cfg(feature = "std")]
    use std::rc::Rc;

    /// An immutably borrowed Haw from [`RefCell::borrow´]
    pub type HawRef<'a, T, K> = core::cell::Ref<'a, Haw<T, K>>;
    /// A mutably borrowed Haw from [`RefCell::borrow_mut´]
    pub type HawRefMut<'a, T, K> = core::cell::RefMut<'a, Haw<T, K>>;

    /// An inner read wheel impl for single-threaded executions
    #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
    #[cfg_attr(feature = "serde", serde(bound = "T: Default"))]
    #[derive(Clone)]
    #[doc(hidden)]
    pub struct Inner<T: Aggregator, K: Kind>(Rc<RefCell<Haw<T, K>>>);

    impl<T: Aggregator, K: Kind> Inner<T, K> {
        #[inline(always)]
        pub fn new(val: Haw<T, K>) -> Self {
            Self(Rc::new(RefCell::new(val)))
        }

        #[inline(always)]
        pub fn read(&self) -> HawRef<'_, T, K> {
            self.0.borrow()
        }

        #[inline(always)]
        pub fn write(&self) -> HawRefMut<'_, T, K> {
            self.0.borrow_mut()
        }
    }

}

cfg_sync! {
    use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock};
    use std::sync::Arc;

    /// The lock you get from [`RwLock::read`].
    pub type HawRef<'a, T, K> = MappedRwLockReadGuard<'a, Haw<T, K>>;
    /// The lock you get from [`RwLock::write`].
    pub type HawRefMut<'a, T, K> = MappedRwLockWriteGuard<'a, Haw<T, K>>;

    /// An inner read wheel impl for multi-reader setups
    #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
    #[cfg_attr(feature = "serde", serde(bound = "T: Default"))]
    #[derive(Clone)]
    #[doc(hidden)]
    pub struct Inner<T: Aggregator, K: Kind>(Arc<RwLock<Haw<T, K>>>);

    impl<T: Aggregator, K: Kind> Inner<T, K> {
        #[inline(always)]
        pub fn new(val: Haw<T, K>) -> Self {
            Self(Arc::new(RwLock::new(val)))
        }

        #[inline(always)]
        pub fn read(&self) -> HawRef<'_, T, K> {
            parking_lot::RwLockReadGuard::map(self.0.read(), |v| v)
        }

        #[inline(always)]
        pub fn write(&self) -> HawRefMut<'_, T, K> {
            parking_lot::RwLockWriteGuard::map(self.0.write(), |v| v)
        }
    }

}

/// Aggregate Mode
#[derive(Clone, Debug, PartialEq, Copy, Default)]
pub enum Mode {
    /// Lazy aggregation
    #[default]
    Lazy,
    /// Eager aggregation
    Eager,
}

/// A Lazy aggregate scheme
#[derive(Clone, Debug, Copy, Default)]
pub struct Lazy;

/// An Eager aggregate scheme
#[derive(Clone, Debug, Copy, Default)]
pub struct Eager;

/// A trait for defining the type of aggregation scheme
pub trait Kind: private::Sealed {
    #[doc(hidden)]
    fn mode() -> Mode;
}

impl Kind for Lazy {
    fn mode() -> Mode {
        Mode::Lazy
    }
}
impl Kind for Eager {
    fn mode() -> Mode {
        Mode::Eager
    }
}

/// Sealed traits
mod private {
    use core::fmt::Debug;

    pub trait Sealed: Clone + Copy + Debug + Default + Send + 'static {}
}

impl private::Sealed for Lazy {}
impl private::Sealed for Eager {}
