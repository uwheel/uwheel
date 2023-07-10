/// Aggregation Wheel based on a fixed-sized circular buffer
///
/// This is the core data structure that is reused between different hierarchies (e.g., seconds, minutes, hours, days)
pub mod aggregation;
/// Hierarchical Aggregation Wheel (HAW)
pub mod hierarchical;

pub use hierarchical::{
    DaysWheel,
    Haw,
    HoursWheel,
    MinutesWheel,
    Options,
    SecondsWheel,
    WeeksWheel,
    YearsWheel,
    DAYS,
    HOURS,
    MINUTES,
    SECONDS,
    WEEKS,
    YEARS,
};

use crate::{aggregator::Aggregator, time, wheels::rw::write::WriteAheadWheel};

pub use inner_impl::{HawRef, HawRefMut, InnerHaw};

/// A read wheel with hierarchical aggregation wheels backed by interior mutability.
///
/// By default allows a single reader using `RefCell`, and multiple-readers with `sync` flag enabled using `parking_lot`
#[derive(Clone, Debug)]
pub struct ReadWheel<A: Aggregator> {
    inner: InnerHaw<A>,
}
impl<A: Aggregator> ReadWheel<A> {
    /// Creates a new Wheel starting from the given time and with drill down enabled
    ///
    /// Time is represented as milliseconds
    pub fn with_drill_down(time: u64) -> Self {
        let opts = Options::default().with_drill_down();
        Self {
            inner: InnerHaw::new(Haw::with_options(time, opts)),
        }
    }

    /// Creates a new Wheel starting from the given time
    ///
    /// Time is represented as milliseconds
    pub fn new(time: u64) -> Self {
        Self {
            inner: InnerHaw::new(Haw::new(time)),
        }
    }
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

    /// Returns Duration that represents where the wheel currently is in its cycle
    #[inline]
    pub fn current_time_in_cycle(&self) -> time::Duration {
        self.inner.read().current_time_in_cycle()
    }

    /// Advance the watermark of the wheel by the given [time::Duration]
    #[inline]
    pub(crate) fn advance(&self, duration: time::Duration, waw: &mut WriteAheadWheel<A>) {
        self.inner.write().advance(duration, waw);
    }

    /// Advances the time of the wheel aligned by the lowest unit (Second)
    #[inline]
    pub(crate) fn advance_to(&self, watermark: u64, waw: &mut WriteAheadWheel<A>) {
        self.inner.write().advance_to(watermark, waw);
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
    pub fn interval_and_lower(&self, dur: time::Duration) -> Option<A::Aggregate> {
        self.interval(dur).map(|partial| A::lower(partial))
    }

    /// Returns the partial aggregate in the given time interval
    #[inline]
    pub fn interval(&self, dur: time::Duration) -> Option<A::PartialAggregate> {
        self.inner.read().interval(dur)
    }

    /// Executes a Landmark Window that combines total partial aggregates across all wheels
    #[inline]
    pub fn landmark(&self) -> Option<A::PartialAggregate> {
        self.inner.read().landmark()
    }
    pub(crate) fn merge(&self, other: &Self) {
        self.inner.write().merge(&mut other.inner.write());
    }

    /// Raw access to the internal Hierarchical Aggregation Wheel
    pub fn raw(&self) -> HawRef<'_, A> {
        self.inner.read()
    }
}

// Two different Inner Read Wheel implementations below:

#[cfg(feature = "sync")]
mod inner_impl {
    use super::{hierarchical::Haw, Aggregator};
    use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock};
    use std::sync::Arc;

    /// The lock you get from [`RwLock::read`].
    pub type HawRef<'a, T> = MappedRwLockReadGuard<'a, Haw<T>>;
    /// The lock you get from [`RwLock::write`].
    pub type HawRefMut<'a, T> = MappedRwLockWriteGuard<'a, Haw<T>>;

    /// An inner read wheel impl for multi-reader setups
    #[derive(Clone, Debug)]
    pub struct InnerHaw<T: Aggregator>(Arc<RwLock<Haw<T>>>);

    impl<T: Aggregator> InnerHaw<T> {
        #[inline(always)]
        pub fn new(val: Haw<T>) -> Self {
            Self(Arc::new(RwLock::new(val)))
        }

        #[inline(always)]
        pub fn read(&self) -> HawRef<'_, T> {
            parking_lot::RwLockReadGuard::map(self.0.read(), |v| v)
        }

        #[inline(always)]
        pub fn write(&self) -> HawRefMut<'_, T> {
            parking_lot::RwLockWriteGuard::map(self.0.write(), |v| v)
        }
    }
    #[allow(unsafe_code)]
    unsafe impl<T: Aggregator> Send for InnerHaw<T> {}
    #[allow(unsafe_code)]
    unsafe impl<T: Aggregator> Sync for InnerHaw<T> {}
}

#[cfg(not(feature = "sync"))]
mod inner_impl {
    use super::{hierarchical::Haw, Aggregator};
    use core::cell::RefCell;

    /// An immutably borrowed Haw from [`RefCell::borrow´]
    pub type HawRef<'a, T> = core::cell::Ref<'a, Haw<T>>;
    /// A mutably borrowed Haw from [`RefCell::borrow_mut´]
    pub type HawRefMut<'a, T> = core::cell::RefMut<'a, Haw<T>>;

    /// An inner read wheel impl for single-threaded executions
    #[derive(Debug, Clone)]
    pub struct InnerHaw<T: Aggregator>(RefCell<Haw<T>>);

    impl<T: Aggregator> InnerHaw<T> {
        #[inline(always)]
        pub fn new(val: Haw<T>) -> Self {
            Self(RefCell::new(val))
        }

        #[inline(always)]
        pub fn read(&self) -> HawRef<'_, T> {
            self.0.borrow()
        }

        #[inline(always)]
        pub fn write(&self) -> HawRefMut<'_, T> {
            self.0.borrow_mut()
        }
    }
}
