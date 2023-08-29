/// Aggregation Wheel based on a fixed-sized circular buffer
///
/// This is the core data structure that is reused between different hierarchies (e.g., seconds, minutes, hours, days)
pub mod aggregation;
/// Hierarchical Aggregation Wheel (HAW)
pub mod hierarchical;

#[cfg(feature = "profiler")]
pub(crate) mod stats;

use crate::{time::Duration, WriteAheadWheel};
pub use hierarchical::{Haw, DAYS, HOURS, MINUTES, SECONDS, WEEKS, YEARS};

use crate::aggregator::Aggregator;

pub use inner_impl::{HawRef, HawRefMut, Inner};

/// Aggregate Mode
#[derive(Clone, Debug, Copy, Default)]
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

/// A read wheel with hierarchical aggregation wheels backed by interior mutability.
///
/// By default allows a single reader using `RefCell`, and multiple-readers with the `sync` flag enabled using `parking_lot`
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone, Debug)]
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
    /// Creates a new Wheel starting from the given time and with drill down enabled
    ///
    /// Time is represented as milliseconds
    pub fn with_drill_down(time: u64) -> Self {
        Self {
            inner: Inner::new(Haw::with_drill_down(time)),
        }
    }

    /// Creates a new Wheel starting from the given time
    ///
    /// Time is represented as milliseconds
    pub fn new(time: u64) -> Self {
        Self {
            inner: Inner::new(Haw::new(time)),
        }
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

    /// Returns Duration that represents where the wheel currently is in its cycle
    #[inline]
    pub fn current_time_in_cycle(&self) -> Duration {
        self.inner.read().current_time_in_cycle()
    }

    /// Advance the watermark of the wheel by the given [time::Duration]
    #[inline]
    #[doc(hidden)]
    pub fn advance(&self, duration: Duration, waw: &mut WriteAheadWheel<A>) {
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
    pub fn interval_and_lower(&self, dur: Duration) -> Option<A::Aggregate> {
        self.interval(dur).map(|partial| A::lower(partial))
    }

    /// Returns the partial aggregate in the given time interval
    #[inline]
    pub fn interval(&self, dur: Duration) -> Option<A::PartialAggregate> {
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
    /// Returns a reference to the internal [Haw] data structure
    pub fn as_ref(&self) -> HawRef<'_, A, K> {
        self.inner.read()
    }
}

// Two different Inner Read Wheel implementations below:

#[cfg(feature = "sync")]
mod inner_impl {
    use super::{hierarchical::Haw, Aggregator, Kind};
    use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock};
    use std::sync::Arc;

    /// The lock you get from [`RwLock::read`].
    pub type HawRef<'a, T, K> = MappedRwLockReadGuard<'a, Haw<T, K>>;
    /// The lock you get from [`RwLock::write`].
    pub type HawRefMut<'a, T, K> = MappedRwLockWriteGuard<'a, Haw<T, K>>;

    /// An inner read wheel impl for multi-reader setups
    #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
    #[cfg_attr(feature = "serde", serde(bound = "T: Default"))]
    #[derive(Clone, Debug)]
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

#[cfg(not(feature = "sync"))]
mod inner_impl {
    use super::{hierarchical::Haw, Aggregator, Kind};
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
    #[derive(Debug, Clone)]
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
