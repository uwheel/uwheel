use crate::aggregator::Aggregator;

pub use self::inner_impl::AggWheelRef;
use self::inner_impl::{AggWheel, AggWheelRefMut};
use super::AggregationWheel;
use crate::rw_wheel::WheelExt;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

// An internal wrapper Struct that containing a possible AggregationWheel
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Debug, Clone)]
pub struct MaybeWheel<const CAP: usize, A: Aggregator> {
    slots: usize,
    drill_down: bool,
    inner: AggWheel<CAP, A>,
}
impl<const CAP: usize, A: Aggregator> MaybeWheel<CAP, A> {
    pub fn with_capacity_and_drill_down(slots: usize) -> Self {
        Self {
            slots,
            drill_down: true,
            inner: AggWheel::new(),
        }
    }
    pub fn with_capacity(slots: usize) -> Self {
        Self {
            slots,
            drill_down: false,
            inner: AggWheel::new(),
        }
    }
    pub fn clear(&self) {
        if let Some(wheel) = self.inner.write().as_mut() {
            wheel.clear();
        }
    }
    pub fn merge(&self, other: &Self) {
        if let Some(wheel) = self.inner.write().as_mut() {
            // TODO: fix better checks
            wheel.merge(other.read().as_deref().unwrap());
        }
    }
    #[inline]
    pub fn interval(&self, interval: usize) -> Option<A::PartialAggregate> {
        if let Some(wheel) = self.inner.read().as_ref() {
            wheel.interval(interval)
        } else {
            None
        }
    }
    #[inline]
    pub fn interval_or_total(&self, interval: usize) -> Option<A::PartialAggregate> {
        if let Some(wheel) = self.inner.read().as_ref() {
            wheel.interval_or_total(interval)
        } else {
            None
        }
    }
    #[inline]
    pub fn total(&self) -> Option<A::PartialAggregate> {
        if let Some(wheel) = self.inner.read().as_ref() {
            wheel.total()
        } else {
            None
        }
    }
    #[inline(always)]
    pub fn read(&self) -> AggWheelRef<'_, CAP, A> {
        self.inner.read()
    }
    #[inline]
    pub fn rotation_count(&self) -> usize {
        self.inner
            .read()
            .as_ref()
            .map(|w| w.rotation_count())
            .unwrap_or(0)
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.read().as_ref().map(|w| w.len()).unwrap_or(0)
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline]
    pub fn get_or_insert(&self) -> AggWheelRefMut<'_, CAP, A> {
        let mut inner = self.inner.write();
        if inner.is_none() {
            let agg_wheel = {
                if self.drill_down {
                    AggregationWheel::with_capacity_and_drill_down(self.slots)
                } else {
                    AggregationWheel::with_capacity(self.slots)
                }
            };
            *inner = Some(Box::new(agg_wheel));
        }
        inner
    }
}

#[cfg(feature = "sync")]
mod inner_impl {
    use super::{AggregationWheel, Aggregator};
    use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock};
    use std::sync::Arc;

    /// The lock you get from [`RwLock::read`].
    pub type AggWheelRef<'a, const CAP: usize, T> =
        MappedRwLockReadGuard<'a, Option<Box<AggregationWheel<CAP, T>>>>;
    /// The lock you get from [`RwLock::write`].
    pub type AggWheelRefMut<'a, const CAP: usize, T> =
        MappedRwLockWriteGuard<'a, Option<Box<AggregationWheel<CAP, T>>>>;

    /// An AggregationWheel backed by interior mutability
    ///
    /// ``RefCell`` for single threded exuections and ``Arc<RwLock<_>>`` with the sync feature enabled
    #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
    #[cfg_attr(feature = "serde", serde(bound = "T: Default"))]
    #[derive(Clone, Debug)]
    pub struct AggWheel<const CAP: usize, T: Aggregator + Clone>(
        Arc<RwLock<Option<Box<AggregationWheel<CAP, T>>>>>,
    );

    impl<const CAP: usize, T: Aggregator + Clone> AggWheel<CAP, T> {
        #[inline(always)]
        pub fn new() -> Self {
            Self(Arc::new(RwLock::new(None)))
        }

        #[inline(always)]
        pub fn read(&self) -> AggWheelRef<'_, CAP, T> {
            parking_lot::RwLockReadGuard::map(self.0.read(), |v| v)
        }

        #[inline(always)]
        pub fn write(&self) -> AggWheelRefMut<'_, CAP, T> {
            parking_lot::RwLockWriteGuard::map(self.0.write(), |v| v)
        }
    }
    #[allow(unsafe_code)]
    unsafe impl<const CAP: usize, T: Aggregator + Clone> Send for AggWheel<CAP, T> {}
    #[allow(unsafe_code)]
    unsafe impl<const CAP: usize, T: Aggregator + Clone> Sync for AggWheel<CAP, T> {}
}

#[cfg(not(feature = "sync"))]
mod inner_impl {
    use super::{AggregationWheel, Aggregator};
    #[cfg(not(feature = "std"))]
    use alloc::boxed::Box;
    use core::cell::RefCell;

    /// An immutably borrowed Option<Box<AggregationWheel<_>>> from [`RefCell::borrow´]
    pub type AggWheelRef<'a, const CAP: usize, T> =
        core::cell::Ref<'a, Option<Box<AggregationWheel<CAP, T>>>>;
    /// An mutably borrowed Option<Box<AggregationWheel<_>>> from [`RefCell::borrow_mut´]
    pub type AggWheelRefMut<'a, const CAP: usize, T> =
        core::cell::RefMut<'a, Option<Box<AggregationWheel<CAP, T>>>>;

    /// An AggregationWheel backed by interior mutability
    ///
    /// ``RefCell`` for single threded exuections and ``Arc<RwLock<_>>`` with the sync feature enabled
    #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
    #[cfg_attr(feature = "serde", serde(bound = "T: Default"))]
    #[derive(Clone, Debug)]
    pub struct AggWheel<const CAP: usize, T: Aggregator + Clone>(
        RefCell<Option<Box<AggregationWheel<CAP, T>>>>,
    );

    impl<const CAP: usize, T: Aggregator + Clone> AggWheel<CAP, T> {
        #[inline(always)]
        pub fn new() -> Self {
            Self(RefCell::new(None))
        }

        #[inline(always)]
        pub fn read(&self) -> AggWheelRef<'_, CAP, T> {
            self.0.borrow()
        }

        #[inline(always)]
        pub fn write(&self) -> AggWheelRefMut<'_, CAP, T> {
            self.0.borrow_mut()
        }
    }
}
