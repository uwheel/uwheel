use crate::aggregator::Aggregator;

pub use self::inner_impl::AggWheelRef;
use self::inner_impl::{AggWheelRefMut, Inner};
use super::AggregationWheel;
use crate::rw_wheel::WheelExt;

#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

// An internal wrapper Struct that containing a possible AggregationWheel
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Debug, Clone)]
pub struct MaybeWheel<A: Aggregator> {
    slots: usize,
    drill_down: bool,
    inner: Inner<A>,
}
impl<A: Aggregator> MaybeWheel<A> {
    pub fn with_capacity_and_drill_down(slots: usize) -> Self {
        Self {
            slots,
            drill_down: true,
            inner: Inner::new(),
        }
    }
    pub fn with_capacity(slots: usize) -> Self {
        Self {
            slots,
            drill_down: false,
            inner: Inner::new(),
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
            wheel.merge(other.read().as_ref().unwrap());
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
    pub fn size_bytes(&self) -> usize {
        if let Some(inner) = self.inner.read().as_ref() {
            inner.size_bytes()
        } else {
            0
        }
    }
    #[inline(always)]
    pub fn read(&self) -> AggWheelRef<'_, A> {
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
    pub fn get_or_insert(&self) -> AggWheelRefMut<'_, A> {
        let mut inner = self.inner.write();
        if inner.is_none() {
            let agg_wheel = {
                if self.drill_down {
                    AggregationWheel::with_capacity_and_drill_down(self.slots)
                } else {
                    AggregationWheel::with_capacity(self.slots)
                }
            };
            *inner = Some(agg_wheel);
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
    pub type AggWheelRef<'a, T> = MappedRwLockReadGuard<'a, Option<AggregationWheel<T>>>;
    /// The lock you get from [`RwLock::write`].
    pub type AggWheelRefMut<'a, T> = MappedRwLockWriteGuard<'a, Option<AggregationWheel<T>>>;

    /// An AggregationWheel backed by interior mutability
    ///
    /// ``RefCell`` for single threded exuections and ``Arc<RwLock<_>>`` with the sync feature enabled
    #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
    #[cfg_attr(feature = "serde", serde(bound = "T: Default"))]
    #[derive(Clone, Debug)]
    pub struct Inner<T: Aggregator + Clone>(Arc<RwLock<Option<AggregationWheel<T>>>>);

    impl<T: Aggregator + Clone> Inner<T> {
        #[inline(always)]
        pub fn new() -> Self {
            Self(Arc::new(RwLock::new(None)))
        }

        #[inline(always)]
        pub fn read(&self) -> AggWheelRef<'_, T> {
            parking_lot::RwLockReadGuard::map(self.0.read(), |v| v)
        }

        #[inline(always)]
        pub fn write(&self) -> AggWheelRefMut<'_, T> {
            parking_lot::RwLockWriteGuard::map(self.0.write(), |v| v)
        }
    }
    #[allow(unsafe_code)]
    unsafe impl<T: Aggregator + Clone> Send for Inner<T> {}
    #[allow(unsafe_code)]
    unsafe impl<T: Aggregator + Clone> Sync for Inner<T> {}
}

#[cfg(not(feature = "sync"))]
mod inner_impl {
    use super::{AggregationWheel, Aggregator};
    use core::cell::RefCell;

    /// An immutably borrowed Option<AggregationWheel<_>> from [`RefCell::borrow´]
    pub type AggWheelRef<'a, T> = core::cell::Ref<'a, Option<AggregationWheel<T>>>;
    /// An mutably borrowed Option<AggregationWheel<_>> from [`RefCell::borrow_mut´]
    pub type AggWheelRefMut<'a, T> = core::cell::RefMut<'a, Option<AggregationWheel<T>>>;

    /// An AggregationWheel backed by interior mutability
    ///
    /// ``RefCell`` for single threded exuections and ``Arc<RwLock<_>>`` with the sync feature enabled
    #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
    #[cfg_attr(feature = "serde", serde(bound = "T: Default"))]
    #[derive(Clone, Debug)]
    pub struct Inner<T: Aggregator + Clone>(RefCell<Option<AggregationWheel<T>>>);

    impl<T: Aggregator + Clone> Inner<T> {
        #[inline(always)]
        pub fn new() -> Self {
            Self(RefCell::new(None))
        }

        #[inline(always)]
        pub fn read(&self) -> AggWheelRef<'_, T> {
            self.0.borrow()
        }

        #[inline(always)]
        pub fn write(&self) -> AggWheelRefMut<'_, T> {
            self.0.borrow_mut()
        }
    }
}
