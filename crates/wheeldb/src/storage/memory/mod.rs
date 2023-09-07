use awheel::{
    rw_wheel::read::aggregation::combine_or_insert,
    time::Duration,
    Aggregator,
    Entry,
    ReadWheel,
};
use core::{borrow::Borrow, ops::RangeBounds};

use super::Storage;
use inner_impl::Inner;

#[derive(Clone)]
pub struct MemoryStorage<K: Ord + PartialEq, A: Aggregator> {
    inner: Inner<K, A>,
}

impl<K: Ord + PartialEq, A: Aggregator> Default for MemoryStorage<K, A> {
    fn default() -> Self {
        Self {
            inner: Inner::new(),
        }
    }
}

impl<K: Ord + PartialEq, A: Aggregator> Storage<K, A> for MemoryStorage<K, A> {
    #[inline]
    fn insert_wal(&self, _entry: &Entry<A::Input>) {
        // ignore as this is memory only storage
    }
    #[inline]
    fn add_wheel(&self, key: K, wheel: &ReadWheel<A>) {
        self.inner.write().insert(key, wheel.clone());
    }
    fn get<Q>(&self, key: &Q) -> Option<ReadWheel<A>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        self.inner.read().get(key).cloned()
    }

    fn landmark_range<Q, R>(&self, range: R) -> Option<<A as Aggregator>::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        let mut res: Option<A::PartialAggregate> = None;
        let inner = self.inner.read();
        for (_, rw) in inner.range(range) {
            if let Some(landmark) = rw.landmark() {
                combine_or_insert::<A>(&mut res, landmark);
            }
        }
        res
    }

    fn interval_range<Q, R>(&self, interval: Duration, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        let mut res: Option<A::PartialAggregate> = None;
        let inner = self.inner.read();
        for (_, rw) in inner.range(range) {
            if let Some(partial_agg) = rw.interval(interval) {
                combine_or_insert::<A>(&mut res, partial_agg);
            }
        }
        res
    }
}
#[cfg(feature = "sync")]
mod inner_impl {
    use super::*;
    use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock};
    use std::{collections::BTreeMap, sync::Arc};

    pub type TreeRef<'a, K, T> = MappedRwLockReadGuard<'a, BTreeMap<K, ReadWheel<T>>>;
    pub type TreeRefMut<'a, K, T> = MappedRwLockWriteGuard<'a, BTreeMap<K, ReadWheel<T>>>;

    /// An inner read wheel impl for multi-reader setups
    #[derive(Clone)]
    pub struct Inner<K: Ord + PartialEq, T: Aggregator>(Arc<RwLock<BTreeMap<K, ReadWheel<T>>>>);

    impl<K: Ord + PartialEq, T: Aggregator> Inner<K, T> {
        #[inline(always)]
        pub fn new() -> Self {
            Self(Arc::new(RwLock::new(Default::default())))
        }

        #[inline(always)]
        pub fn read(&self) -> TreeRef<'_, K, T> {
            parking_lot::RwLockReadGuard::map(self.0.read(), |v| v)
        }

        #[inline(always)]
        pub fn write(&self) -> TreeRefMut<'_, K, T> {
            parking_lot::RwLockWriteGuard::map(self.0.write(), |v| v)
        }
    }
}

#[cfg(not(feature = "sync"))]
mod inner_impl {
    use super::*;
    #[cfg(not(feature = "std"))]
    use alloc::collections::BTreeMap;
    use core::cell::RefCell;
    #[cfg(feature = "std")]
    use std::collections::BTreeMap;

    /// An immutably borrowed Tree from [`RefCell::borrow´]
    pub type TreeRef<'a, K, T> = core::cell::Ref<'a, BTreeMap<K, ReadWheel<T>>>;
    /// A mutably borrowed Tree from [`RefCell::borrow_mut´]
    pub type TreeRefMut<'a, K, T> = core::cell::RefMut<'a, BTreeMap<K, ReadWheel<T>>>;

    /// An inner read wheel impl for single-threaded executions
    #[derive(Clone)]
    pub struct Inner<K: Ord + PartialEq, T: Aggregator>(RefCell<BTreeMap<K, ReadWheel<T>>>);

    impl<K: Ord + PartialEq, T: Aggregator> Inner<K, T> {
        #[inline(always)]
        pub fn new() -> Self {
            Self(RefCell::new(Default::default()))
        }

        #[inline(always)]
        pub fn read(&self) -> TreeRef<'_, K, T> {
            self.0.borrow()
        }

        #[inline(always)]
        pub fn write(&self) -> TreeRefMut<'_, K, T> {
            self.0.borrow_mut()
        }
    }
}
