use self::inner_impl::InnerTree;
use super::Key;
use crate::{
    aggregator::Aggregator,
    time,
    wheels::rw::{read::aggregation::combine_or_insert, write::WriteAheadWheel},
    ReadWheel,
};
use core::{borrow::Borrow, ops::RangeBounds};
use hashbrown::hash_map::IterMut;

/// A Read Tree Wheel holding a ReadWheel per key
///
/// Backed by interior mutability with ``RefCell`` by default for single-threaded executions
/// and Arc<RwLock<_>> with the ``sync`` feature enabled.
#[derive(Clone)]
pub struct ReadTreeWheel<K: Key, A: Aggregator + Clone> {
    inner: InnerTree<K, A>,
}
impl<K: Key, A: Aggregator + Clone + 'static> Default for ReadTreeWheel<K, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Key, A: Aggregator + Clone + 'static> ReadTreeWheel<K, A> {
    pub(super) fn new() -> Self {
        Self {
            inner: InnerTree::new(),
        }
    }
    /// Returns the ReadWheel for a given key
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<ReadWheel<A>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        self.inner.read().get(key).cloned()
    }
    /// Returns the landmark window of all wheels across a range of keys
    #[inline]
    pub fn landmark_range<Q, R>(&self, range: R) -> Option<A::PartialAggregate>
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
    /// Returns the lowered aggregate result of a landmark window across a range of keys
    #[inline]
    pub fn landmark_range_and_lower<Q, R>(&self, range: R) -> Option<A::Aggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        self.landmark_range(range).map(|partial| A::lower(partial))
    }
    /// Returns the partial aggregate in the given time interval across a range of keys
    #[inline]
    pub fn interval_range<Q, R>(&self, dur: time::Duration, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        let mut res: Option<A::PartialAggregate> = None;
        let inner = self.inner.read();
        for (_, rw) in inner.range(range) {
            if let Some(partial_agg) = rw.interval(dur) {
                combine_or_insert::<A>(&mut res, partial_agg);
            }
        }
        res
    }
    /// Returns the aggregate in the given time interval across a range of keys
    #[inline]
    pub fn interval_range_and_lower<Q, R>(
        &self,
        dur: time::Duration,
        range: R,
    ) -> Option<A::Aggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        self.interval_range(dur, range)
            .map(|partial| A::lower(partial))
    }
    /// Advance the watermark of the wheel by the given [time::Duration]
    #[inline]
    pub(crate) fn advance(&self, duration: time::Duration, waws: IterMut<K, WriteAheadWheel<A>>) {
        let mut inner = self.inner.write();
        for (key, waw) in waws {
            match inner.get_mut(key) {
                Some(rw) => {
                    rw.advance(duration, waw);
                }
                None => {
                    let rw = ReadWheel::new(waw.watermark());
                    rw.advance(duration, waw);
                    inner.insert(key.clone(), rw);
                }
            }
        }
    }
}

// Two different Inner Read Wheel implementations below:

#[cfg(feature = "sync")]
mod inner_impl {
    use super::{Aggregator, Key};
    use crate::ReadWheel;
    use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock};
    use std::{collections::BTreeMap, sync::Arc};

    pub type TreeRef<'a, K, T> = MappedRwLockReadGuard<'a, BTreeMap<K, ReadWheel<T>>>;
    pub type TreeRefMut<'a, K, T> = MappedRwLockWriteGuard<'a, BTreeMap<K, ReadWheel<T>>>;

    /// An inner read wheel impl for multi-reader setups
    #[derive(Clone, Debug)]
    pub struct InnerTree<K: Key, T: Aggregator + Clone>(Arc<RwLock<BTreeMap<K, ReadWheel<T>>>>);

    impl<K: Key, T: Aggregator + Clone> InnerTree<K, T> {
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
    #[allow(unsafe_code)]
    unsafe impl<K: Key, T: Aggregator + Clone> Send for InnerTree<K, T> {}
    #[allow(unsafe_code)]
    unsafe impl<K: Key, T: Aggregator + Clone> Sync for InnerTree<K, T> {}
}

#[cfg(not(feature = "sync"))]
mod inner_impl {
    use super::{Aggregator, Key};
    use crate::ReadWheel;
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
    #[derive(Debug, Clone)]
    pub struct InnerTree<K: Key, T: Aggregator + Clone>(RefCell<BTreeMap<K, ReadWheel<T>>>);

    impl<K: Key, T: Aggregator + Clone> InnerTree<K, T> {
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
