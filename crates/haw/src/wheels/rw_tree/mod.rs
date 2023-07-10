use crate::{
    aggregator::Aggregator,
    time,
    wheels::rw::read::aggregation::combine_or_insert,
    Entry,
    Error,
    ReadWheel,
    RwWheel,
};
use core::{borrow::Borrow, hash::Hash, ops::RangeBounds};
use hashbrown::{hash_map::IterMut, HashMap};

use self::inner_impl::InnerTree;

use super::rw::write::WriteAheadWheel;
pub trait Key: PartialEq + Ord + Hash + Eq + Send + Sync + Clone + 'static {}
impl<T> Key for T where T: PartialEq + Ord + Hash + Eq + Send + Sync + Clone + 'static {}

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
    pub fn new() -> Self {
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

/// A Reader-Writer Tree Wheel
pub struct RwTreeWheel<K: Key, A: Aggregator + Clone> {
    star_wheel: RwWheel<A>,
    write: HashMap<K, WriteAheadWheel<A>>,
    read: ReadTreeWheel<K, A>,
}
impl<K: Key, A: Aggregator + Clone + 'static> RwTreeWheel<K, A> {
    pub fn new(watermark: u64) -> Self {
        Self {
            star_wheel: RwWheel::new(watermark),
            write: Default::default(),
            read: ReadTreeWheel::new(),
        }
    }
    #[inline]
    pub fn watermark(&self) -> u64 {
        self.star_wheel.watermark()
    }
    #[inline]
    pub fn star_wheel(&self) -> &ReadWheel<A> {
        self.star_wheel.read()
    }
    #[inline]
    pub fn insert(&mut self, key: K, entry: Entry<A::Input>) -> Result<(), Error<A::Input>> {
        self.star_wheel.write().insert(entry)?;

        let wheel = self
            .write
            .entry(key)
            .or_insert(WriteAheadWheel::with_watermark(
                self.star_wheel.read().watermark(),
            ));

        wheel.insert(entry)?;

        Ok(())
    }
    /// Returns a reference to the underlying ReadTreeWheel
    ///
    /// This wheel may be cloned and shared across threads
    pub fn read(&self) -> &ReadTreeWheel<K, A> {
        &self.read
    }
    /// Advance the watermark of the Tree Wheel by the given [time::Duration]
    #[inline]
    pub fn advance(&mut self, duration: time::Duration) {
        self.star_wheel.advance(duration);
        self.read.advance(duration, self.write.iter_mut())
    }

    /// Advances the time of the Tree wheel aligned by the lowest unit (Second)
    #[inline]
    pub fn advance_to(&mut self, watermark: u64) {
        let diff = watermark.saturating_sub(self.watermark());
        self.advance(time::Duration::milliseconds(diff as i64));
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

    /// An immutably borrowed Haw from [`RefCell::borrow´]
    pub type TreeRef<'a, K, T> = core::cell::Ref<'a, BTreeMap<K, ReadWheel<T>>>;
    /// A mutably borrowed Haw from [`RefCell::borrow_mut´]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{aggregator::U32SumAggregator, time::*};

    #[test]
    fn rw_tree_test() {
        let mut rw_tree: RwTreeWheel<&'static str, U32SumAggregator> = RwTreeWheel::new(0);
        rw_tree.insert("max 1", Entry::new(10, 1000)).unwrap();
        rw_tree.insert("max 2", Entry::new(5, 1000)).unwrap();
        rw_tree.insert("max 3", Entry::new(3, 1000)).unwrap();

        let res = rw_tree
            .read()
            .interval_range(1.seconds(), "max 1"..="max 3");
        assert_eq!(res, None);

        rw_tree.advance(2.seconds());

        let res = rw_tree.read().interval_range(1.seconds(), "max 1".."max 3");
        assert_eq!(res, Some(15u32));

        let res = rw_tree.read().interval_range(1.seconds(), "max 1"..);
        assert_eq!(res, Some(18u32));
    }
    #[test]
    fn rw_tree_test_across_thread() {
        let mut rw_tree: RwTreeWheel<&'static str, U32SumAggregator> = RwTreeWheel::new(0);
        rw_tree.insert("max 1", Entry::new(10, 1000)).unwrap();
        rw_tree.insert("max 2", Entry::new(5, 1000)).unwrap();
        rw_tree.insert("max 3", Entry::new(3, 1000)).unwrap();

        rw_tree.advance(2.seconds());

        let read_wheel = rw_tree.read().clone();

        let handle = std::thread::spawn(move || {
            assert_eq!(
                read_wheel.get("max 1").unwrap().interval(1.seconds()),
                Some(10u32)
            );
            assert_eq!(
                read_wheel.interval_range(1.seconds(), "max 1".."max 3"),
                Some(15u32)
            );
        });

        handle.join().expect("Failed to join the thread.");
    }
}
