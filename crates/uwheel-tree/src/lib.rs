//! uwheel-tree contains the Wheel-Tree index

use core::hash::Hash;
use uwheel::{aggregator::Aggregator, wheels::read::hierarchical::WheelRange, ReaderWheel};

pub trait Key: PartialEq + Ord + Hash + Eq + Send + Sync + Clone + 'static {}
impl<T> Key for T where T: PartialEq + Ord + Hash + Eq + Send + Sync + Clone + 'static {}

use concurrent_map::{ConcurrentMap, Minimum};
use core::{borrow::Borrow, ops::RangeBounds};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    ops::Bound,
};
use uwheel::{wheels::read::aggregation::combine_or_insert, DeltaState, Duration, OffsetDateTime};

pub enum TopKQuery {
    TimeFilter(OffsetDateTime, OffsetDateTime),
    Landmark,
}

/// A concurrent Wheel-Tree data structure
///
/// It is backed by a lock-free B+tree [implementation](https://github.com/komora-io/concurrent-map)
/// where each key contains a Hierarchical Aggregation Wheel.
#[derive(Clone)]
pub struct WheelTree<K: Key + Minimum, A: Aggregator + Clone>
where
    A::PartialAggregate: Sync,
{
    star: ReaderWheel<A>,
    inner: ConcurrentMap<K, ReaderWheel<A>>,
}
impl<K: Key + Minimum, A: Aggregator + Clone + 'static> Default for WheelTree<K, A>
where
    A::PartialAggregate: Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Key + Minimum, A: Aggregator + Clone + 'static> WheelTree<K, A>
where
    A::PartialAggregate: Sync,
{
    pub fn new() -> Self {
        Self {
            star: ReaderWheel::new(0),
            inner: ConcurrentMap::new(),
        }
    }
    /// Returns the ReaderWheel for a given key
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<ReaderWheel<A>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        self.inner.get(key)
    }
    /// Returns the landmark window of all wheels across a range of keys
    #[inline]
    pub fn landmark_range<Q, R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        match (range.start_bound(), range.end_bound()) {
            (Bound::Unbounded, Bound::Unbounded) => self.star.landmark(),
            _ => self
                .inner
                .range(range)
                .fold(None, |mut acc, (_, wheel)| match wheel.landmark() {
                    Some(landmark) => {
                        combine_or_insert::<A>(&mut acc, landmark);
                        acc
                    }
                    None => acc,
                }),
        }
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

    pub fn range_with_time_filter<Q, R>(
        &self,
        range: R,
        start: OffsetDateTime,
        end: OffsetDateTime,
    ) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        match (range.start_bound(), range.end_bound()) {
            (Bound::Unbounded, Bound::Unbounded) => self
                .star
                .as_ref()
                .combine_range(WheelRange::new(start, end)),
            _ => self.inner.range(range).fold(None, |mut acc, (_, wheel)| {
                match wheel.as_ref().combine_range(WheelRange::new(start, end)) {
                    Some(agg) => {
                        combine_or_insert::<A>(&mut acc, agg);
                        acc
                    }
                    None => acc,
                }
            }),
        }
    }

    pub fn range_with_time_filter_smart<Q, R>(
        &self,
        range: R,
        start: OffsetDateTime,
        end: OffsetDateTime,
    ) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        match (range.start_bound(), range.end_bound()) {
            (Bound::Unbounded, Bound::Unbounded) => self
                .star
                .as_ref()
                .combine_range(WheelRange::new(start, end)),
            _ => self.inner.range(range).fold(None, |mut acc, (_, wheel)| {
                match wheel.as_ref().combine_range(WheelRange::new(start, end)) {
                    Some(agg) => {
                        combine_or_insert::<A>(&mut acc, agg);
                        acc
                    }
                    None => acc,
                }
            }),
        }
    }

    /// Returns the amount of memory used
    pub fn memory_usage_bytes(&self) -> usize {
        let keys = self.inner.len();
        let keys_size_bytes = std::mem::size_of::<K>() * keys;

        let mut wheels_size_bytes = 0;
        for (_, wheel) in self.inner.range(..) {
            wheels_size_bytes += wheel.as_ref().size_bytes();
        }
        // add for star wheel as well
        wheels_size_bytes += self.star.as_ref().size_bytes();

        keys_size_bytes + wheels_size_bytes
    }

    /// Executes a Top-K query using a landmark query
    pub fn top_k_landmark(&self, k: usize) -> Vec<(K, A::PartialAggregate)>
    where
        A::PartialAggregate: Ord + PartialEq,
    {
        self.top_k(k, TopKQuery::Landmark)
    }

    fn top_k(&self, k: usize, query: TopKQuery) -> Vec<(K, A::PartialAggregate)>
    where
        A::PartialAggregate: Ord + PartialEq,
    {
        let mut heap = BinaryHeap::with_capacity(k);
        let mut partial_map = HashMap::with_capacity(self.inner.len());

        // for each keyed-wheel run the time filter and collect results
        for (key, wheel) in self.inner.range(..) {
            let agg = match query {
                TopKQuery::TimeFilter(start, end) => {
                    wheel.as_ref().combine_range(WheelRange::new(start, end))
                }
                TopKQuery::Landmark => wheel.as_ref().landmark(),
            };
            partial_map.insert(key.clone(), agg.unwrap_or_default());
        }
        // iterate the partial map and create the top-k output
        for (key, agg) in partial_map {
            if heap.len() < k {
                heap.push((key, agg));
            } else if let Some(top) = heap.peek() {
                if agg.cmp(&top.1) == Ordering::Greater {
                    let _ = heap.pop();
                    heap.push((key, agg));
                }
            }
        }
        let mut top_k: Vec<_> = heap.into_vec();
        top_k.sort_unstable_by(|a, b| a.1.cmp(&b.1));

        // DESC order
        top_k.reverse();

        top_k
    }

    /// Executes a Top-K query with a given time filter
    pub fn top_k_with_time_filter(
        &self,
        k: usize,
        start: OffsetDateTime,
        end: OffsetDateTime,
    ) -> Vec<(K, A::PartialAggregate)>
    where
        A::PartialAggregate: Ord + PartialEq,
    {
        self.top_k(k, TopKQuery::TimeFilter(start, end))
    }

    /// Returns the partial aggregate in the given time interval across a range of keys
    #[inline]
    pub fn interval_range<Q, R>(&self, dur: Duration, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        self.inner
            .range(range)
            .fold(None, |mut acc, (_, wheel)| match wheel.interval(dur) {
                Some(agg) => {
                    combine_or_insert::<A>(&mut acc, agg);
                    acc
                }
                None => acc,
            })
    }
    /// Returns the aggregate in the given time interval across a range of keys
    #[inline]
    pub fn interval_range_and_lower<Q, R>(&self, dur: Duration, range: R) -> Option<A::Aggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        self.interval_range(dur, range)
            .map(|partial| A::lower(partial))
    }

    /// Inserts a wheel into the tree by the given key
    ///
    /// Note that this wheel may be updated from a different thread.
    #[inline]
    pub fn insert(&self, key: K, wheel: ReaderWheel<A>) {
        self.inner.insert(key, wheel);
    }

    pub fn insert_star(&mut self, wheel: ReaderWheel<A>) {
        self.star = wheel;
    }

    /// Merges a set of deltas into a wheel specified by key
    #[inline]
    pub fn merge_delta(&self, key: K, delta: DeltaState<A::PartialAggregate>) {
        if let Some(wheel) = self.get(&key) {
            wheel.delta_advance(delta.deltas);
        } else {
            let wheel = ReaderWheel::from_delta_state(delta);
            self.inner.insert(key, wheel);
        }
    }
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;
    use uwheel::{aggregator::sum::U64SumAggregator, NumericalDuration};

    use super::*;

    #[test]
    fn wheel_tree_test() {
        let wheel_tree: WheelTree<usize, U64SumAggregator> = WheelTree::default();
        let watermark = 1699488000000;

        wheel_tree.merge_delta(
            1,
            DeltaState::new(watermark, vec![Some(10), None, Some(15), None]),
        );
        wheel_tree.merge_delta(
            2,
            DeltaState::new(watermark, vec![Some(20), None, Some(1), None]),
        );

        assert_eq!(wheel_tree.landmark_range(0..3), Some(46));
        assert_eq!(wheel_tree.interval_range(3.seconds(), 0..3), Some(16));
        assert_eq!(wheel_tree.interval_range(1.seconds(), 0..3), Some(0));
        assert_eq!(wheel_tree.get(&1).unwrap().interval(3.seconds()), Some(15));

        let start = datetime!(2023-11-09 00:00:00 UTC);
        let end = datetime!(2023-11-09 00:00:02 UTC);
        assert_eq!(
            wheel_tree.range_with_time_filter(0..=1, start, end),
            Some(10)
        );

        assert_eq!(
            wheel_tree.range_with_time_filter(0..=2, start, end),
            Some(30)
        );

        let start = datetime!(2023-11-09 00:00:00 UTC);
        let end = datetime!(2023-11-09 00:00:04 UTC);

        assert_eq!(
            wheel_tree.range_with_time_filter(0..=1, start, end),
            Some(25)
        );

        assert_eq!(
            wheel_tree.range_with_time_filter(0..=2, start, end),
            Some(25 + 21)
        );
    }
}
