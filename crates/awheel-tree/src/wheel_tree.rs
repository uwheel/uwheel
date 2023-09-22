use super::Key;
use awheel_core::{
    aggregator::Aggregator,
    delta::DeltaState,
    rw_wheel::read::{aggregation::combine_or_insert, Eager},
    time,
    ReadWheel,
};
use concurrent_map::{ConcurrentMap, Minimum};
use core::{borrow::Borrow, ops::RangeBounds};

/// A concurrent Wheel-Tree data structure
///
/// It is backed by a lock-free B+tree [implementation](https://github.com/komora-io/concurrent-map)
/// where each key contains a Hierarchical Aggregation Wheel.
#[derive(Clone)]
pub struct WheelTree<K: Key + Minimum, A: Aggregator + Clone>
where
    A::PartialAggregate: Sync,
{
    inner: ConcurrentMap<K, ReadWheel<A, Eager>>,
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
    pub(super) fn new() -> Self {
        Self {
            inner: ConcurrentMap::new(),
        }
    }
    /// Returns the ReadWheel for a given key
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<ReadWheel<A, Eager>>
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
        let mut res: Option<A::PartialAggregate> = None;
        for (_, rw) in self.inner.range(range) {
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
        for (_, rw) in self.inner.range(range) {
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

    /// Merges a set of deltas into a wheel specified by key
    #[inline]
    pub fn merge_delta(&self, key: K, delta: DeltaState<A::PartialAggregate>) {
        if let Some(wheel) = self.get(&key) {
            wheel.delta_advance(delta.deltas);
        } else {
            let wheel = ReadWheel::from_delta_state(delta);
            self.inner.insert(key, wheel);
        }
    }
}

#[cfg(test)]
mod tests {
    use awheel_core::{aggregator::sum::U64SumAggregator, time::NumericalDuration};

    use super::*;

    #[test]
    fn wheel_tree_test() {
        let wheel_tree: WheelTree<usize, U64SumAggregator> = WheelTree::default();

        wheel_tree.merge_delta(1, DeltaState::new(0, vec![Some(10), None, Some(15)]));
        wheel_tree.merge_delta(2, DeltaState::new(0, vec![Some(20), None, Some(1)]));

        assert_eq!(wheel_tree.landmark_range(0..3), Some(46));
        assert_eq!(wheel_tree.interval_range(3.seconds(), 0..3), Some(46));
        assert_eq!(wheel_tree.interval_range(1.seconds(), 0..3), Some(16));
        assert_eq!(wheel_tree.get(&1).unwrap().interval(3.seconds()), Some(25));
        assert_eq!(wheel_tree.get(&1).unwrap().interval(2.seconds()), Some(15));
    }
}
