use super::Key;
use awheel_core::{
    aggregator::Aggregator,
    delta::DeltaState,
    rw_wheel::read::aggregation::combine_or_insert,
    time_internal::Duration,
    OffsetDateTime,
    ReadWheel,
};
use concurrent_map::{ConcurrentMap, Minimum};
use core::{borrow::Borrow, ops::RangeBounds};
use std::ops::Bound;

/// A concurrent Wheel-Tree data structure
///
/// It is backed by a lock-free B+tree [implementation](https://github.com/komora-io/concurrent-map)
/// where each key contains a Hierarchical Aggregation Wheel.
#[derive(Clone)]
pub struct WheelTree<K: Key + Minimum, A: Aggregator + Clone>
where
    A::PartialAggregate: Sync,
{
    star: ReadWheel<A>,
    inner: ConcurrentMap<K, ReadWheel<A>>,
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
            star: ReadWheel::new(0),
            inner: ConcurrentMap::new(),
        }
    }
    /// Returns the ReadWheel for a given key
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<ReadWheel<A>>
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
            (Bound::Unbounded, Bound::Unbounded) => self.star.as_ref().combine_range(start, end),
            _ => self.inner.range(range).fold(None, |mut acc, (_, wheel)| {
                match wheel.as_ref().combine_range(start, end) {
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
            (Bound::Unbounded, Bound::Unbounded) => {
                self.star.as_ref().combine_range_smart(start, end)
            }
            _ => self.inner.range(range).fold(None, |mut acc, (_, wheel)| {
                match wheel.as_ref().combine_range_smart(start, end) {
                    Some(agg) => {
                        combine_or_insert::<A>(&mut acc, agg);
                        acc
                    }
                    None => acc,
                }
            }),
        }
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
    pub fn insert(&self, key: K, wheel: ReadWheel<A>) {
        self.inner.insert(key, wheel);
    }

    pub fn insert_star(&mut self, wheel: ReadWheel<A>) {
        self.star = wheel;
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
    use awheel_core::{
        aggregator::sum::U64SumAggregator,
        datetime,
        time_internal::NumericalDuration,
    };

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
