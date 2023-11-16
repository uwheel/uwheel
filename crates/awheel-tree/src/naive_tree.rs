use super::Key;
use awheel_core::{
    aggregator::Aggregator,
    delta::DeltaState,
    rw_wheel::read::aggregation::combine_or_insert,
    time_internal as time,
    ReadWheel,
};
use core::{borrow::Borrow, ops::RangeBounds};
use std::collections::BTreeMap;

pub trait Tree<K, A>
where
    K: Key,
    A: Aggregator,
{
    fn landmark_range<Q, R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq;

    fn interval_range<Q, R>(&self, dur: time::Duration, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq;

    fn get<Q>(&self, key: &Q) -> Option<&ReadWheel<A>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq;

    fn merge_delta(&mut self, key: K, delta: DeltaState<A::PartialAggregate>);
}

/// Naivé impl of Wheel Tree using a B-tree that maintains HAW per key.
#[derive(Clone)]
pub struct WheelTree<K: Key, A: Aggregator + Clone> {
    inner: BTreeMap<K, ReadWheel<A>>,
}
impl<K: Key, A: Aggregator + Clone + 'static> Default for WheelTree<K, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Key, A: Aggregator> Tree<K, A> for WheelTree<K, A> {
    /// Returns the ReadWheel for a given key
    #[inline]
    fn get<Q>(&self, key: &Q) -> Option<&ReadWheel<A>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        self.inner.get(key)
    }
    /// Returns the landmark window of all wheels across a range of keys
    #[inline]
    fn landmark_range<Q, R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        self.inner
            .range(range)
            .fold(None, |mut acc, (_, wheel)| match wheel.landmark() {
                Some(landmark) => {
                    combine_or_insert::<A>(&mut acc, landmark);
                    acc
                }
                None => acc,
            })
    }
    // /// Returns the lowered aggregate result of a landmark window across a range of keys
    // #[inline]
    // pub fn landmark_range_and_lower<Q, R>(&self, range: R) -> Option<A::Aggregate>
    // where
    //     R: RangeBounds<Q>,
    //     K: Borrow<Q>,
    //     Q: ?Sized + Ord + PartialEq,
    // {
    //     self.landmark_range(range).map(|partial| A::lower(partial))
    // }
    /// Returns the partial aggregate in the given time interval across a range of keys
    #[inline]
    fn interval_range<Q, R>(&self, dur: time::Duration, range: R) -> Option<A::PartialAggregate>
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
    // /// Returns the aggregate in the given time interval across a range of keys
    // #[inline]
    // pub fn interval_range_and_lower<Q, R>(
    //     &self,
    //     dur: time::Duration,
    //     range: R,
    // ) -> Option<A::Aggregate>
    // where
    //     R: RangeBounds<Q>,
    //     K: Borrow<Q>,
    //     Q: ?Sized + Ord + PartialEq,
    // {
    //     self.interval_range(dur, range)
    //         .map(|partial| A::lower(partial))
    // }

    /// Merges a set of deltas into a wheel specified by key
    #[inline]
    fn merge_delta(&mut self, key: K, delta: DeltaState<A::PartialAggregate>) {
        if let Some(wheel) = self.get(&key) {
            wheel.delta_advance(delta.deltas);
        } else {
            let wheel = ReadWheel::from_delta_state(delta);
            self.inner.insert(key, wheel);
        }
    }
}

impl<K: Key, A: Aggregator> WheelTree<K, A> {
    pub(super) fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }
    // /// Returns the ReadWheel for a given key
    // #[inline]
    // pub fn get<Q>(&self, key: &Q) -> Option<&ReadWheel<A>>
    // where
    //     K: Borrow<Q>,
    //     Q: ?Sized + Ord + PartialEq,
    // {
    //     self.inner.get(key)
    // }
    // /// Returns the landmark window of all wheels across a range of keys
    // #[inline]
    // pub fn landmark_range<Q, R>(&self, range: R) -> Option<A::PartialAggregate>
    // where
    //     R: RangeBounds<Q>,
    //     K: Borrow<Q>,
    //     Q: ?Sized + Ord + PartialEq,
    // {
    //     self.inner
    //         .range(range)
    //         .fold(None, |mut acc, (_, wheel)| match wheel.landmark() {
    //             Some(landmark) => {
    //                 combine_or_insert::<A>(&mut acc, landmark);
    //                 acc
    //             }
    //             None => acc,
    //         })
    // }
    // /// Returns the lowered aggregate result of a landmark window across a range of keys
    // #[inline]
    // pub fn landmark_range_and_lower<Q, R>(&self, range: R) -> Option<A::Aggregate>
    // where
    //     R: RangeBounds<Q>,
    //     K: Borrow<Q>,
    //     Q: ?Sized + Ord + PartialEq,
    // {
    //     self.landmark_range(range).map(|partial| A::lower(partial))
    // }
    // /// Returns the partial aggregate in the given time interval across a range of keys
    // #[inline]
    // pub fn interval_range<Q, R>(&self, dur: time::Duration, range: R) -> Option<A::PartialAggregate>
    // where
    //     R: RangeBounds<Q>,
    //     K: Borrow<Q>,
    //     Q: ?Sized + Ord + PartialEq,
    // {
    //     self.inner
    //         .range(range)
    //         .fold(None, |mut acc, (_, wheel)| match wheel.interval(dur) {
    //             Some(agg) => {
    //                 combine_or_insert::<A>(&mut acc, agg);
    //                 acc
    //             }
    //             None => acc,
    //         })
    // }
    // /// Returns the aggregate in the given time interval across a range of keys
    // #[inline]
    // pub fn interval_range_and_lower<Q, R>(
    //     &self,
    //     dur: time::Duration,
    //     range: R,
    // ) -> Option<A::Aggregate>
    // where
    //     R: RangeBounds<Q>,
    //     K: Borrow<Q>,
    //     Q: ?Sized + Ord + PartialEq,
    // {
    //     self.interval_range(dur, range)
    //         .map(|partial| A::lower(partial))
    // }

    // /// Merges a set of deltas into a wheel specified by key
    // #[inline]
    // pub fn merge_delta(&mut self, key: K, delta: DeltaState<A::PartialAggregate>) {
    //     if let Some(wheel) = self.get(&key) {
    //         wheel.delta_advance(delta.deltas);
    //     } else {
    //         let wheel = ReadWheel::from_delta_state(delta);
    //         self.inner.insert(key, wheel);
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use awheel_core::{aggregator::sum::U64SumAggregator, time::NumericalDuration};

    use super::*;

    #[test]
    fn wheel_tree_test() {
        let mut wheel_tree: WheelTree<usize, U64SumAggregator> = WheelTree::default();

        wheel_tree.merge_delta(1, DeltaState::new(0, vec![Some(10), None, Some(15)]));
        wheel_tree.merge_delta(2, DeltaState::new(0, vec![Some(20), None, Some(1)]));

        assert_eq!(wheel_tree.landmark_range(0..3), Some(46));
        assert_eq!(wheel_tree.interval_range(3.seconds(), 0..3), Some(46));
        assert_eq!(wheel_tree.interval_range(1.seconds(), 0..3), Some(16));
        assert_eq!(wheel_tree.get(&1).unwrap().interval(3.seconds()), Some(25));
        assert_eq!(wheel_tree.get(&1).unwrap().interval(2.seconds()), Some(15));
    }
}
