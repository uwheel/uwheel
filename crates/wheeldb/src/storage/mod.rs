use core::{borrow::Borrow, ops::RangeBounds};

use awheel::{time::Duration, Aggregator, ReadWheel};

pub mod memory;
#[allow(dead_code)]
pub mod sqlite;

pub trait Storage<K, A: Aggregator> {
    fn insert(&self, key: K, wheel: &ReadWheel<A>);

    fn get<Q>(&self, key: &Q) -> Option<ReadWheel<A>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq;

    fn landmark_range<Q, R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq;

    fn interval_range<Q, R>(&self, interval: Duration, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq;
}
