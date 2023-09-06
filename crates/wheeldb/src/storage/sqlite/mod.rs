mod connection;

use core::marker::PhantomData;

use awheel::{time::Duration, Aggregator, ReadWheel};
use connection::Connection;
use core::{borrow::Borrow, ops::RangeBounds};

use super::Storage;

#[allow(dead_code)]
pub struct SQLite<K: Ord + PartialEq, A: Aggregator> {
    connection: Connection,
    _marker: PhantomData<(K, A)>,
}

impl<K: Ord + PartialEq, A: Aggregator> Storage<K, A> for SQLite<K, A> {
    #[inline]
    fn insert(&self, _key: K, _wheel: &ReadWheel<A>) {
        unimplemented!();
    }
    fn get<Q>(&self, _key: &Q) -> Option<ReadWheel<A>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        unimplemented!();
    }

    fn landmark_range<Q, R>(&self, _range: R) -> Option<<A as Aggregator>::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        unimplemented!();
    }

    fn interval_range<Q, R>(&self, _interval: Duration, _range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        unimplemented!();
    }
}
