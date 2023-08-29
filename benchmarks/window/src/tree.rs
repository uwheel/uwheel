use std::collections::BTreeMap;

use awheel::{
    aggregator::sum::U64SumAggregator,
    rw_wheel::read::aggregation::combine_or_insert,
    Aggregator,
};
use cxx::UniquePtr;

pub trait Tree<A: Aggregator>: Default {
    fn insert(&mut self, ts: u64, agg: A::PartialAggregate);
    fn query(&self) -> Option<A::PartialAggregate>;
    fn range_query(&self, from: u64, to: u64) -> Option<A::PartialAggregate>;
    fn evict_range(&mut self, to: u64);
    fn evict(&mut self);
}

impl<A: Aggregator> Tree<A> for BTreeMap<u64, A::PartialAggregate> {
    #[inline]
    fn insert(&mut self, ts: u64, agg: A::PartialAggregate) {
        self.entry(ts)
            .and_modify(|f| {
                *f = A::combine(*f, agg);
            })
            .or_insert(agg);
    }
    #[inline]
    fn range_query(&self, from: u64, to: u64) -> Option<A::PartialAggregate> {
        let mut res: Option<A::PartialAggregate> = None;
        for (_ts, agg) in self.range(from..to) {
            combine_or_insert::<A>(&mut res, *agg);
        }
        res
    }
    #[inline]
    fn query(&self) -> Option<A::PartialAggregate> {
        let mut res: Option<A::PartialAggregate> = None;
        for (_ts, agg) in self.range(..) {
            combine_or_insert::<A>(&mut res, *agg);
        }
        res
    }
    #[inline]
    fn evict_range(&mut self, to: u64) {
        let mut split_tree = self.split_off(&(to - 1));
        std::mem::swap(&mut split_tree, self);
    }
    fn evict(&mut self) {
        let _ = self.pop_first();
    }
}
pub struct FiBA4 {
    fiba: UniquePtr<crate::bfinger_four::FiBA_SUM_4>,
}
impl Default for FiBA4 {
    fn default() -> Self {
        Self {
            fiba: crate::bfinger_four::create_fiba_4_with_sum(),
        }
    }
}

impl Tree<U64SumAggregator> for FiBA4 {
    #[inline]
    fn insert(&mut self, ts: u64, agg: u64) {
        self.fiba.pin_mut().insert(&ts, &agg);
    }
    #[inline]
    fn query(&self) -> Option<u64> {
        Some(self.fiba.query())
    }
    #[inline]
    fn range_query(&self, from: u64, to: u64) -> Option<u64> {
        Some(self.fiba.range(from, to - 1))
    }
    #[inline]
    fn evict_range(&mut self, to: u64) {
        self.fiba.pin_mut().bulk_evict(&(to - 1));
    }
    fn evict(&mut self) {
        self.fiba.pin_mut().evict();
    }
}

pub struct FiBA8 {
    fiba: UniquePtr<crate::bfinger_eight::FiBA_SUM_8>,
}
impl Default for FiBA8 {
    fn default() -> Self {
        Self {
            fiba: crate::bfinger_eight::create_fiba_8_with_sum(),
        }
    }
}

impl Tree<U64SumAggregator> for FiBA8 {
    #[inline]
    fn insert(&mut self, ts: u64, agg: u64) {
        self.fiba.pin_mut().insert(&ts, &agg);
    }
    #[inline]
    fn query(&self) -> Option<u64> {
        Some(self.fiba.query())
    }
    #[inline]
    fn range_query(&self, from: u64, to: u64) -> Option<u64> {
        Some(self.fiba.range(from, to - 1))
    }
    #[inline]
    fn evict_range(&mut self, to: u64) {
        self.fiba.pin_mut().bulk_evict(&(to - 1));
    }
    fn evict(&mut self) {
        self.fiba.pin_mut().evict();
    }
}
