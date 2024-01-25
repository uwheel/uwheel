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
    fn size_bytes(&self) -> usize;
    fn combine_ops(&self) -> usize;
}

#[derive(Default)]
pub struct BTree<A: Aggregator> {
    inner: BTreeMap<u64, A::PartialAggregate>,
}
impl<A: Aggregator> Tree<A> for BTree<A> {
    #[inline]
    fn insert(&mut self, ts: u64, agg: A::PartialAggregate) {
        self.inner
            .entry(ts)
            .and_modify(|f| {
                *f = A::combine(*f, agg);
            })
            .or_insert(agg);
    }
    #[inline]
    fn range_query(&self, from: u64, to: u64) -> Option<A::PartialAggregate> {
        let mut res: Option<A::PartialAggregate> = None;
        for (_ts, agg) in self.inner.range(from..to) {
            combine_or_insert::<A>(&mut res, *agg);
        }
        res
    }
    #[inline]
    fn query(&self) -> Option<A::PartialAggregate> {
        let mut res: Option<A::PartialAggregate> = None;
        for (_ts, agg) in self.inner.range(..) {
            combine_or_insert::<A>(&mut res, *agg);
        }
        res
    }
    #[inline]
    fn evict_range(&mut self, to: u64) {
        let mut split_tree = self.inner.split_off(&(to - 1));
        std::mem::swap(&mut split_tree, &mut self.inner);
    }
    fn evict(&mut self) {
        let _ = self.inner.pop_first();
    }
    fn size_bytes(&self) -> usize {
        use std::mem::size_of;
        // Estimate the memory usage of the BTreeMap itself
        let btree_map_size = size_of::<Self>();

        // Estimate the memory usage of each key-value pair
        let key_value_size = size_of::<u64>() + size_of::<u64>();

        // Calculate the total memory usage estimation
        btree_map_size + key_value_size * self.inner.len()
    }
    fn combine_ops(&self) -> usize {
        unimplemented!();
    }
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
    fn size_bytes(&self) -> usize {
        use std::mem::size_of;
        // Estimate the memory usage of the BTreeMap itself
        let btree_map_size = size_of::<Self>();

        // Estimate the memory usage of each key-value pair
        let key_value_size = size_of::<u64>() + size_of::<u64>();

        // Calculate the total memory usage estimation
        btree_map_size + key_value_size * self.len()
    }
    fn combine_ops(&self) -> usize {
        unimplemented!();
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
    fn size_bytes(&self) -> usize {
        self.fiba.memory_usage()
    }
    #[inline]
    fn combine_ops(&self) -> usize {
        self.fiba.combine_operations()
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
    fn size_bytes(&self) -> usize {
        self.fiba.memory_usage()
    }
    #[inline]
    fn combine_ops(&self) -> usize {
        self.fiba.combine_operations()
    }
}
