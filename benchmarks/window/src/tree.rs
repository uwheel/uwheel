use std::collections::BTreeMap;

use awheel::{
    aggregator::sum::U64SumAggregator,
    rw_wheel::read::aggregation::combine_or_insert,
    Aggregator,
};
use cxx::UniquePtr;
use segment_tree::{ops::Add, SegmentPoint};

pub trait Tree<A: Aggregator>: Default {
    fn insert(&mut self, ts: u64, agg: A::PartialAggregate);
    fn query(&self) -> Option<A::PartialAggregate>;
    fn analyze_query(&self) -> (Option<A::PartialAggregate>, usize);
    fn range_query(&self, from: u64, to: u64) -> Option<A::PartialAggregate>;
    fn analyze_range_query(&self, from: u64, to: u64) -> (Option<A::PartialAggregate>, usize);
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
    fn analyze_range_query(&self, from: u64, to: u64) -> (Option<A::PartialAggregate>, usize) {
        let mut ops = 0;
        let mut res = A::IDENTITY;
        for (_ts, agg) in self.inner.range(from..to) {
            res = A::combine(res, *agg);
            ops += 1;
        }
        (Some(res), ops)
    }
    #[inline]
    fn analyze_query(&self) -> (Option<A::PartialAggregate>, usize) {
        let mut ops = 0;
        let mut res = A::IDENTITY;

        for (_ts, agg) in self.inner.range(..) {
            res = A::combine(res, *agg);
            ops += 1;
        }
        (Some(res), ops)
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
    fn analyze_range_query(&self, from: u64, to: u64) -> (Option<A::PartialAggregate>, usize) {
        let mut ops = 0;
        let mut res = A::IDENTITY;
        for (_ts, agg) in self.range(from..to) {
            res = A::combine(res, *agg);
            ops += 1;
        }
        (Some(res), ops)
    }

    #[inline]
    fn analyze_query(&self) -> (Option<A::PartialAggregate>, usize) {
        let mut ops = 0;
        let mut res = A::IDENTITY;
        for (_ts, agg) in self.range(..) {
            res = A::combine(res, *agg);
            ops += 1;
        }
        (Some(res), ops)
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
    fn analyze_query(&self) -> (Option<u64>, usize) {
        let ops_before = self.combine_ops();
        let result = Some(self.fiba.query());
        let ops_after = self.combine_ops();
        let ops = ops_after - ops_before;
        (result, ops)
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
    fn analyze_range_query(&self, from: u64, to: u64) -> (Option<u64>, usize) {
        let ops_before = self.combine_ops();
        let result = Some(self.fiba.range(from, to - 1));
        let ops_after = self.combine_ops();
        let ops = ops_after - ops_before;
        (result, ops)
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
    fn analyze_query(&self) -> (Option<u64>, usize) {
        let ops_before = self.combine_ops();
        let result = Some(self.fiba.query());
        let ops_after = self.combine_ops();
        let ops = ops_after - ops_before;
        (result, ops)
    }
    #[inline]
    fn range_query(&self, from: u64, to: u64) -> Option<u64> {
        Some(self.fiba.range(from, to - 1))
    }

    #[inline]
    fn analyze_range_query(&self, from: u64, to: u64) -> (Option<u64>, usize) {
        let ops_before = self.combine_ops();
        let result = Some(self.fiba.range(from, to - 1));
        let ops_after = self.combine_ops();
        let ops = ops_after - ops_before;
        (result, ops)
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

pub struct Bclassic2 {
    fiba: UniquePtr<crate::bclassic_two::Bclassic_2>,
}
impl Default for Bclassic2 {
    fn default() -> Self {
        Self {
            fiba: crate::bclassic_two::create_bclassic_2_with_sum(),
        }
    }
}

impl Tree<U64SumAggregator> for Bclassic2 {
    #[inline]
    fn insert(&mut self, ts: u64, agg: u64) {
        self.fiba.pin_mut().insert(&ts, &agg);
    }

    #[inline]
    fn analyze_query(&self) -> (Option<u64>, usize) {
        let ops_before = self.combine_ops();
        let result = Some(self.fiba.query());
        let ops_after = self.combine_ops();
        let ops = ops_after - ops_before;
        (result, ops)
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
    fn analyze_range_query(&self, from: u64, to: u64) -> (Option<u64>, usize) {
        let ops_before = self.combine_ops();
        let result = Some(self.fiba.range(from, to - 1));
        let ops_after = self.combine_ops();
        let ops = ops_after - ops_before;
        (result, ops)
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

pub struct Bclassic4 {
    fiba: UniquePtr<crate::bclassic_four::Bclassic_4>,
}
impl Default for Bclassic4 {
    fn default() -> Self {
        Self {
            fiba: crate::bclassic_four::create_bclassic_4_with_sum(),
        }
    }
}

impl Tree<U64SumAggregator> for Bclassic4 {
    #[inline]
    fn insert(&mut self, ts: u64, agg: u64) {
        self.fiba.pin_mut().insert(&ts, &agg);
    }

    #[inline]
    fn analyze_query(&self) -> (Option<u64>, usize) {
        let ops_before = self.combine_ops();
        let result = Some(self.fiba.query());
        let ops_after = self.combine_ops();
        let ops = ops_after - ops_before;
        (result, ops)
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
    fn analyze_range_query(&self, from: u64, to: u64) -> (Option<u64>, usize) {
        let ops_before = self.combine_ops();
        let result = Some(self.fiba.range(from, to - 1));
        let ops_after = self.combine_ops();
        let ops = ops_after - ops_before;
        (result, ops)
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

pub struct Bclassic8 {
    fiba: UniquePtr<crate::bclassic_eight::Bclassic_8>,
}
impl Default for Bclassic8 {
    fn default() -> Self {
        Self {
            fiba: crate::bclassic_eight::create_bclassic_8_with_sum(),
        }
    }
}

impl Tree<U64SumAggregator> for Bclassic8 {
    #[inline]
    fn insert(&mut self, ts: u64, agg: u64) {
        self.fiba.pin_mut().insert(&ts, &agg);
    }
    #[inline]
    fn query(&self) -> Option<u64> {
        Some(self.fiba.query())
    }

    #[inline]
    fn analyze_query(&self) -> (Option<u64>, usize) {
        let ops_before = self.combine_ops();
        let result = Some(self.fiba.query());
        let ops_after = self.combine_ops();
        let ops = ops_after - ops_before;
        (result, ops)
    }
    #[inline]
    fn range_query(&self, from: u64, to: u64) -> Option<u64> {
        Some(self.fiba.range(from, to - 1))
    }

    #[inline]
    fn analyze_range_query(&self, from: u64, to: u64) -> (Option<u64>, usize) {
        let ops_before = self.combine_ops();
        let result = Some(self.fiba.range(from, to - 1));
        let ops_after = self.combine_ops();
        let ops = ops_after - ops_before;
        (result, ops)
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

#[derive(Default)]
pub struct SegmentTree {
    min_ts: u64,
    // 2n * sizeof(N)
    inner: SegmentPoint<u64, segment_tree::ops::Add>,
}

impl SegmentTree {
    pub fn build(min_ts: u64, values: &[u64]) -> Self {
        Self {
            min_ts: min_ts / 1000, // convert to secs
            inner: SegmentPoint::build(values.to_vec(), Add),
        }
    }
}

impl Tree<U64SumAggregator> for SegmentTree {
    #[inline]
    fn insert(&mut self, _ts: u64, _agg: u64) {}
    #[inline]
    fn query(&self) -> Option<u64> {
        // query the whole SegmentTree
        Some(self.inner.query(0, self.inner.len()).0)
    }

    #[inline]
    fn analyze_query(&self) -> (Option<u64>, usize) {
        let (res, ops) = self.inner.query(0, self.inner.len());
        (Some(res), ops)
    }
    #[inline]
    fn range_query(&self, from: u64, to: u64) -> Option<u64> {
        let from_as_secs = from / 1000;
        let to_as_secs = to / 1000;
        let distance = to_as_secs - from_as_secs;

        let start_pos = from_as_secs - self.min_ts;
        let mut end_pos = start_pos + distance;
        if end_pos == 0 {
            end_pos = self.inner.len() as u64;
        }
        // O(log(len)) time
        let (result, _) = self.inner.query(start_pos as usize, end_pos as usize);
        Some(result)
    }

    #[inline]
    fn analyze_range_query(&self, from: u64, to: u64) -> (Option<u64>, usize) {
        let from_as_secs = from / 1000;
        let to_as_secs = to / 1000;
        let distance = to_as_secs - from_as_secs;

        let start_pos = from_as_secs - self.min_ts;
        let mut end_pos = start_pos + distance;
        if end_pos == 0 {
            end_pos = self.inner.len() as u64;
        }
        // O(log(len)) time
        let (result, ops) = self.inner.query(start_pos as usize, end_pos as usize);
        (Some(result), ops)
    }

    #[inline]
    fn evict_range(&mut self, _to: u64) {}
    fn evict(&mut self) {}
    fn size_bytes(&self) -> usize {
        self.inner.len() * std::mem::size_of::<u64>()
    }
    #[inline]
    fn combine_ops(&self) -> usize {
        0
    }
}
