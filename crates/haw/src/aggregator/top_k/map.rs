use super::{entry::TopKEntry, state::TopKState};
use crate::{
    aggregator::{AggState, AllAggregator},
    Aggregator,
};
use core::cmp::Reverse;
use hashbrown::HashMap;

#[cfg(not(feature = "std"))]
use alloc::collections::BinaryHeap;
#[cfg(feature = "std")]
use std::collections::BinaryHeap;

#[derive(Clone, Debug)]
pub struct TopKMap<const KEY_BYTES: usize> {
    table: HashMap<[u8; KEY_BYTES], AggState>,
    aggregator: AllAggregator,
}

impl<const KEY_BYTES: usize> TopKMap<KEY_BYTES> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            table: HashMap::with_capacity(capacity),
            aggregator: AllAggregator,
        }
    }
    #[inline]
    pub fn insert(&mut self, key: [u8; KEY_BYTES], delta: AggState) {
        self.table
            .entry(key)
            .and_modify(|curr_delta| {
                *curr_delta = self.aggregator.combine(*curr_delta, delta);
            })
            .or_insert(delta);
    }
    pub fn to_state<const K: usize>(mut self) -> TopKState<K, KEY_BYTES> {
        let mut top_k_heap = BinaryHeap::with_capacity(K);

        // build top-k from table
        for (key, agg) in self.table.drain() {
            let entry = TopKEntry::new(key, agg);
            if top_k_heap.len() < K {
                top_k_heap.push(Reverse(entry));
            } else if let Some(min_entry) = top_k_heap.peek() {
                if entry > min_entry.0 {
                    let _ = top_k_heap.pop();
                    top_k_heap.push(Reverse(entry));
                }
            }
        }

        // remove reverse
        let top_k_unreversed: BinaryHeap<Option<TopKEntry<KEY_BYTES, AggState>>> =
            top_k_heap.into_iter().map(|m| Some(m.0)).collect();

        let mut sorted_vec = top_k_unreversed.into_sorted_vec();

        // fill remainder if needed...
        let rem = K - sorted_vec.len();
        for _i in 0..rem {
            sorted_vec.push(None);
        }

        TopKState::from(sorted_vec)
    }
}
