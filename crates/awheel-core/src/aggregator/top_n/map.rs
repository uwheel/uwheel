use super::{entry::TopNEntry, state::TopNState, KeyBounds};
use crate::Aggregator;
use core::cmp::Reverse;
use hashbrown::HashMap;

#[cfg(not(feature = "std"))]
use alloc::collections::BinaryHeap;
#[cfg(feature = "std")]
use std::collections::BinaryHeap;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone, Debug, Default)]
pub struct TopNMap<Key, A>
where
    Key: KeyBounds,
    A: Aggregator,
    A::PartialAggregate: Ord,
{
    table: HashMap<Key, A::PartialAggregate>,
}

impl<Key, A> TopNMap<Key, A>
where
    Key: KeyBounds,
    A: Aggregator,
    A::PartialAggregate: Ord,
{
    #[inline]
    pub fn insert(&mut self, key: Key, delta: A::PartialAggregate) {
        self.table
            .entry(key)
            .and_modify(|curr_delta| {
                *curr_delta = A::combine(*curr_delta, delta);
            })
            .or_insert(delta);
    }
    pub fn to_state<const N: usize>(mut self) -> TopNState<Key, N, A> {
        let mut top_n_heap = BinaryHeap::with_capacity(N);

        // build top-k from table
        for (key, agg) in self.table.drain() {
            let entry = TopNEntry::new(key, agg);
            if top_n_heap.len() < N {
                top_n_heap.push(Reverse(entry));
            } else if let Some(min_entry) = top_n_heap.peek() {
                if entry > min_entry.0 {
                    let _ = top_n_heap.pop();
                    top_n_heap.push(Reverse(entry));
                }
            }
        }

        // remove reverse
        let top_n_unreversed: BinaryHeap<Option<TopNEntry<Key, A::PartialAggregate>>> =
            top_n_heap.into_iter().map(|m| Some(m.0)).collect();

        let mut sorted_vec = top_n_unreversed.into_sorted_vec();

        // fill remainder if needed...
        let rem = N - sorted_vec.len();
        for _i in 0..rem {
            sorted_vec.push(None);
        }

        TopNState::from(sorted_vec)
    }
}
