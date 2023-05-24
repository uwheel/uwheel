use crate::aggregator::Aggregator;
use core::fmt::Debug;

mod entry;
pub mod map;
mod state;

pub use map::TopKMap;
pub use state::TopKState;

use super::AggState;

#[derive(Default, Debug, Clone, Copy)]
pub struct TopKAggregator<const K: usize, const KEY_BYTES: usize>;

impl<const K: usize, const KEY_BYTES: usize> Aggregator for TopKAggregator<K, KEY_BYTES> {
    type Input = ([u8; KEY_BYTES], f64);
    type Window = TopKMap<KEY_BYTES>;
    type PartialAggregate = TopKState<K, KEY_BYTES>;
    type Aggregate = TopKState<K, KEY_BYTES>;

    fn insert(&self, window: &mut Self::Window, input: Self::Input) {
        let agg_state = AggState::new(input.1);
        window.insert(input.0, agg_state);
    }
    fn lift(&self, window: Self::Window) -> Self::PartialAggregate {
        window.to_state()
    }
    #[inline]
    fn init_window(&self, input: Self::Input) -> Self::Window {
        let mut window = TopKMap::with_capacity(1024);
        self.insert(&mut window, input);
        window
    }

    #[inline]
    fn combine(
        &self,
        mut a: Self::PartialAggregate,
        b: Self::PartialAggregate,
    ) -> Self::PartialAggregate {
        a.merge(b);
        a
    }
    #[inline]
    fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate {
        a
    }
}
#[cfg(test)]
mod tests {
    use crate::{aggregator::top_k::TopKAggregator, Entry, Wheel};

    use super::state::TopKState;

    #[test]
    fn top_k_test() {
        let mut wheel: Wheel<TopKAggregator<10, 4>> = Wheel::new(0);

        wheel
            .insert(Entry::new((1u32.to_le_bytes(), 10.0), 1000))
            .unwrap();
        wheel
            .insert(Entry::new((2u32.to_le_bytes(), 50.0), 1000))
            .unwrap();
        wheel
            .insert(Entry::new((3u32.to_le_bytes(), 30.0), 1000))
            .unwrap();

        wheel.advance_to(2000);

        let state: TopKState<10, 4> = wheel.seconds_unchecked().interval(1).unwrap();
        let arr = state.iter();
        assert_eq!(arr[0].unwrap().data.sum(), 10.0);
        assert_eq!(arr[1].unwrap().data.sum(), 30.0);
        assert_eq!(arr[2].unwrap().data.sum(), 50.0);
        assert!(arr[3].is_none());
        assert!(arr[4].is_none());
        assert!(arr[5].is_none());
        assert!(arr[6].is_none());
        assert!(arr[7].is_none());
        assert!(arr[8].is_none());
        assert!(arr[9].is_none());

        // insert same keys with different values at different timestamp
        wheel
            .insert(Entry::new((1u32.to_le_bytes(), 100.0), 2000))
            .unwrap();
        wheel
            .insert(Entry::new((2u32.to_le_bytes(), 10.0), 2000))
            .unwrap();
        wheel
            .insert(Entry::new((3u32.to_le_bytes(), 20.0), 2000))
            .unwrap();

        // insert some new keys
        wheel
            .insert(Entry::new((4u32.to_le_bytes(), 15.0), 2000))
            .unwrap();
        wheel
            .insert(Entry::new((5u32.to_le_bytes(), 1.0), 2000))
            .unwrap();
        wheel
            .insert(Entry::new((6u32.to_le_bytes(), 5000.0), 2000))
            .unwrap();

        // advance again
        wheel.advance_to(3000);

        // interval of last 2 seconds should be two TopKState's combined
        let state: TopKState<10, 4> = wheel.seconds_unchecked().interval(2).unwrap();
        let arr = state.iter();
        assert_eq!(arr[0].unwrap().data.sum(), 1.0);
        assert_eq!(arr[1].unwrap().data.sum(), 15.0);
        assert_eq!(arr[2].unwrap().data.sum(), 50.0);
        assert_eq!(arr[3].unwrap().data.sum(), 60.0);
        assert_eq!(arr[4].unwrap().data.sum(), 110.0);
        assert_eq!(arr[5].unwrap().data.sum(), 5000.0);
        assert!(arr[6].is_none());
        assert!(arr[7].is_none());
        assert!(arr[8].is_none());
        assert!(arr[9].is_none());
    }
}
