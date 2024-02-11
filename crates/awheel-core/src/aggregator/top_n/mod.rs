use crate::{aggregator::Aggregator, rw_wheel::read::hierarchical::CombineHint};
use core::{fmt::Debug, marker::PhantomData};

mod entry;
mod key;
mod map;
mod order;
mod state;

pub use key::KeyBounds;
pub use map::TopNMap;
pub use order::{Ascending, Descending, Order};
pub use state::TopNState;

/// A Top-N Aggregator
///
/// Orders data in ascending order by default
#[derive(Debug, Clone, Copy)]
pub struct TopNAggregator<Key: KeyBounds, const N: usize, A: Aggregator, OrderBy = Ascending>(
    PhantomData<(Key, OrderBy, A)>,
);
impl<Key, const N: usize, A, OrderBy> Default for TopNAggregator<Key, N, A, OrderBy>
where
    Key: KeyBounds,
    OrderBy: Order,
    A: Aggregator + Clone + Copy,
    A::PartialAggregate: Ord + Copy,
{
    // have to implement manually as Key does not implement Default
    fn default() -> Self {
        Self(PhantomData)
    }
}
impl<Key, const N: usize, A, OrderBy> Aggregator for TopNAggregator<Key, N, A, OrderBy>
where
    Key: KeyBounds,
    OrderBy: Order,
    A: Aggregator + Clone + Copy,
    A::PartialAggregate: Ord + Copy,
{
    const IDENTITY: Self::PartialAggregate = TopNState::identity();

    type Input = (Key, A::Input);
    type MutablePartialAggregate = TopNMap<Key, A>;
    type PartialAggregate = TopNState<Key, N, A>;
    type Aggregate = TopNState<Key, N, A>;

    type CombineSimd = fn(&[Self::PartialAggregate]) -> Self::PartialAggregate;
    type CombineInverse =
        fn(Self::PartialAggregate, Self::PartialAggregate) -> Self::PartialAggregate;

    #[inline]
    fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
        let mut map = TopNMap::default();
        Self::combine_mutable(&mut map, input);
        map
    }
    #[inline]
    fn combine_mutable(map: &mut Self::MutablePartialAggregate, input: Self::Input) {
        let inner_mutable = A::lift(input.1);
        map.insert(input.0, A::freeze(inner_mutable));
    }
    fn freeze(mutable: Self::MutablePartialAggregate) -> Self::PartialAggregate {
        mutable.build(OrderBy::ordering())
    }

    #[inline]
    fn combine(mut a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a.merge(b, OrderBy::ordering());
        a
    }
    #[inline]
    fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
        a
    }
    // hint that this top-n combine operation is expensive
    fn combine_hint() -> Option<CombineHint> {
        Some(CombineHint::Expensive)
    }
}
#[cfg(test)]
mod tests {
    use crate::{
        aggregator::{all::AllAggregator, sum::U64SumAggregator, top_n::TopNAggregator},
        time_internal::NumericalDuration,
        Entry,
        RwWheel,
    };

    use super::state::TopNState;

    #[test]
    fn top_n_all_aggregator_test() {
        let mut wheel: RwWheel<TopNAggregator<u32, 10, AllAggregator>> = RwWheel::new(0);

        wheel.insert(Entry::new((1u32, 10.0), 1000));
        wheel.insert(Entry::new((2u32, 50.0), 1000));
        wheel.insert(Entry::new((3u32, 30.0), 1000));

        wheel.advance_to(2000);

        let state: TopNState<u32, 10, AllAggregator> = wheel.read().interval(1.seconds()).unwrap();
        let arr = state.as_ref();
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
        wheel.insert(Entry::new((1u32, 100.0), 2000));
        wheel.insert(Entry::new((2u32, 10.0), 2000));
        wheel.insert(Entry::new((3u32, 20.0), 2000));

        // insert some new keys
        wheel.insert(Entry::new((4u32, 15.0), 2000));
        wheel.insert(Entry::new((5u32, 1.0), 2000));
        wheel.insert(Entry::new((6u32, 5000.0), 2000));

        // advance again
        wheel.advance_to(3000);

        // interval of last 2 seconds should be two TopKState's combined
        let state: TopNState<u32, 10, AllAggregator> = wheel.read().interval(2.seconds()).unwrap();
        let arr = state.as_ref();
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
    #[test]
    fn top_n_u64_sum_aggregator_test() {
        let mut wheel: RwWheel<TopNAggregator<u32, 10, U64SumAggregator>> = RwWheel::new(0);

        wheel.insert(Entry::new((1u32, 10), 1000));
        wheel.insert(Entry::new((2u32, 50), 1000));
        wheel.insert(Entry::new((3u32, 30), 1000));

        wheel.advance_to(2000);

        let state: TopNState<u32, 10, U64SumAggregator> =
            wheel.read().interval(1.seconds()).unwrap();
        let arr = state.as_ref();
        assert_eq!(arr[0].unwrap().data, 10);
        assert_eq!(arr[1].unwrap().data, 30);
        assert_eq!(arr[2].unwrap().data, 50);
        assert!(arr[3].is_none());
        assert!(arr[4].is_none());
        assert!(arr[5].is_none());
        assert!(arr[6].is_none());
        assert!(arr[7].is_none());
        assert!(arr[8].is_none());
        assert!(arr[9].is_none());

        // insert same keys with different values at different timestamp
        wheel.insert(Entry::new((1u32, 100), 2000));
        wheel.insert(Entry::new((2u32, 10), 2000));
        wheel.insert(Entry::new((3u32, 20), 2000));

        // insert some new keys
        wheel.insert(Entry::new((4u32, 15), 2000));
        wheel.insert(Entry::new((5u32, 1), 2000));
        wheel.insert(Entry::new((6u32, 5000), 2000));

        // advance again
        wheel.advance_to(3000);

        // interval of last 2 seconds should be two TopKState's combined
        let state: TopNState<u32, 10, U64SumAggregator> =
            wheel.read().interval(2.seconds()).unwrap();
        let arr = state.as_ref();
        assert_eq!(arr[0].unwrap().data, 1);
        assert_eq!(arr[1].unwrap().data, 15);
        assert_eq!(arr[2].unwrap().data, 50);
        assert_eq!(arr[3].unwrap().data, 60);
        assert_eq!(arr[4].unwrap().data, 110);
        assert_eq!(arr[5].unwrap().data, 5000);
        assert!(arr[6].is_none());
        assert!(arr[7].is_none());
        assert!(arr[8].is_none());
        assert!(arr[9].is_none());
    }
}
