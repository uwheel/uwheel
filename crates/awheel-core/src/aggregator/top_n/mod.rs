use crate::aggregator::Aggregator;
use core::{fmt::Debug, hash::Hash, marker::PhantomData};

mod entry;
pub mod map;
mod state;

pub use map::TopNMap;
pub use state::TopNState;

#[cfg(not(feature = "serde"))]
pub trait KeyBounds: Hash + Debug + Copy + Eq + Default + Send + 'static {}
#[cfg(feature = "serde")]
pub trait KeyBounds:
    Hash
    + Debug
    + Copy
    + Eq
    + Default
    + Send
    + serde::Serialize
    + for<'a> serde::Deserialize<'a>
    + 'static
{
}

#[cfg(not(feature = "serde"))]
impl<T> KeyBounds for T where T: Hash + Debug + Copy + Eq + Default + Send + Clone + 'static {}

#[cfg(feature = "serde")]
impl<T> KeyBounds for T where
    T: Hash
        + Debug
        + Copy
        + Eq
        + Default
        + Send
        + Clone
        + serde::Serialize
        + for<'a> serde::Deserialize<'a>
        + 'static
{
}

#[derive(Default, Debug, Clone, Copy)]
pub struct TopNAggregator<Key: KeyBounds, const N: usize, A: Aggregator>(PhantomData<(Key, A)>);

impl<Key, const N: usize, A> Aggregator for TopNAggregator<Key, N, A>
where
    Key: KeyBounds,
    A: Aggregator + Clone + Copy,
    A::PartialAggregate: Ord + Copy,
{
    type Input = (Key, A::Input);
    type MutablePartialAggregate = TopNMap<Key, A>;
    type PartialAggregate = TopNState<Key, N, A>;
    type Aggregate = TopNState<Key, N, A>;

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
        mutable.to_state()
    }

    #[inline]
    fn combine(mut a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a.merge(b);
        a
    }
    #[inline]
    fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
        a
    }
}
#[cfg(test)]
mod tests {
    use crate::{
        aggregator::{top_n::TopNAggregator, AllAggregator, U64SumAggregator},
        time::NumericalDuration,
        Entry,
        RwWheel,
    };

    use super::state::TopNState;

    #[test]
    fn top_n_all_aggregator_test() {
        let mut wheel: RwWheel<TopNAggregator<u32, 10, AllAggregator>> = RwWheel::new(0);

        wheel
            .write()
            .insert(Entry::new((1u32, 10.0), 1000))
            .unwrap();
        wheel
            .write()
            .insert(Entry::new((2u32, 50.0), 1000))
            .unwrap();
        wheel
            .write()
            .insert(Entry::new((3u32, 30.0), 1000))
            .unwrap();

        wheel.advance_to(2000);

        let state: TopNState<u32, 10, AllAggregator> = wheel.read().interval(1.seconds()).unwrap();
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
            .write()
            .insert(Entry::new((1u32, 100.0), 2000))
            .unwrap();
        wheel
            .write()
            .insert(Entry::new((2u32, 10.0), 2000))
            .unwrap();
        wheel
            .write()
            .insert(Entry::new((3u32, 20.0), 2000))
            .unwrap();

        // insert some new keys
        wheel
            .write()
            .insert(Entry::new((4u32, 15.0), 2000))
            .unwrap();
        wheel.write().insert(Entry::new((5u32, 1.0), 2000)).unwrap();
        wheel
            .write()
            .insert(Entry::new((6u32, 5000.0), 2000))
            .unwrap();

        // advance again
        wheel.advance_to(3000);

        // interval of last 2 seconds should be two TopKState's combined
        let state: TopNState<u32, 10, AllAggregator> = wheel.read().interval(2.seconds()).unwrap();
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
    #[test]
    fn top_n_u64_sum_aggregator_test() {
        let mut wheel: RwWheel<TopNAggregator<u32, 10, U64SumAggregator>> = RwWheel::new(0);

        wheel.write().insert(Entry::new((1u32, 10), 1000)).unwrap();
        wheel.write().insert(Entry::new((2u32, 50), 1000)).unwrap();
        wheel.write().insert(Entry::new((3u32, 30), 1000)).unwrap();

        wheel.advance_to(2000);

        let state: TopNState<u32, 10, U64SumAggregator> =
            wheel.read().interval(1.seconds()).unwrap();
        let arr = state.iter();
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
        wheel.write().insert(Entry::new((1u32, 100), 2000)).unwrap();
        wheel.write().insert(Entry::new((2u32, 10), 2000)).unwrap();
        wheel.write().insert(Entry::new((3u32, 20), 2000)).unwrap();

        // insert some new keys
        wheel.write().insert(Entry::new((4u32, 15), 2000)).unwrap();
        wheel.write().insert(Entry::new((5u32, 1), 2000)).unwrap();
        wheel
            .write()
            .insert(Entry::new((6u32, 5000), 2000))
            .unwrap();

        // advance again
        wheel.advance_to(3000);

        // interval of last 2 seconds should be two TopKState's combined
        let state: TopNState<u32, 10, U64SumAggregator> =
            wheel.read().interval(2.seconds()).unwrap();
        let arr = state.iter();
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
