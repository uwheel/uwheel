use super::super::Aggregator;

macro_rules! max_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        #[derive(Default, Debug, Clone, Copy)]
        pub struct $struct;

        impl Aggregator for $struct {
            type Input = $type;
            type MutablePartialAggregate = $pa;
            type Aggregate = $type;
            type PartialAggregate = $pa;

            fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
                input.into()
            }

            #[inline]
            fn combine_mutable(a: &mut Self::MutablePartialAggregate, input: Self::Input) {
                *a = <$type>::max(*a, input);
            }

            fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
                a.into()
            }

            #[inline]
            fn combine(
                a: Self::PartialAggregate,
                b: Self::PartialAggregate,
            ) -> Self::PartialAggregate {
                <$type>::max(a, b)
            }
            #[inline]
            fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
                a
            }
        }
    };
}

max_impl!(U16MaxAggregator, u16, u16);
max_impl!(U32MaxAggregator, u32, u32);
max_impl!(U64MaxAggregator, u64, u64);
max_impl!(U128MaxAggregator, u128, u128);
max_impl!(I16MaxAggregator, i16, i16);
max_impl!(I32MaxAggregator, i32, i32);
max_impl!(I64MaxAggregator, i64, i64);
max_impl!(I128MaxAggregator, i128, i128);
max_impl!(F32MaxAggregator, f32, f32);
max_impl!(F64MaxAggregator, f64, f64);

#[cfg(test)]
mod tests {
    use crate::{time::NumericalDuration, Entry, Wheel};

    use super::*;

    #[test]
    fn max_test() {
        let mut wheel = Wheel::<U64MaxAggregator>::new(0);
        wheel.insert(Entry::new(1, 1000)).unwrap();
        wheel.insert(Entry::new(5, 2000)).unwrap();
        wheel.insert(Entry::new(10, 3000)).unwrap();
        wheel.advance(3.seconds());
        assert_eq!(wheel.interval_and_lower(3.seconds()), Some(5));
        wheel.advance(1.seconds());
        assert_eq!(wheel.interval_and_lower(3.seconds()), Some(10));
    }
}
