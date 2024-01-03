use super::super::Aggregator;

macro_rules! min_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        #[derive(Default, Debug, Clone, Copy)]
        #[allow(missing_docs)]
        pub struct $struct;

        impl Aggregator for $struct {
            const IDENTITY: Self::PartialAggregate = <$type>::MAX;
            const PREFIX_SUPPORT: bool = false;

            type Input = $type;
            type MutablePartialAggregate = $pa;
            type Aggregate = $type;
            type PartialAggregate = $pa;

            fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
                input.into()
            }

            #[inline]
            fn combine_mutable(a: &mut Self::MutablePartialAggregate, input: Self::Input) {
                *a = <$type>::min(*a, input);
            }

            fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
                a.into()
            }

            #[inline]
            fn combine(
                a: Self::PartialAggregate,
                b: Self::PartialAggregate,
            ) -> Self::PartialAggregate {
                <$type>::min(a, b)
            }
            #[inline]
            fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
                a
            }
        }
    };
}

min_impl!(U16MinAggregator, u16, u16);
min_impl!(U32MinAggregator, u32, u32);
min_impl!(U64MinAggregator, u64, u64);
min_impl!(I16MinAggregator, i16, i16);
min_impl!(I32MinAggregator, i32, i32);
min_impl!(I64MinAggregator, i64, i64);
min_impl!(I128MinAggregator, i128, i128);
min_impl!(F32MinAggregator, f32, f32);
min_impl!(F64MinAggregator, f64, f64);

#[cfg(test)]
mod tests {
    use crate::{time_internal::NumericalDuration, Entry, RwWheel};

    use super::*;

    #[test]
    fn min_test() {
        let mut wheel = RwWheel::<U64MinAggregator>::new(0);
        wheel.insert(Entry::new(1, 1000));
        wheel.insert(Entry::new(5, 2000));
        wheel.insert(Entry::new(10, 3000));
        wheel.advance(3.seconds());
        assert_eq!(wheel.read().interval_and_lower(3.seconds()), Some(1));
        wheel.advance(1.seconds());
        assert_eq!(wheel.read().interval_and_lower(3.seconds()), Some(1));
    }
}
