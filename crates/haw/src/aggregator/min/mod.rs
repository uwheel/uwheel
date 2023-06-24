use super::super::Aggregator;

macro_rules! min_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        #[derive(Default, Debug, Clone, Copy)]
        pub struct $struct;

        impl Aggregator for $struct {
            type Input = $type;
            type Window = $pa;
            type Aggregate = $type;
            type PartialAggregate = $pa;
            #[inline]
            fn insert(&self, window: &mut Self::Window, input: Self::Input) {
                *window = <$type>::min(*window, input);
            }

            fn init_window(&self, input: Self::Input) -> Self::Window {
                input.into()
            }

            fn lift(&self, window: Self::Window) -> Self::PartialAggregate {
                window.into()
            }

            #[inline]
            fn combine(
                &self,
                a: Self::PartialAggregate,
                b: Self::PartialAggregate,
            ) -> Self::PartialAggregate {
                <$type>::min(a, b)
            }
            #[inline]
            fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate {
                a
            }
        }
    };
}

min_impl!(U16MinAggregator, u16, u16);
min_impl!(U32MinAggregator, u32, u32);
min_impl!(U64MinAggregator, u64, u64);
min_impl!(U128MinAggregator, u128, u128);
min_impl!(I16MinAggregator, i16, i16);
min_impl!(I32MinAggregator, i32, i32);
min_impl!(I64MinAggregator, i64, i64);
min_impl!(I128MinAggregator, i128, i128);
min_impl!(F32MinAggregator, f32, f32);
min_impl!(F64MinAggregator, f64, f64);

#[cfg(test)]
mod tests {
    use crate::{time::NumericalDuration, Entry, Wheel};

    use super::*;

    #[test]
    fn min_test() {
        let mut wheel = Wheel::<U64MinAggregator>::new(0);
        wheel.insert(Entry::new(1, 1000)).unwrap();
        wheel.insert(Entry::new(5, 2000)).unwrap();
        wheel.insert(Entry::new(10, 3000)).unwrap();
        wheel.advance(3.seconds());
        assert_eq!(wheel.interval_and_lower(3.seconds()), Some(1));
        wheel.advance(1.seconds());
        assert_eq!(wheel.interval_and_lower(3.seconds()), Some(1));
    }
}
