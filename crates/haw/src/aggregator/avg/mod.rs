use super::super::Aggregator;
use crate::aggregator::InverseExt;

macro_rules! avg_impl {
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
                let (ref mut sum, ref mut count) = window;
                *sum += input;
                *count += 1 as $type;
            }

            fn init_window(&self, input: Self::Input) -> Self::Window {
                (input, 1 as $type)
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
                let sum = a.0 + b.0;
                let count = a.1 + b.1;
                (sum, count)
            }
            #[inline]
            fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate {
                a.0 / a.1
            }
        }
        impl InverseExt for $struct {
            #[inline]
            fn inverse_combine(
                &self,
                a: Self::PartialAggregate,
                b: Self::PartialAggregate,
            ) -> Self::PartialAggregate {
                let (a_sum, a_count) = a;
                let (b_sum, b_count) = b;
                let inv_sum = a_sum - b_sum;
                let inv_count = a_count - b_count;
                (inv_sum, inv_count)
            }
        }
    };
}

avg_impl!(U16AvgAggregator, u16, (u16, u16));
avg_impl!(U32AvgAggregator, u32, (u32, u32));
avg_impl!(U64AvgAggregator, u64, (u64, u64));
avg_impl!(U128AvgAggregator, u128, (u128, u128));
avg_impl!(I16AvgAggregator, i16, (i16, i16));
avg_impl!(I32AvgAggregator, i32, (i32, i32));
avg_impl!(I64AvgAggregator, i64, (i64, i64));
avg_impl!(I128AvgAggregator, i128, (i128, i128));
avg_impl!(F32AvgAggregator, f32, (f32, f32));
avg_impl!(F64AvgAggregator, f64, (f64, f64));

#[cfg(test)]
mod tests {
    use crate::{time::NumericalDuration, Wheel, SECONDS};

    use super::*;

    #[test]
    fn avg_test() {
        let mut time = 0u64;
        let mut wheel = Wheel::<U64AvgAggregator>::new(time);

        for _ in 0..SECONDS + 1 {
            wheel.advance_to(time);
            let entry = crate::Entry::new(10, time);
            wheel.insert(entry).unwrap();
            time += 1000; // increase by 1 second
        }
        // partial aggregate
        assert_eq!(wheel.interval(15.seconds()), Some((150, 15)));
        // full aggregate
        assert_eq!(wheel.interval_and_lower(15.seconds()), Some(10));
    }
}
