use super::super::Aggregator;
use crate::aggregator::InverseExt;

macro_rules! avg_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        #[derive(Default, Debug, Clone, Copy)]
        #[allow(missing_docs)]
        pub struct $struct;

        impl Aggregator for $struct {
            const IDENTITY: Self::PartialAggregate = (0 as $type, 0 as $type);
            type Input = $type;
            type MutablePartialAggregate = $pa;
            type Aggregate = $type;
            type PartialAggregate = $pa;

            const PREFIX_SUPPORT: bool = true;
            #[cfg(feature = "simd")]
            const SIMD_LANES: usize = 0;
            #[cfg(feature = "simd")]
            type CombineSimd = fn(&[$pa]) -> $pa;

            type CombineInverse = fn($pa, $pa) -> $pa;

            fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
                (input, 1 as $type)
            }
            #[inline]
            fn combine_mutable(a: &mut Self::MutablePartialAggregate, input: Self::Input) {
                let (ref mut sum, ref mut count) = a;
                *sum += input;
                *count += 1 as $type;
            }

            fn freeze(mutable: Self::MutablePartialAggregate) -> Self::PartialAggregate {
                mutable.into()
            }

            #[inline]
            fn combine(
                a: Self::PartialAggregate,
                b: Self::PartialAggregate,
            ) -> Self::PartialAggregate {
                let sum = a.0 + b.0;
                let count = a.1 + b.1;
                (sum, count)
            }

            #[inline]
            fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
                a.0 / a.1
            }
        }
        impl InverseExt for $struct {
            #[inline]
            fn inverse_combine(
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
avg_impl!(I16AvgAggregator, i16, (i16, i16));
avg_impl!(I32AvgAggregator, i32, (i32, i32));
avg_impl!(I64AvgAggregator, i64, (i64, i64));
avg_impl!(I128AvgAggregator, i128, (i128, i128));
avg_impl!(F32AvgAggregator, f32, (f32, f32));
avg_impl!(F64AvgAggregator, f64, (f64, f64));

#[cfg(test)]
mod tests {
    use crate::{time_internal::NumericalDuration, RwWheel, SECONDS};

    use super::*;

    #[test]
    fn avg_test() {
        let mut time = 0u64;
        let mut wheel = RwWheel::<U64AvgAggregator>::new(time);

        for _ in 0..SECONDS + 1 {
            wheel.advance_to(time);
            let entry = crate::Entry::new(10, time);
            wheel.insert(entry);
            time += 1000; // increase by 1 second
        }
        // partial aggregate
        assert_eq!(wheel.read().interval(15.seconds()), Some((150, 15)));
        // full aggregate
        assert_eq!(wheel.read().interval_and_lower(15.seconds()), Some(10));
    }
}
