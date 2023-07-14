use super::super::Aggregator;
use crate::aggregator::InverseExt;

macro_rules! integer_sum_impl {
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
                *a += input;
            }
            fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
                a.into()
            }

            #[inline]
            fn combine(
                a: Self::PartialAggregate,
                b: Self::PartialAggregate,
            ) -> Self::PartialAggregate {
                a.saturating_add(b)
            }
            #[inline]
            fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
                a
            }
        }
        impl InverseExt for $struct {
            #[inline]
            fn inverse_combine(
                a: Self::PartialAggregate,
                b: Self::PartialAggregate,
            ) -> Self::PartialAggregate {
                a.saturating_sub(b)
            }
        }
    };
}

macro_rules! float_sum_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        #[derive(Default, Debug, Clone, Copy)]
        pub struct $struct;

        impl Aggregator for $struct {
            type Input = $pa;
            type Aggregate = $type;
            type PartialAggregate = $pa;
            type MutablePartialAggregate = $pa;
            fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
                input.into()
            }
            #[inline]
            fn combine_mutable(a: &mut Self::MutablePartialAggregate, input: Self::Input) {
                *a += input;
            }

            fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
                a.into()
            }

            fn combine(
                a: Self::PartialAggregate,
                b: Self::PartialAggregate,
            ) -> Self::PartialAggregate {
                a + b
            }
            fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
                a as Self::Aggregate
            }
        }
    };
}

integer_sum_impl!(U16SumAggregator, u16, u16);
integer_sum_impl!(U32SumAggregator, u32, u32);
integer_sum_impl!(U64SumAggregator, u64, u64);
integer_sum_impl!(U128SumAggregator, u128, u128);
integer_sum_impl!(I16SumAggregator, i16, i16);
integer_sum_impl!(I32SumAggregator, i32, i32);
integer_sum_impl!(I64SumAggregator, i64, i64);
integer_sum_impl!(I128SumAggregator, i128, i128);

float_sum_impl!(F32SumAggregator, f32, f32);
float_sum_impl!(F64SumAggregator, f64, f64);
