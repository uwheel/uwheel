use super::super::Aggregator;
use crate::aggregator::InverseExt;

macro_rules! integer_sum_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        #[derive(Default, Debug, Clone, Copy)]
        #[allow(missing_docs)]
        pub struct $struct;

        impl Aggregator for $struct {
            const IDENTITY: Self::PartialAggregate = 0;

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

            #[cfg(feature = "simd")]
            #[inline]
            fn combine_slice(slice: &[Self::PartialAggregate]) -> Option<Self::PartialAggregate> {
                Some(arrow2::compute::aggregate::sum_slice(slice))
            }

            #[inline]
            fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
                a
            }

            #[cfg(feature = "pco")]
            fn compress(_data: &[Self::PartialAggregate]) -> Option<Vec<u8>> {
                Some(pco::standalone::auto_compress(
                    _data,
                    pco::DEFAULT_COMPRESSION_LEVEL,
                ))
            }

            #[cfg(feature = "pco")]
            fn decompress(_bytes: &[u8]) -> Option<Vec<Self::PartialAggregate>> {
                Some(
                    pco::standalone::auto_decompress::<Self::PartialAggregate>(&_bytes)
                        .expect("failed to decompress"),
                )
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
        #[allow(missing_docs)]
        pub struct $struct;

        impl Aggregator for $struct {
            const IDENTITY: Self::PartialAggregate = 0.0;
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
            #[cfg(feature = "simd")]
            #[inline]
            fn combine_slice(slice: &[Self::PartialAggregate]) -> Option<Self::PartialAggregate> {
                Some(arrow2::compute::aggregate::sum_slice(slice))
            }
            fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
                a as Self::Aggregate
            }
            #[cfg(feature = "pco")]
            fn compress(_data: &[Self::PartialAggregate]) -> Option<Vec<u8>> {
                Some(pco::standalone::auto_compress(
                    _data,
                    pco::DEFAULT_COMPRESSION_LEVEL,
                ))
            }

            #[cfg(feature = "pco")]
            fn decompress(_bytes: &[u8]) -> Option<Vec<Self::PartialAggregate>> {
                Some(
                    pco::standalone::auto_decompress::<Self::PartialAggregate>(&_bytes)
                        .expect("failed to decompress"),
                )
            }
        }
    };
}

// integer_sum_impl!(U16SumAggregator, u16, u16);
integer_sum_impl!(U32SumAggregator, u32, u32);
integer_sum_impl!(U64SumAggregator, u64, u64);
// integer_sum_impl!(I16SumAggregator, i16, i16);
integer_sum_impl!(I32SumAggregator, i32, i32);
integer_sum_impl!(I64SumAggregator, i64, i64);
// integer_sum_impl!(I128SumAggregator, i128, i128);

float_sum_impl!(F32SumAggregator, f32, f32);
float_sum_impl!(F64SumAggregator, f64, f64);
