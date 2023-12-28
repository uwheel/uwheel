use super::super::Aggregator;
use crate::aggregator::InverseExt;

#[cfg(feature = "simd")]
use core::simd::{f32x16, f64x8, i32x16, i64x8, u32x16, u64x8};
#[cfg(feature = "simd")]
use core::simd::{SimdFloat, SimdInt, SimdUint};

#[cfg(feature = "simd")]
use multiversion::multiversion;

macro_rules! sum_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        sum_impl!($struct, $type, $pa, ());
    };
    ($struct:tt, $type:ty, $pa:tt, $simd: ty) => {
        #[derive(Default, Debug, Clone, Copy)]
        #[allow(missing_docs)]
        pub struct $struct;

        impl Aggregator for $struct {
            const IDENTITY: Self::PartialAggregate = 0 as $pa;

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
                a + b
            }

            #[cfg(feature = "simd")]
            #[multiversion(targets = "simd")]
            #[inline]
            fn combine_slice(slice: &[$pa]) -> Option<$pa> {
                let (head, chunks, tail) = slice.as_simd();
                let chunk = chunks
                    .iter()
                    .fold(<$simd>::default(), |acc, chunk| acc + *chunk);
                Some(
                    chunk.reduce_sum()
                        + head.iter().copied().sum::<$pa>()
                        + tail.iter().copied().sum::<$pa>(),
                )
            }

            #[cfg(feature = "simd")]
            #[multiversion(targets = "simd")]
            #[inline]
            fn merge_slices(dst: &mut [$pa], src: &[$pa]) {
                let (src_head, src_chunks, src_tail) = src.as_simd::<{ <$simd>::LANES }>();
                let (dst_head, dst_chunks, dst_tail) = dst.as_simd_mut::<{ <$simd>::LANES }>();

                // add to destination using scalar approach
                for (d, s) in dst_head.iter_mut().zip(src_head.iter()) {
                    *d += s;
                }

                // add to destination using simd chunks
                for (d, s) in dst_chunks.iter_mut().zip(src_chunks.iter()) {
                    *d += s;
                }

                // add to destination using scalar approach
                for (d, s) in dst_tail.iter_mut().zip(src_tail.iter()) {
                    *d += s;
                }
            }

            #[inline]
            fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
                a.into()
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
                if a > b {
                    a - b
                } else {
                    0 as $pa
                }
            }
        }
    };
}

// #[cfg(not(feature = "simd"))]
// integer_sum_impl!(U16SumAggregator, u16, u16);
// #[cfg(feature = "simd")]
// integer_sum_impl!(U16SumAggregator, u16, u16, u16x32);

#[cfg(not(feature = "simd"))]
sum_impl!(U32SumAggregator, u32, u32);
#[cfg(feature = "simd")]
sum_impl!(U32SumAggregator, u32, u32, u32x16);

#[cfg(not(feature = "simd"))]
sum_impl!(U64SumAggregator, u64, u64);
#[cfg(feature = "simd")]
sum_impl!(U64SumAggregator, u64, u64, u64x8);

// #[cfg(not(feature = "simd"))]
// integer_sum_impl!(I16SumAggregator, i16, i16);
// #[cfg(feature = "simd")]
// integer_sum_impl!(I16SumAggregator, i16, i16, i16x32);

#[cfg(not(feature = "simd"))]
sum_impl!(I32SumAggregator, i32, i32);
#[cfg(feature = "simd")]
sum_impl!(I32SumAggregator, i32, i32, i32x16);

#[cfg(not(feature = "simd"))]
sum_impl!(I64SumAggregator, i64, i64);
#[cfg(feature = "simd")]
sum_impl!(I64SumAggregator, i64, i64, i64x8);

// #[cfg(not(feature = "simd"))]
// integer_sum_impl!(I128SumAggregator, i128, i128);

#[cfg(not(feature = "simd"))]
sum_impl!(F32SumAggregator, f32, f32);
#[cfg(feature = "simd")]
sum_impl!(F32SumAggregator, f32, f32, f32x16);

#[cfg(not(feature = "simd"))]
sum_impl!(F64SumAggregator, f64, f64);
#[cfg(feature = "simd")]
sum_impl!(F64SumAggregator, f64, f64, f64x8);
