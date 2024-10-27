use super::super::Aggregator;

#[cfg(feature = "simd")]
use core::simd::prelude::{SimdFloat, SimdInt, SimdUint};
#[cfg(feature = "simd")]
use core::simd::{f32x32, f64x32, i16x64, i32x32, i64x32, u16x64, u32x32, u64x32};

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
            fn merge(dst: &mut [$pa], src: &[$pa]) {
                let (src_head, src_chunks, src_tail) = src.as_simd::<{ <$simd>::LEN }>();
                let (dst_head, dst_chunks, dst_tail) = dst.as_simd_mut::<{ <$simd>::LEN }>();

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

            #[inline]
            fn combine_inverse() -> Option<fn(Self::PartialAggregate, Self::PartialAggregate) -> Self::PartialAggregate> {
                Some(|a, b| if a > b { a - b } else { 0 as $pa })
            }

            #[cfg(feature = "simd")]
            #[inline]
            fn combine_simd() -> Option<fn(&[Self::PartialAggregate]) -> Self::PartialAggregate> {
                Some(|slice: &[$pa]| Self::simd_sum(slice))
            }
        }
        impl $struct {
            #[cfg(feature = "simd")]
            #[multiversion(targets = "simd")]
            #[inline]
            fn simd_sum(slice: &[$pa]) -> $pa {
                let (head, chunks, tail) = slice.as_simd();
                let chunk = chunks
                    .iter()
                    .fold(<$simd>::default(), |acc, chunk| acc + *chunk);
                chunk.reduce_sum()
                    + head.iter().copied().sum::<$pa>()
                    + tail.iter().copied().sum::<$pa>()
            }

            #[inline]
            #[cfg(feature = "simd")]
            #[multiversion(targets = "simd")]
            fn simd_build_prefix(slice: &[$pa]) -> Vec<$pa> {
                let len = slice.len();
                let mut output = vec![<$pa>::default(); len];

                let chunk_size = <$simd>::LEN;
                let chunks = slice.chunks_exact(chunk_size);
                let remainder = chunks.remainder();

                let mut cumulative_sum = <$pa>::default();

                // Process each chunk
                for (i, chunk) in chunks.enumerate() {
                    let mut data = <$simd>::from_slice(chunk);

                    // Calculate prefix sum within one SIMD chunk
                    for j in 1..chunk_size {
                        data[j] += data[j - 1];
                    }

                    data += <$simd>::splat(cumulative_sum);

                    // Update cumulative sum for next chunk
                    cumulative_sum = data[chunk_size - 1];

                    // copy back
                    output[i * chunk_size..i * chunk_size + data.as_array().len()]
                        .copy_from_slice(data.as_array());
                }

                // Handle remaining elements (non-SIMD part)
                let remainder_start = len - remainder.len();
                if !remainder.is_empty() {
                    output[remainder_start] = remainder[0] + cumulative_sum;
                    for i in 1..remainder.len() {
                        output[remainder_start + i] =
                            remainder[i] + output[remainder_start + i - 1];
                    }
                }

                output
            }
        }
    };
}

#[cfg(not(feature = "simd"))]
sum_impl!(U16SumAggregator, u16, u16);
#[cfg(feature = "simd")]
sum_impl!(U16SumAggregator, u16, u16, u16x64);

#[cfg(not(feature = "simd"))]
sum_impl!(U32SumAggregator, u32, u32);
#[cfg(feature = "simd")]
sum_impl!(U32SumAggregator, u32, u32, u32x32);

#[cfg(not(feature = "simd"))]
sum_impl!(U64SumAggregator, u64, u64);
#[cfg(feature = "simd")]
sum_impl!(U64SumAggregator, u64, u64, u64x32);

#[cfg(not(feature = "simd"))]
sum_impl!(I16SumAggregator, i16, i16);
#[cfg(feature = "simd")]
sum_impl!(I16SumAggregator, i16, i16, i16x64);

#[cfg(not(feature = "simd"))]
sum_impl!(I32SumAggregator, i32, i32);
#[cfg(feature = "simd")]
sum_impl!(I32SumAggregator, i32, i32, i32x32);

#[cfg(not(feature = "simd"))]
sum_impl!(I64SumAggregator, i64, i64);
#[cfg(feature = "simd")]
sum_impl!(I64SumAggregator, i64, i64, i64x32);

#[cfg(not(feature = "simd"))]
sum_impl!(F32SumAggregator, f32, f32);
#[cfg(feature = "simd")]
sum_impl!(F32SumAggregator, f32, f32, f32x32);

#[cfg(not(feature = "simd"))]
sum_impl!(F64SumAggregator, f64, f64);
#[cfg(feature = "simd")]
sum_impl!(F64SumAggregator, f64, f64, f64x32);

#[cfg(test)]
mod tests {
    use crate::{duration::NumericalDuration, Entry, RwWheel};

    use super::*;

    #[test]
    fn sum_test() {
        let mut wheel = RwWheel::<U64SumAggregator>::new(0);
        wheel.insert(Entry::new(1, 1000));
        wheel.insert(Entry::new(5, 2000));
        wheel.insert(Entry::new(10, 3000));
        wheel.advance(3.seconds());
        assert_eq!(wheel.read().interval_and_lower(3.seconds()), Some(6));
        wheel.advance(1.seconds());
        assert_eq!(wheel.read().interval_and_lower(3.seconds()), Some(16));
    }

    #[test]
    fn sum_prefix_test() {
        let partials = vec![1, 2, 3];
        let prefix_sum = U64SumAggregator::build_prefix(&partials);
        assert_eq!(U64SumAggregator::prefix_query(&prefix_sum, 0, 1), Some(3));
        assert_eq!(U64SumAggregator::prefix_query(&prefix_sum, 1, 2), Some(5));
        assert_eq!(U64SumAggregator::prefix_query(&prefix_sum, 0, 2), Some(6));
    }

    #[cfg(feature = "simd")]
    #[test]
    fn combine_simd() {
        let values = (0..1000u64).collect::<Vec<u64>>();
        let native_sum = values.iter().sum();
        assert_eq!(U64SumAggregator::combine_slice(&values), Some(native_sum));
    }

    #[cfg(feature = "simd")]
    #[test]
    fn simd_sum_prefix_functionality_test() {
        // less than 1 chunk
        let partials = vec![1, 2, 3];
        let prefix_sum = U64SumAggregator::simd_build_prefix(&partials);
        assert_eq!(U64SumAggregator::prefix_query(&prefix_sum, 0, 1), Some(3));
        assert_eq!(U64SumAggregator::prefix_query(&prefix_sum, 1, 2), Some(5));
        assert_eq!(U64SumAggregator::prefix_query(&prefix_sum, 0, 2), Some(6));

        // greater than 1 chunk
        let partials: Vec<u64> = (1..101).collect();
        let prefix_sum = U64SumAggregator::simd_build_prefix(&partials);
        assert_eq!(
            U64SumAggregator::prefix_query(&prefix_sum, 0, 99),
            Some(5050)
        );

        // different SIMD LANE
        let partials = vec![1i16, 2i16, 3i16];
        let prefix_sum = I16SumAggregator::simd_build_prefix(&partials);
        assert_eq!(
            I16SumAggregator::prefix_query(&prefix_sum, 0, 1),
            Some(3i16)
        );
        assert_eq!(
            I16SumAggregator::prefix_query(&prefix_sum, 1, 2),
            Some(5i16)
        );
        assert_eq!(
            I16SumAggregator::prefix_query(&prefix_sum, 0, 2),
            Some(6i16)
        );
    }

    #[cfg(feature = "simd")]
    #[test]
    fn simd_sum_prefix_performance_test() {
        let len = 100_000_000;
        let partials: Vec<u64> = vec![1u64; len];
        let prefix_sum = U64SumAggregator::simd_build_prefix(&partials);
        assert_eq!(
            U64SumAggregator::prefix_query(&prefix_sum, 0, len - 1),
            Some(len as u64)
        );
    }
}
