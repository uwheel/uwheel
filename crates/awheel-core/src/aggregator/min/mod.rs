use super::super::Aggregator;

#[cfg(feature = "simd")]
use core::simd::prelude::{SimdFloat, SimdInt, SimdOrd, SimdUint};
#[cfg(feature = "simd")]
use core::simd::{f32x32, f64x32, i16x64, i32x32, i64x32, u16x64, u32x32, u64x32};

#[cfg(feature = "simd")]
use multiversion::multiversion;

macro_rules! min_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        min_impl!($struct, $type, $pa, ());
    };
    ($struct:tt, $type:ty, $pa:tt, $simd: ty) => {
        #[derive(Default, Debug, Clone, Copy)]
        #[allow(missing_docs)]
        pub struct $struct;

        impl Aggregator for $struct {
            const IDENTITY: Self::PartialAggregate = <$type>::MAX;
            type CombineSimd = fn(&[$pa]) -> $pa;
            type CombineInverse = fn($pa, $pa) -> $pa;

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

            #[cfg(feature = "simd")]
            #[inline]
            fn combine_simd() -> Option<Self::CombineSimd> {
                Some(|slice: &[$pa]| Self::simd_min(slice))
            }
        }

        impl $struct {
            #[cfg(feature = "simd")]
            #[multiversion(targets = "simd")]
            #[inline]
            fn simd_min(slice: &[$pa]) -> $pa {
                let (head, chunks, tail) = slice.as_simd();
                let chunk = chunks.iter().fold(
                    <$simd>::from_array([$pa::MAX; <$simd>::LEN]),
                    |acc, chunk| acc.simd_min(*chunk),
                );
                let min_chunk = chunk.reduce_min();
                let head = head.iter().copied().fold($pa::MAX, <$type>::min);
                let tail = tail.iter().copied().fold($pa::MAX, <$type>::min);
                let min_remainder = <$type>::min(head, tail);
                <$type>::min(min_chunk, min_remainder)
            }
        }
    };
}

#[cfg(not(feature = "simd"))]
min_impl!(U16MinAggregator, u16, u16);
#[cfg(feature = "simd")]
min_impl!(U16MinAggregator, u16, u16, u16x64);

#[cfg(not(feature = "simd"))]
min_impl!(U32MinAggregator, u32, u32);
#[cfg(feature = "simd")]
min_impl!(U32MinAggregator, u32, u32, u32x32);

#[cfg(not(feature = "simd"))]
min_impl!(U64MinAggregator, u64, u64);
#[cfg(feature = "simd")]
min_impl!(U64MinAggregator, u64, u64, u64x32);

#[cfg(not(feature = "simd"))]
min_impl!(I16MinAggregator, i16, i16);
#[cfg(feature = "simd")]
min_impl!(I16MinAggregator, i16, i16, i16x64);

#[cfg(not(feature = "simd"))]
min_impl!(I32MinAggregator, i32, i32);
#[cfg(feature = "simd")]
min_impl!(I32MinAggregator, i32, i32, i32x32);

#[cfg(not(feature = "simd"))]
min_impl!(I64MinAggregator, i64, i64);
#[cfg(feature = "simd")]
min_impl!(I64MinAggregator, i64, i64, i64x32);

#[cfg(not(feature = "simd"))]
min_impl!(F32MinAggregator, f32, f32);
#[cfg(feature = "simd")]
min_impl!(F32MinAggregator, f32, f32, f32x32);

#[cfg(not(feature = "simd"))]
min_impl!(F64MinAggregator, f64, f64);
#[cfg(feature = "simd")]
min_impl!(F64MinAggregator, f64, f64, f64x32);

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

    #[cfg(feature = "simd")]
    #[test]
    fn combine_simd() {
        let values = (0..1000u64).collect::<Vec<u64>>();
        assert_eq!(U64MinAggregator::combine_slice(&values), Some(0));
    }
}
