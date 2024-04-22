use super::super::Aggregator;

#[cfg(feature = "simd")]
use core::simd::prelude::{SimdFloat, SimdInt, SimdOrd, SimdUint};
#[cfg(feature = "simd")]
use core::simd::{f32x32, f64x32, i16x64, i32x32, i64x32, u16x64, u32x32, u64x32};

#[cfg(feature = "simd")]
use multiversion::multiversion;

macro_rules! max_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        max_impl!($struct, $type, $pa, ());
    };
    ($struct:tt, $type:ty, $pa:tt, $simd: ty) => {
        #[derive(Default, Debug, Clone, Copy)]
        #[allow(missing_docs)]
        pub struct $struct;

        impl Aggregator for $struct {
            const IDENTITY: Self::PartialAggregate = <$type>::MIN;
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

            #[cfg(feature = "simd")]
            #[inline]
            fn combine_simd() -> Option<fn(&[Self::PartialAggregate]) -> Self::PartialAggregate> {
                Some(|slice: &[$pa]| Self::simd_max(slice))
            }
        }

        impl $struct {
            #[cfg(feature = "simd")]
            #[multiversion(targets = "simd")]
            #[inline]
            fn simd_max(slice: &[$pa]) -> $pa {
                let (head, chunks, tail) = slice.as_simd();
                let chunk = chunks.iter().fold(
                    <$simd>::from_array([$pa::MIN; <$simd>::LEN]),
                    |acc, chunk| acc.simd_max(*chunk),
                );
                let head_max = head.iter().copied().fold(chunk.reduce_max(), <$type>::max);
                tail.iter().copied().fold(head_max, <$type>::max)
            }
        }
    };
}

#[cfg(not(feature = "simd"))]
max_impl!(U16MaxAggregator, u16, u16);
#[cfg(feature = "simd")]
max_impl!(U16MaxAggregator, u16, u16, u16x64);

#[cfg(not(feature = "simd"))]
max_impl!(U32MaxAggregator, u32, u32);
#[cfg(feature = "simd")]
max_impl!(U32MaxAggregator, u32, u32, u32x32);

#[cfg(not(feature = "simd"))]
max_impl!(U64MaxAggregator, u64, u64);
#[cfg(feature = "simd")]
max_impl!(U64MaxAggregator, u64, u64, u64x32);

#[cfg(not(feature = "simd"))]
max_impl!(I16MaxAggregator, i16, i16);
#[cfg(feature = "simd")]
max_impl!(I16MaxAggregator, i16, i16, i16x64);

#[cfg(not(feature = "simd"))]
max_impl!(I32MaxAggregator, i32, i32);
#[cfg(feature = "simd")]
max_impl!(I32MaxAggregator, i32, i32, i32x32);

#[cfg(not(feature = "simd"))]
max_impl!(I64MaxAggregator, i64, i64);
#[cfg(feature = "simd")]
max_impl!(I64MaxAggregator, i64, i64, i64x32);

#[cfg(not(feature = "simd"))]
max_impl!(F32MaxAggregator, f32, f32);
#[cfg(feature = "simd")]
max_impl!(F32MaxAggregator, f32, f32, f32x32);

#[cfg(not(feature = "simd"))]
max_impl!(F64MaxAggregator, f64, f64);
#[cfg(feature = "simd")]
max_impl!(F64MaxAggregator, f64, f64, f64x32);

#[cfg(test)]
mod tests {
    use crate::{duration::NumericalDuration, Entry, RwWheel};

    use super::*;

    #[test]
    fn max_test() {
        let mut wheel = RwWheel::<U64MaxAggregator>::new(0);
        wheel.insert(Entry::new(1, 1000));
        wheel.insert(Entry::new(5, 2000));
        wheel.insert(Entry::new(10, 3000));
        wheel.advance(3.seconds());
        assert_eq!(wheel.read().interval_and_lower(3.seconds()), Some(5));
        wheel.advance(1.seconds());
        assert_eq!(wheel.read().interval_and_lower(3.seconds()), Some(10));
    }

    #[cfg(feature = "simd")]
    #[test]
    fn combine_simd() {
        let values = (0..1000u64).collect::<Vec<u64>>();
        assert_eq!(U64MaxAggregator::combine_slice(&values), Some(999));
    }
}
