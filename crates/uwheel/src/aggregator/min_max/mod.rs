use crate::{aggregator::PartialAggregateType, Aggregator};

#[inline]
fn min<T: PartialOrd>(a: T, b: T) -> T {
    if a < b {
        a
    } else {
        b
    }
}

#[inline]
fn max<T: PartialOrd>(a: T, b: T) -> T {
    if a > b {
        a
    } else {
        b
    }
}

/// MinMax Aggregate State
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct MinMaxState<T: PartialOrd + Copy> {
    min: T,
    max: T,
}
impl<T: PartialOrd + Copy> MinMaxState<T> {
    #[inline]
    fn merge(&mut self, other: Self) {
        self.min = min(self.min, other.min);
        self.max = max(self.max, other.max);
    }
    /// Returns the minimum value seen so far
    pub fn min_value(&self) -> T {
        self.min
    }
    /// Returns the maximum value seen so far
    pub fn max_value(&self) -> T {
        self.max
    }
}

macro_rules! min_max_partial_impl {
    ($type:ty) => {
        impl PartialAggregateType for MinMaxState<$type> {}
    };
}

min_max_partial_impl!(u8);
min_max_partial_impl!(u16);
min_max_partial_impl!(u32);
min_max_partial_impl!(u64);
min_max_partial_impl!(i8);
min_max_partial_impl!(i16);
min_max_partial_impl!(i32);
min_max_partial_impl!(i64);
min_max_partial_impl!(f32);
min_max_partial_impl!(f64);

macro_rules! min_max_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        #[derive(Default, Debug, Clone, Copy)]
        #[allow(missing_docs)]
        pub struct $struct;

        impl Aggregator for $struct {
            const IDENTITY: Self::PartialAggregate = MinMaxState {
                min: <$type>::MAX,
                max: <$type>::MIN,
            };

            type Input = $type;
            type MutablePartialAggregate = Self::PartialAggregate;
            type Aggregate = Self::PartialAggregate;
            type PartialAggregate = MinMaxState<$pa>;

            fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
                Self::PartialAggregate {
                    min: input,
                    max: input,
                }
            }

            #[inline]
            fn combine_mutable(mutable: &mut Self::MutablePartialAggregate, input: Self::Input) {
                let state = Self::lift(input);
                mutable.merge(state)
            }

            fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
                a.into()
            }

            #[inline]
            fn combine(
                mut a: Self::PartialAggregate,
                b: Self::PartialAggregate,
            ) -> Self::PartialAggregate {
                a.merge(b);
                a
            }
            #[inline]
            fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
                a
            }
        }
    };
}

min_max_impl!(U16MinMaxAggregator, u16, u16);
min_max_impl!(U32MinMaxAggregator, u32, u32);
min_max_impl!(U64MinMaxAggregator, u64, u64);
min_max_impl!(I8MinMaxAggregator, i8, i8);
min_max_impl!(I16MinMaxAggregator, i16, i16);
min_max_impl!(I32MinMaxAggregator, i32, i32);
min_max_impl!(I64MinMaxAggregator, i64, i64);
min_max_impl!(F32MinMaxAggregator, f32, f32);
min_max_impl!(F64MinMaxAggregator, f64, f64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u32_min_max() {
        let mut agg = U32MinMaxAggregator::IDENTITY;
        for i in 1..=10 {
            U32MinMaxAggregator::combine_mutable(&mut agg, i);
        }
        let result = U32MinMaxAggregator::lower(agg);
        assert_eq!(result.min_value(), 1);
        assert_eq!(result.max_value(), 10);
    }

    #[test]
    fn test_u32_min_max_empty() {
        let result = U32MinMaxAggregator::IDENTITY;
        assert_eq!(result.min_value(), u32::MAX);
        assert_eq!(result.max_value(), u32::MIN);
    }

    #[test]
    fn test_u32_min_max_single_value() {
        let mut agg = U32MinMaxAggregator::IDENTITY;
        U32MinMaxAggregator::combine_mutable(&mut agg, 5);
        let result = U32MinMaxAggregator::lower(agg);
        assert_eq!(result.min_value(), 5);
        assert_eq!(result.max_value(), 5);
    }

    #[test]
    fn test_u32_min_max_descending_order() {
        let mut agg = U32MinMaxAggregator::IDENTITY;
        for i in (1..=10).rev() {
            U32MinMaxAggregator::combine_mutable(&mut agg, i);
        }
        let result = U32MinMaxAggregator::lower(agg);
        assert_eq!(result.min_value(), 1);
        assert_eq!(result.max_value(), 10);
    }

    #[test]
    fn test_u32_min_max_duplicate_values() {
        let mut agg = U32MinMaxAggregator::IDENTITY;
        for &i in &[3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5] {
            U32MinMaxAggregator::combine_mutable(&mut agg, i);
        }
        let result = U32MinMaxAggregator::lower(agg);
        assert_eq!(result.min_value(), 1);
        assert_eq!(result.max_value(), 9);
    }

    #[test]
    fn test_i32_min_max() {
        let mut agg = I32MinMaxAggregator::IDENTITY;
        for &i in &[-5, 0, 5, -10, 10, -3, 3] {
            I32MinMaxAggregator::combine_mutable(&mut agg, i);
        }
        let result = I32MinMaxAggregator::lower(agg);
        assert_eq!(result.min_value(), -10);
        assert_eq!(result.max_value(), 10);
    }

    #[test]
    fn test_i32_min_max_extremes() {
        let mut agg = I32MinMaxAggregator::IDENTITY;
        I32MinMaxAggregator::combine_mutable(&mut agg, i32::MIN);
        I32MinMaxAggregator::combine_mutable(&mut agg, i32::MAX);
        I32MinMaxAggregator::combine_mutable(&mut agg, 0);
        let result = I32MinMaxAggregator::lower(agg);
        assert_eq!(result.min_value(), i32::MIN);
        assert_eq!(result.max_value(), i32::MAX);
    }

    #[test]
    fn test_f32_min_max() {
        let mut agg = F32MinMaxAggregator::IDENTITY;
        for i in 1..=10 {
            F32MinMaxAggregator::combine_mutable(&mut agg, i as f32);
        }
        let result = F32MinMaxAggregator::lower(agg);
        assert_eq!(result.min_value(), 1.0);
        assert_eq!(result.max_value(), 10.0);
    }

    #[test]
    fn test_f32_min_max_empty() {
        let result = F32MinMaxAggregator::IDENTITY;
        assert_eq!(result.min_value(), f32::MAX);
        assert_eq!(result.max_value(), f32::MIN);
    }
}
