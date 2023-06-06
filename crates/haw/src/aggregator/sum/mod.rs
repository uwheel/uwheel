use super::super::Aggregator;

macro_rules! unsigned_sum_impl {
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
                *window += input;
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
                a.saturating_add(b)
            }
            #[inline]
            fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate {
                a
            }
        }
    };
}

macro_rules! signed_sum_impl {
    ($struct:tt, $type:ty, $pa:tt) => {
        #[derive(Default, Debug, Clone, Copy)]
        pub struct $struct;

        impl Aggregator for $struct {
            type Input = $type;
            type Aggregate = $type;
            type PartialAggregate = $pa;
            type Window = $pa;

            #[inline]
            fn insert(&self, window: &mut Self::Window, input: Self::Input) {
                *window = window.saturating_add(input);
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
                a.saturating_add(b)
            }
            #[inline]
            fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate {
                a
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
            type Window = $pa;
            #[inline]
            fn insert(&self, window: &mut Self::Window, input: Self::Input) {
                *window += input;
            }

            fn init_window(&self, input: Self::Input) -> Self::Window {
                input.into()
            }

            fn lift(&self, window: Self::Window) -> Self::PartialAggregate {
                window.into()
            }

            fn combine(
                &self,
                a: Self::PartialAggregate,
                b: Self::PartialAggregate,
            ) -> Self::PartialAggregate {
                a + b
            }
            fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate {
                a as Self::Aggregate
            }
        }
    };
}

unsigned_sum_impl!(U16SumAggregator, u16, u16);
unsigned_sum_impl!(U32SumAggregator, u32, u32);
unsigned_sum_impl!(U64SumAggregator, u64, u64);
unsigned_sum_impl!(U128SumAggregator, u128, u128);

signed_sum_impl!(I16SumAggregator, i16, i16);
signed_sum_impl!(I32SumAggregator, i32, i32);
signed_sum_impl!(I64SumAggregator, i64, i64);
signed_sum_impl!(I128SumAggregator, i128, i128);

float_sum_impl!(F32SumAggregator, f32, f32);
float_sum_impl!(F64SumAggregator, f64, f64);
