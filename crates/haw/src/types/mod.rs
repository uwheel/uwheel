use core::{
    convert::TryFrom,
    fmt::Debug,
    mem::size_of,
    ops::{Index, IndexMut},
};
#[cfg(feature = "rkyv")]
use rkyv::Archive;

#[cfg(not(feature = "rkyv"))]
pub trait PartialAggregateBounds: Default + Debug + Clone + Copy + Send {}

#[cfg(feature = "rkyv")]
pub trait PartialAggregateBounds: Archive + Default + Debug + Clone + Copy + Send {}

#[cfg(not(feature = "rkyv"))]
impl<T> PartialAggregateBounds for T where T: Clone + Copy + Debug + Sync + Send + Default + 'static {}

#[cfg(feature = "rkyv")]
impl<T> PartialAggregateBounds for T where
    T: Archive + Clone + Copy + Debug + Sync + Send + Default + 'static
{
}

// Inspired by arrow2's NativeType trait (https://github.com/jorgecarleitao/arrow2/tree/main/src/types)
pub trait PartialAggregateType: PartialAggregateBounds {
    /// Type denoting its representation as bytes.
    /// This is `[u8; N]` where `N = size_of::<T>`.
    type Bytes: AsRef<[u8]>
        + Index<usize, Output = u8>
        + IndexMut<usize, Output = u8>
        + for<'a> TryFrom<&'a [u8]>
        + Debug
        + Default;

    /// To bytes in little endian
    fn to_le_bytes(&self) -> Self::Bytes;

    /// To bytes in big endian
    fn to_be_bytes(&self) -> Self::Bytes;

    /// From bytes in little endian
    fn from_le_bytes(bytes: Self::Bytes) -> Self;

    /// From bytes in big endian
    fn from_be_bytes(bytes: Self::Bytes) -> Self;
}

macro_rules! partial_agg {
    ($type:ty) => {
        impl PartialAggregateType for $type {
            type Bytes = [u8; size_of::<Self>()];
            #[inline]
            fn to_le_bytes(&self) -> Self::Bytes {
                Self::to_le_bytes(*self)
            }

            #[inline]
            fn to_be_bytes(&self) -> Self::Bytes {
                Self::to_be_bytes(*self)
            }

            #[inline]
            fn from_le_bytes(bytes: Self::Bytes) -> Self {
                Self::from_le_bytes(bytes)
            }

            #[inline]
            fn from_be_bytes(bytes: Self::Bytes) -> Self {
                Self::from_be_bytes(bytes)
            }
        }
    };
}

partial_agg!(u8);
partial_agg!(u16);
partial_agg!(u32);
partial_agg!(u64);
partial_agg!(i8);
partial_agg!(i16);
partial_agg!(i32);
partial_agg!(i64);
partial_agg!(f32);
partial_agg!(f64);
partial_agg!(i128);
partial_agg!(u128);
