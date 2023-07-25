use core::{fmt::Debug, hash::Hash};

#[cfg(not(feature = "serde"))]
pub trait KeyBounds: Hash + Debug + Copy + Eq + Default + Send + 'static {}
#[cfg(feature = "serde")]
pub trait KeyBounds:
    Hash
    + Debug
    + Copy
    + Eq
    + Default
    + Send
    + serde::Serialize
    + for<'a> serde::Deserialize<'a>
    + 'static
{
}

#[cfg(not(feature = "serde"))]
impl<T> KeyBounds for T where T: Hash + Debug + Copy + Eq + Default + Send + Clone + 'static {}

#[cfg(feature = "serde")]
impl<T> KeyBounds for T where
    T: Hash
        + Debug
        + Copy
        + Eq
        + Default
        + Send
        + Clone
        + serde::Serialize
        + for<'a> serde::Deserialize<'a>
        + 'static
{
}
