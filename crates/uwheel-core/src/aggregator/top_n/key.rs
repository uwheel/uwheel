use core::{fmt::Debug, hash::Hash};

/// Required bounds for a TopN Key
#[cfg(not(feature = "serde"))]
pub trait KeyBounds: Hash + Debug + Copy + Eq + Send + 'static {}
/// Required bounds for a TopN Key
#[cfg(feature = "serde")]
pub trait KeyBounds:
    Hash + Debug + Copy + Eq + Send + serde::Serialize + for<'a> serde::Deserialize<'a> + 'static
{
}

#[cfg(not(feature = "serde"))]
impl<T> KeyBounds for T where T: Hash + Debug + Copy + Eq + Send + Clone + 'static {}

#[cfg(feature = "serde")]
impl<T> KeyBounds for T where
    T: Hash
        + Debug
        + Copy
        + Eq
        + Send
        + Clone
        + serde::Serialize
        + for<'a> serde::Deserialize<'a>
        + 'static
{
}
