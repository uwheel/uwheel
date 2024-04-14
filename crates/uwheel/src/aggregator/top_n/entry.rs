use super::KeyBounds;
use core::{cmp::Ordering, fmt::Debug};

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(
    feature = "serde",
    serde(bound = "A: serde::Serialize + for<'a> serde::Deserialize<'a>")
)]
#[derive(Debug, Copy, Clone, Eq)]
pub struct TopNEntry<Key, A>
where
    Key: KeyBounds,
    A: Ord + Copy,
{
    pub key: Key,
    pub data: A,
}

impl<Key, A> TopNEntry<Key, A>
where
    Key: KeyBounds,
    A: Ord + Copy,
{
    pub fn new(key: Key, data: A) -> Self {
        Self { key, data }
    }
}
impl<Key, A> Ord for TopNEntry<Key, A>
where
    Key: KeyBounds,
    A: Ord + Copy,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(&other.data)
    }
}

impl<Key, A> PartialOrd for TopNEntry<Key, A>
where
    Key: KeyBounds,
    A: Ord + Copy,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<Key, A> PartialEq for TopNEntry<Key, A>
where
    Key: KeyBounds,
    A: Ord + Copy,
{
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}
