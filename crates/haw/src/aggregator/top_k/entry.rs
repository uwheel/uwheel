use core::{cmp::Ordering, fmt::Debug};

#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Copy, Clone, Eq)]
pub struct TopKEntry<const KEY_BYTES: usize, A: Ord + Copy> {
    pub key: [u8; KEY_BYTES],
    pub data: A,
}

impl<const KEY_BYTES: usize, A: Ord + Copy> TopKEntry<KEY_BYTES, A> {
    pub fn new(key: [u8; KEY_BYTES], data: A) -> Self {
        Self { key, data }
    }
}
impl<const KEY_BYTES: usize, A: Ord + Copy> Ord for TopKEntry<KEY_BYTES, A> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(&other.data)
    }
}

impl<const KEY_BYTES: usize, A: Ord + Copy> PartialOrd for TopKEntry<KEY_BYTES, A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<const KEY_BYTES: usize, A: Ord + Copy> PartialEq for TopKEntry<KEY_BYTES, A> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}
