use awheel::{Aggregator, Entry, ReadWheel};

use super::Storage;

#[derive(Default, Clone)]
pub struct MemoryStorage;

impl Storage for MemoryStorage {
    #[inline]
    fn insert_wal<A>(&self, _entry: &Entry<A::Input>)
    where
        A: Aggregator,
    {
        // Ephemeral storage, do nothing.
    }
    #[inline]
    fn sync<A>(&self, _wheel: &ReadWheel<A>)
    where
        A: Aggregator,
    {
        // Ephemeral storage, do nothing.
    }
}
