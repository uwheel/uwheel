use awheel::{Aggregator, Entry, ReadWheel};

pub mod memory;
#[allow(dead_code)]
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub trait Storage {
    fn insert_wal<A: Aggregator>(&self, entry: &Entry<A::Input>);
    fn sync<A: Aggregator>(&self, wheel: &ReadWheel<A>);
}
