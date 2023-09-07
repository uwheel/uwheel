#![no_std]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

pub mod storage;

pub use awheel::{self, aggregator::sum::U64SumAggregator, *};

use awheel::{rw_wheel::read::Lazy, time::Duration};
use storage::{memory::MemoryStorage, Storage};

/// A tiny embeddable temporal database
#[allow(dead_code)]
pub struct WheelDB<A: Aggregator, S = MemoryStorage<&'static str, A>> {
    id: &'static str,
    wheel: RwWheel<A, Lazy>,
    storage: S,
}
impl<A: Aggregator, S> WheelDB<A, S> {
    pub fn with_storage(id: &'static str, storage: S) -> Self {
        Self {
            id,
            wheel: RwWheel::new(0),
            storage,
        }
    }
}
impl<A: Aggregator> WheelDB<A, MemoryStorage<&'static str, A>> {
    pub fn new(id: &'static str) -> Self {
        Self {
            id,
            wheel: RwWheel::new(0),
            storage: Default::default(),
        }
    }
}
impl<A: Aggregator, S: Storage<&'static str, A>> WheelDB<A, S> {
    #[inline]
    pub fn watermark(&self) -> u64 {
        self.wheel.watermark()
    }

    pub fn now(&self) -> Duration {
        Duration::milliseconds(self.watermark() as i64)
    }

    #[inline]
    pub fn insert(&mut self, entry: impl Into<Entry<A::Input>>) {
        let entry = entry.into();
        // 1. Insert to WAL table
        self.storage.insert_wal(&entry);

        // 2. insert into wheel
        self.wheel.insert(entry);
    }
    #[inline]
    pub fn insert_bulk(&mut self, _entry: impl IntoIterator<Item = Entry<A::Input>>) {
        unimplemented!();
    }
    pub fn read(&self) -> &ReadWheel<A, Lazy> {
        self.wheel.read()
    }
    pub fn advance(&mut self, duration: Duration) {
        // TODO: impl SQLite WAL logic
        self.wheel.advance(duration);
    }
    pub fn advance_to(&mut self, watermark: u64) {
        // TODO: impl SQLite WAL logic
        self.wheel.advance_to(watermark);
    }
    pub fn checkpoint(&self) {
        self.storage.add_wheel(self.id, self.wheel.read());
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::sqlite::SQLite;
    use awheel::{aggregator::sum::I32SumAggregator, time::NumericalDuration};

    use super::*;

    #[test]
    fn basic_db_test() {
        let mut db: WheelDB<I32SumAggregator> = WheelDB::new("test");
        db.insert(Entry::new(10, 1000));
        db.advance(1.seconds());
        db.checkpoint();
    }
    #[test]
    fn sqlite_storage_test() {
        let mut db: WheelDB<I32SumAggregator, _> = WheelDB::with_storage(
            "test",
            SQLite::<&'static str, I32SumAggregator>::new(":memory:"),
        );
        db.insert(Entry::new(10, 1000));
        db.advance(1.seconds());
        db.checkpoint();
    }
}
