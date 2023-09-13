#![no_std]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

pub mod storage;

pub use awheel::{self, aggregator, aggregator::sum::U64SumAggregator};
use awheel::{
    rw_wheel::{read::Lazy, Options},
    time::Duration,
    Aggregator,
    Entry,
    ReadWheel,
    RwWheel,
};
use storage::{memory::MemoryStorage, Storage};

/// A tiny embeddable temporal database
#[allow(dead_code)]
pub struct WheelDB<'a, A: Aggregator, S = MemoryStorage> {
    id: &'a str,
    wheel: RwWheel<A, Lazy>,
    storage: S,
}
impl<'a, A: Aggregator, S> WheelDB<'a, A, S> {
    pub fn open_default_with_storage(id: &'static str, storage: S) -> Self {
        Self::open_with_storage(id, Default::default(), storage)
    }
    pub fn open_with_storage(id: &'static str, opts: Options, storage: S) -> Self {
        Self {
            id,
            wheel: RwWheel::with_options(opts),
            storage,
        }
    }
}
impl<'a, A: Aggregator> WheelDB<'a, A, MemoryStorage> {
    pub fn open(id: &'static str, opts: Options) -> Self {
        Self {
            id,
            wheel: RwWheel::with_options(opts),
            storage: Default::default(),
        }
    }
    pub fn open_default(id: &'static str) -> Self {
        Self::open(id, Default::default())
    }
}

impl<'a, A: Aggregator, S: Storage> WheelDB<'a, A, S> {
    #[inline]
    pub fn watermark(&self) -> u64 {
        self.wheel.watermark()
    }

    pub fn now(&self) -> Duration {
        Duration::milliseconds(self.watermark() as i64)
    }
    pub fn id(&self) -> &'a str {
        self.id
    }

    #[inline]
    pub fn insert(&mut self, entry: impl Into<Entry<A::Input>>) {
        let entry = entry.into();
        // 1. Insert to WAL table
        self.storage.insert_wal::<A>(&entry);

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
        self.storage.sync(self.wheel.read());
    }
}

#[cfg(test)]
mod tests {
    use awheel::{aggregator::sum::I32SumAggregator, time::NumericalDuration};

    use super::*;

    #[test]
    fn basic_db_test() {
        let mut db: WheelDB<I32SumAggregator> = WheelDB::open_default("test");
        db.insert(Entry::new(10, 1000));
        db.advance(1.seconds());
        db.checkpoint();
    }
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_storage_test() {
        use crate::storage::sqlite::SQLite;
        let mut db: WheelDB<I32SumAggregator, _> =
            WheelDB::open_default_with_storage("test", SQLite::new(":memory:"));
        db.insert(Entry::new(10, 1000));
        db.advance(1.seconds());
        db.checkpoint();
    }
}
