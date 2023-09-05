#![no_std]

#[cfg(feature = "std")]
extern crate std;

extern crate sqlite3_sys as ffi;

extern crate alloc;

pub use awheel::{self, aggregator::sum::U64SumAggregator, *};
use awheel::{rw_wheel::read::Lazy, time::Duration};
use postcard::to_allocvec;

mod connection;

#[macro_export]
macro_rules! c_str_to_str(
    ($string:expr) => (core::str::from_utf8(core::ffi::CStr::from_ptr($string).to_bytes()));
);

#[macro_export]
macro_rules! c_str_to_string(
    ($string:expr) => (
        String::from_utf8_lossy(core::ffi::CStr::from_ptr($string as *const _).to_bytes())
               .into_owned()
    );
);

// NOTE: SQLite commands for future reference
// conn.execute("CREATE TABLE IF NOT EXISTS wheels (wheel BLOB)", ())
//     .unwrap();
// conn.execute(
//     "CREATE TABLE IF NOT EXISTS entries (timestamp INTEGER, data BLOB)",
//     (),
// )
// .unwrap();
// conn.execute(
//     "PRAGMA journal_mode = OFF;
//               PRAGMA temp_store = MEMORY;
//               PRAGMA synchronous = 0;
//     ",
//     (),
// )
// .unwrap();
// self.conn
//     .execute("INSERT INTO wheels (wheel) VALUES (?1)", vec![value])
//     .unwrap();

/// A tiny embeddable temporal database
#[allow(dead_code)]
pub struct WheelDB<A: Aggregator> {
    wheel: RwWheel<A, Lazy>,
    conn: connection::Connection,
}
impl<A: Aggregator> WheelDB<A> {
    pub fn open_in_memory() -> Self {
        let conn = connection::Connection::open(":memory:");
        Self {
            wheel: RwWheel::new(0),
            conn,
        }
    }
    pub fn new(name: &str) -> Self {
        let conn = connection::Connection::open(name);
        Self {
            wheel: RwWheel::new(0),
            conn,
        }
    }
    pub fn watermark(&self) -> u64 {
        self.wheel.watermark()
    }

    #[inline]
    pub fn insert(&mut self, entry: impl Into<Entry<A::Input>>) {
        let entry = entry.into();
        // 1. Insert to WAL table
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
        let bytes = to_allocvec(&self.wheel.read()).unwrap();
        let _compressed = lz4_flex::compress_prepend_size(&bytes);
        // core::writeln!("Storing BLOB as compressed bytes", "{}", compressed.len());
    }
}

#[cfg(test)]
mod tests {
    use awheel::{aggregator::sum::I32SumAggregator, time::NumericalDuration};

    use super::*;

    #[test]
    fn basic_db_test() {
        let mut db: WheelDB<I32SumAggregator> = WheelDB::open_in_memory();
        db.insert(Entry::new(10, 1000));
        db.advance(1.seconds());
        db.checkpoint();
    }
}
