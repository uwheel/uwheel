pub use awheel_core::{self, aggregator::sum::U64SumAggregator};
use awheel_core::{rw_wheel::read::Lazy, time::Duration, Aggregator, Entry, ReadWheel, RwWheel};
use libsql::{params, Connection, Database, Params, Value};
use postcard::to_allocvec;

pub struct WheelDB<A: Aggregator> {
    wheel: RwWheel<A, Lazy>,
    conn: Connection,
}
impl<A: Aggregator> WheelDB<A> {
    pub fn open_in_memory() -> Self {
        let db = Database::open(":memory:").unwrap();
        let conn = db.connect().unwrap();
        Self::setup_db(&conn);
        Self {
            wheel: RwWheel::new(0),
            conn,
        }
    }
    pub fn new(name: &str) -> Self {
        let path = format!("{}.db", name);
        let db = Database::open(path).unwrap();
        let conn = db.connect().unwrap();
        Self::setup_db(&conn);
        Self {
            wheel: RwWheel::new(0),
            conn,
        }
    }

    fn setup_db(conn: &Connection) {
        conn.execute(
            "PRAGMA journal_mode = OFF;
                      PRAGMA temp_store = MEMORY;
                      PRAGMA synchronous = 0;
            ",
            (),
        )
        .unwrap();
        conn.execute("CREATE TABLE IF NOT EXISTS wheels (wheel BLOB)", ())
            .unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS entries (timestamp INTEGER, data BLOB)",
            (),
        )
        .unwrap();
    }

    pub fn watermark(&self) -> u64 {
        self.wheel.watermark()
    }

    #[inline]
    pub fn insert(&mut self, entry: impl Into<Entry<A::Input>>)
    where
        A::Input: Into<Value>,
    {
        let entry = entry.into();
        self.conn
            .execute(
                "INSERT INTO entries (timestamp, data) values (?1, ?2)",
                params![Value::Integer(entry.timestamp as i64), entry.data.into()],
            )
            .unwrap();
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
        let bytes = to_allocvec(&self.wheel).unwrap();
        let compressed = lz4_flex::compress_prepend_size(&bytes);
        println!("Storing BLOB as {} compressed bytes", compressed.len());
        let value = Value::from(compressed);
        self.conn
            .execute("INSERT INTO wheels (wheel) VALUES (?1)", vec![value])
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use awheel_core::{aggregator::sum::I32SumAggregator, time::NumericalDuration};

    use super::*;

    #[test]
    fn basic_db_test() {
        let mut db: WheelDB<I32SumAggregator> = WheelDB::open_in_memory();
        db.insert(Entry::new(10, 1000));
        db.advance(1.seconds());
        db.checkpoint();
    }
}
