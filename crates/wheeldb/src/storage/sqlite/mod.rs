use core::marker::PhantomData;

use awheel::{time::Duration, Aggregator, Entry, ReadWheel};
use core::{borrow::Borrow, ops::RangeBounds};
use postcard::to_allocvec;
use sqlite::Connection;
use std::string::String;

use super::Storage;

// NOTE: SQLite commands for future reference
// conn.execute("CREATE TABLE IF NOT EXISTS wheels (id TEXT, wheel BLOB)", ())
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
//     .unwrap();#[allow(dead_code)]

pub struct SQLite<K: Ord + PartialEq, A: Aggregator> {
    id: String,
    connection: Connection,
    _marker: PhantomData<(K, A)>,
}
impl<K: Ord + PartialEq, A: Aggregator> SQLite<K, A> {
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        let connection = sqlite::open(&id).unwrap();
        Self::setup(&connection);
        Self {
            id: id.clone(),
            connection,
            _marker: PhantomData,
        }
    }

    fn setup(conn: &Connection) {
        conn.execute(
            "PRAGMA journal_mode = OFF;
              PRAGMA temp_store = MEMORY;
              PRAGMA synchronous = 0;
            ",
        )
        .unwrap();
        conn.execute("CREATE TABLE IF NOT EXISTS wheels (id TEXT, wheel BLOB)")
            .unwrap();
    }
}

impl<K: Ord + PartialEq, A: Aggregator> Storage<K, A> for SQLite<K, A> {
    #[inline]
    fn insert_wal(&self, _entry: &Entry<A::Input>) {
        // TODO
    }
    #[inline]
    fn add_wheel(&self, _key: K, _wheel: &ReadWheel<A>) {
        let bytes = to_allocvec(_wheel).unwrap();
        let compressed_blob = lz4_flex::compress_prepend_size(&bytes);
        let query = "INSERT INTO wheels (id, wheel) VALUES (?, ?)";
        std::println!("Adding compressed wheel of {} bytes", compressed_blob.len());
        let mut statement = self.connection.prepare(query).unwrap();
        statement.bind((1, "test")).unwrap();
        statement.bind((2, &compressed_blob[..])).unwrap();
        statement.next().unwrap();
    }
    fn get<Q>(&self, _key: &Q) -> Option<ReadWheel<A>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        unimplemented!();
    }

    fn landmark_range<Q, R>(&self, _range: R) -> Option<<A as Aggregator>::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        unimplemented!();
    }

    fn interval_range<Q, R>(&self, _interval: Duration, _range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: ?Sized + Ord + PartialEq,
    {
        unimplemented!();
    }
}
