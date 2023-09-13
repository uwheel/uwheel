use awheel::{Aggregator, Entry, ReadWheel};
use postcard::to_allocvec;
use sqlite::Connection;

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

pub struct SQLite<'a> {
    id: &'a str,
    connection: Connection,
}
impl<'a> SQLite<'a> {
    pub fn new(id: &'a str) -> Self {
        let connection = sqlite::open(&id).unwrap();
        Self::setup(&connection);
        Self { id, connection }
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

impl<'a> Storage for SQLite<'a> {
    #[inline]
    fn insert_wal<A>(&self, _entry: &Entry<A::Input>)
    where
        A: Aggregator,
    {
        // TODO
    }
    #[inline]
    fn sync<A>(&self, wheel: &ReadWheel<A>)
    where
        A: Aggregator,
    {
        let bytes = to_allocvec(wheel).unwrap();
        let compressed_blob = lz4_flex::compress_prepend_size(&bytes);
        let query = "INSERT INTO wheels (id, wheel) VALUES (?, ?)";
        std::println!("Adding compressed wheel of {} bytes", compressed_blob.len());
        let mut statement = self.connection.prepare(query).unwrap();
        statement.bind((1, self.id)).unwrap();
        statement.bind((2, &compressed_blob[..])).unwrap();
        statement.next().unwrap();
    }
}
