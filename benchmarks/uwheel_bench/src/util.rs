use awheel::{time_internal as time, OffsetDateTime};
use duckdb::{params, Connection, DropBehavior, Result};
use std::{cmp, time::SystemTime};

const MAX_FARE: u64 = 1000;
pub const MAX_KEYS: u64 = 263;

// max 60 seconds ahead of watermark
const MAX_TIMESTAMP_DIFF_MS: u64 = 60000;

pub fn get_random_int_key() -> u32 {
    fastrand::u32(0..1000000)
}

// 2023-10-01 00:00:00
pub const START_DATE: u64 = 1696111200;
pub const START_DATE_MS: u64 = START_DATE * 1000;
// 2023-10-08 00:00:00
pub const END_DATE: u64 = 1696716000;
pub const END_DATE_SHORT: u64 = 1696204800;

// NYC Taxi
pub fn get_random_fare_amount() -> u64 {
    fastrand::u64(0..MAX_FARE)
}
// generate a random timestamp above current watermark
pub fn generate_timestamp(watermark: u64) -> u64 {
    fastrand::u64(watermark..(watermark + MAX_TIMESTAMP_DIFF_MS))
}

// generate random pickup location
pub fn pu_location_id() -> u64 {
    fastrand::u64(0..MAX_KEYS)
}
pub fn rate_code_id() -> u64 {
    fastrand::u64(1..=6)
}

#[derive(Debug, Clone)]
pub struct RideData {
    /// TLC Taxi Zone in which the taximeter was engaged (e.g., "EWR, Newark Airport")
    pub pu_location_id: u64,
    /// The final rate code in effect at the end of the trip.
    /// 1 = Standard rate
    /// 2 = JFK
    /// 3 = Newark
    /// 4 = Nassau or Westchester
    /// 5 = Negotiated fare
    /// 6 = Group ride
    pub rate_code_id: u64,
    /// The time when the meter was engaged.
    pub pu_time: u64,
    /// The time when the meter was disengaged.
    pub do_time: u64,
    // fact (measure)
    pub fare_amount: u64,
}

impl RideData {
    pub fn generate(watermark: u64) -> Self {
        Self {
            pu_location_id: pu_location_id(),
            rate_code_id: rate_code_id(),
            pu_time: 1000,
            do_time: generate_timestamp(watermark),
            fare_amount: get_random_fare_amount(),
        }
    }
    pub fn with_timestamp(timestamp: u64) -> Self {
        Self {
            pu_location_id: pu_location_id(),
            rate_code_id: rate_code_id(),
            pu_time: 1000,
            do_time: timestamp,
            fare_amount: get_random_fare_amount(),
        }
    }
}

pub struct DataGenerator;

impl DataGenerator {
    pub fn generate_batches_random(total_batches: usize, batch_size: usize) -> Vec<Vec<RideData>> {
        let mut batches = Vec::new();
        let watermark = 1000u64;
        for _i in 0..total_batches {
            let batch = generate_ride_data(watermark, batch_size);
            batches.push(batch);
        }
        batches
    }

    pub fn generate_query_data(
        start_date: u64,
        events_per_sec: usize,
        total_seconds: usize,
    ) -> (u64, Vec<Vec<RideData>>) {
        let mut watermark = start_date;
        let mut batches = Vec::new();
        for _sec in 0..total_seconds {
            let mut batch = Vec::with_capacity(events_per_sec);
            for _ in 0..events_per_sec {
                let ride = RideData::with_timestamp(watermark);
                batch.push(ride);
            }
            batches.push(batch);
            watermark += 1000;
        }
        (watermark, batches)
    }

    pub fn generate_batches(events_per_min: usize) -> Vec<Vec<RideData>> {
        // events per min:
        // goal: generate RideData up to X days.
        //let seven_days_as_secs = 604800;
        //time::Duration::days(7)

        let seven_days_as_mins = 10080;

        let mut watermark = 1000u64;
        let mut batches = Vec::new();
        for _min in 0..seven_days_as_mins {
            let batch = generate_ride_data(watermark, events_per_min);
            batches.push(batch);
            watermark += 60000u64; // bump by 1 min
        }
        batches
    }
}

// #[derive(Clone)]
// pub enum QueryV {
//     Q1
// }

#[derive(Clone)]
pub enum QueryType {
    /// Point query
    Keyed(u64),
    /// OLAP query
    All,
    Range(u64, u64),
    TopK,
}
impl Default for QueryType {
    fn default() -> Self {
        Self::new()
    }
}
impl QueryType {
    pub fn new() -> Self {
        let pick = fastrand::u8(0..2);
        if pick == 0u8 {
            QueryType::Keyed(pu_location_id())
        } else {
            QueryType::All
        }
    }
    pub fn random() -> Self {
        let pick = fastrand::u8(0..3);
        if pick == 0u8 {
            Self::olap()
        } else if pick == 1 {
            Self::point()
        } else {
            Self::range()
        }
    }
    pub fn olap() -> Self {
        QueryType::All
    }
    pub fn point() -> Self {
        QueryType::Keyed(pu_location_id())
    }
    pub fn range() -> Self {
        let pu_one = pu_location_id();
        let pu_two = pu_location_id();
        if pu_one <= pu_two {
            QueryType::Range(pu_one, pu_two)
        } else {
            QueryType::Range(pu_two, pu_one)
        }
    }
}

#[derive(Clone)]
pub struct Query {
    pub query_type: QueryType,
    pub interval: TimeInterval,
}
impl Query {
    // #[inline]
    // pub fn olap() -> Self {
    //     Self {
    //         query_type: QueryType::olap(),
    //         interval: QueryInterval::generate_olap(),
    //     }
    // }
    // #[inline]
    // pub fn mix() -> Self {
    //     Self {
    //         query_type: QueryType::new(),
    //         interval: QueryInterval::generate_olap(),
    //     }
    // }
    #[inline]
    pub fn q1() -> Self {
        Self {
            query_type: QueryType::olap(),
            interval: TimeInterval::Landmark,
        }
    }

    #[inline]
    pub fn q2_seconds(watermark: u64) -> Self {
        Self {
            query_type: QueryType::olap(),
            interval: TimeInterval::generate_seconds(watermark),
        }
    }

    #[inline]
    pub fn q2_minutes(watermark: u64) -> Self {
        Self {
            query_type: QueryType::olap(),
            interval: TimeInterval::generate_minutes(watermark),
        }
    }

    #[inline]
    pub fn q2_hours(watermark: u64) -> Self {
        Self {
            query_type: QueryType::olap(),
            interval: TimeInterval::generate_hours(watermark),
        }
    }

    #[inline]
    pub fn q3() -> Self {
        Self {
            query_type: QueryType::point(),
            interval: TimeInterval::Landmark,
        }
    }

    #[inline]
    pub fn q4_seconds(watermark: u64) -> Self {
        Self {
            query_type: QueryType::point(),
            interval: TimeInterval::generate_seconds(watermark),
        }
    }

    #[inline]
    pub fn q5() -> Self {
        Self {
            query_type: QueryType::range(),
            interval: TimeInterval::Landmark,
        }
    }

    #[inline]
    pub fn q6_seconds(watermark: u64) -> Self {
        Self {
            query_type: QueryType::range(),
            interval: TimeInterval::generate_seconds(watermark),
        }
    }

    #[inline]
    pub fn q7() -> Self {
        Self {
            query_type: QueryType::TopK,
            interval: TimeInterval::Landmark,
        }
    }

    #[inline]
    pub fn q8(watermark: u64) -> Self {
        Self {
            query_type: QueryType::TopK,
            interval: TimeInterval::generate_seconds(watermark),
        }
    }
}

pub struct QueryGenerator;
impl QueryGenerator {
    pub fn generate_q1(total: usize) -> Vec<Query> {
        (0..total).map(|_| Query::q1()).collect()
    }
    pub fn generate_q2_seconds(total: usize, watermark: u64) -> Vec<Query> {
        (0..total).map(|_| Query::q2_seconds(watermark)).collect()
    }

    pub fn generate_q2_minutes(total: usize, watermark: u64) -> Vec<Query> {
        (0..total).map(|_| Query::q2_minutes(watermark)).collect()
    }

    pub fn generate_q2_hours(total: usize, watermark: u64) -> Vec<Query> {
        (0..total).map(|_| Query::q2_hours(watermark)).collect()
    }

    pub fn generate_q3(total: usize) -> Vec<Query> {
        (0..total).map(|_| Query::q3()).collect()
    }

    pub fn generate_q4_seconds(total: usize, watermark: u64) -> Vec<Query> {
        (0..total).map(|_| Query::q4_seconds(watermark)).collect()
    }

    pub fn generate_q5(total: usize) -> Vec<Query> {
        (0..total).map(|_| Query::q5()).collect()
    }

    pub fn generate_q6_seconds(total: usize, watermark: u64) -> Vec<Query> {
        (0..total).map(|_| Query::q6_seconds(watermark)).collect()
    }
    pub fn generate_q7(total: usize) -> Vec<Query> {
        (0..total).map(|_| Query::q7()).collect()
    }
    pub fn generate_q8(total: usize, watermark: u64) -> Vec<Query> {
        (0..total).map(|_| Query::q8(watermark)).collect()
    }
}

#[derive(Clone)]
pub enum TimeFilter {
    Seconds(u64, u64),
    Minutes(u64, u64),
    Hours(u64, u64),
    Days(u64, u64),
    Landmark,
}

#[derive(Clone)]
pub enum TimeInterval {
    Range(u64, u64),
    Landmark,
}

impl TimeInterval {
    pub fn generate_seconds(watermark: u64) -> Self {
        let (start, end) = generate_seconds_range(watermark);
        Self::Range(start, end)
    }

    pub fn generate_minutes(watermark: u64) -> Self {
        let (start, end) = generate_minutes_range(watermark);
        // let min_offset = into_offset_date_time_start_end(start, end);
        // dbg!(min_offset);
        Self::Range(start, end)
    }

    pub fn generate_hours(watermark: u64) -> Self {
        let (start, end) = generate_hours_range(watermark);
        // let hr_offset = into_offset_date_time_start_end(start, end);
        // dbg!(hr_offset);
        Self::Range(start, end)
    }
}

#[derive(Clone)]
pub enum QueryInterval {
    Seconds(u32),
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Weeks(u32),
    Landmark,
}

impl QueryInterval {
    pub fn max_seconds() -> u64 {
        // time::Duration::weeks(52).whole_seconds() as u64
        time::Duration::days(7).whole_seconds() as u64
    }
    pub fn generate_stream() -> Self {
        let gran = fastrand::usize(0..2);
        if gran == 0 {
            QueryInterval::Seconds(fastrand::u32(1..60))
        } else {
            QueryInterval::Minutes(fastrand::u32(1..60))
        }
    }
    pub fn generate_olap() -> Self {
        let gran = fastrand::usize(0..3);
        if gran == 0 {
            QueryInterval::Hours(fastrand::u32(1..24))
        } else if gran == 1 {
            QueryInterval::Days(fastrand::u32(1..7))
        } else {
            QueryInterval::Weeks(fastrand::u32(1..52))
        }
    }
    pub fn generate_random() -> Self {
        let gran = fastrand::usize(0..6);
        if gran == 0 {
            QueryInterval::Seconds(fastrand::u32(1..60))
        } else if gran == 1 {
            QueryInterval::Minutes(fastrand::u32(1..60))
        } else if gran == 2 {
            QueryInterval::Hours(fastrand::u32(1..24))
        } else if gran == 3 {
            QueryInterval::Days(fastrand::u32(1..7))
        } else if gran == 4 {
            QueryInterval::Weeks(fastrand::u32(1..52))
        } else {
            QueryInterval::Landmark
        }
    }
}
pub fn generate_ride_data(watermark: u64, size: usize) -> Vec<RideData> {
    (0..size).map(|_| RideData::generate(watermark)).collect()
}

pub fn duckdb_append_streaming(batch: Vec<RideData>, db: &mut Connection) -> Result<()> {
    let mut tx = db.transaction()?;
    tx.set_drop_behavior(DropBehavior::Commit);
    let mut app = tx.appender("rides")?;

    for (i, ride) in batch.into_iter().enumerate() {
        app.append_row(params![
            i as i32,
            ride.pu_location_id,
            ride.do_time,
            ride.fare_amount,
        ])?;
        // flush per append for per-record streaming ingestion..
        // Will be slow but that is how to make the record available for analytics instantly.
        app.flush();
    }
    Ok(())
}

pub fn duckdb_append_batch(batch: Vec<RideData>, db: &mut Connection, keyed: bool) -> Result<()> {
    let mut tx = db.transaction()?;
    tx.set_drop_behavior(DropBehavior::Commit);
    let mut app = tx.appender("rides")?;

    for (i, ride) in batch.into_iter().enumerate() {
        if keyed {
            app.append_row(params![
                i as i32,
                ride.pu_location_id,
                ride.do_time,
                ride.fare_amount,
            ])?;
        } else {
            app.append_row(params![i as i32, ride.do_time, ride.fare_amount,])?;
        }
    }
    app.flush();
    Ok(())
}
pub fn duckdb_query_topn(query: &str, db: &Connection) -> Result<()> {
    let mut stmt = db.prepare(query)?;
    let _sum_res = stmt.query_map([], |row| {
        let res: Result<f64> = row.get(0);
        assert!(res.is_ok());
        Ok(())
    })?;
    Ok(())
}
pub fn duckdb_query_sum(query: &str, db: &Connection) -> Result<()> {
    let mut stmt = db.prepare(query)?;
    let _sum_res = stmt.query_map([], |row| {
        let sum: u64 = row.get(0).unwrap_or(0);
        Ok(sum)
    })?;
    for res in _sum_res {
        #[cfg(feature = "debug")]
        dbg!(&res);

        assert!(res.is_ok());
    }
    Ok(())
}

pub fn duckdb_query_all(query: &str, db: &Connection) -> Result<()> {
    let mut stmt = db.prepare(query)?;
    let _sum_res = stmt.query_map([], |row| {
        let avg: f64 = row.get(0).unwrap_or(0.0);
        let sum: f64 = row.get(1).unwrap_or(0.0);
        let min: f64 = row.get(2).unwrap_or(0.0);
        let max: f64 = row.get(3).unwrap_or(0.0);
        let count: f64 = row.get(4).unwrap_or(0.0);
        Ok((avg, sum, min, max, count))
    })?;
    for res in _sum_res {
        assert!(res.is_ok());
    }
    Ok(())
}

pub fn duckdb_setup(disk: bool, threads: usize, keyed: bool) -> (duckdb::Connection, &'static str) {
    let (db, id) = if disk {
        (
            duckdb::Connection::open("duckdb_ingestion.db").unwrap(),
            "duckdb [disk]",
        )
    } else {
        (
            duckdb::Connection::open_in_memory().unwrap(),
            "duckdb [memory]",
        )
    };
    let create_table_sql = if keyed {
        "
        create table IF NOT EXISTS rides
        (
            id INTEGER not null, -- primary key,
            pu_location_id UBIGINT not null,
            do_time UBIGINT not null,
            fare_amount UBIGINT not null,
        );"
    } else {
        "
        create table IF NOT EXISTS rides
        (
            id INTEGER not null, -- primary key,
            do_time UBIGINT not null,
            fare_amount UBIGINT not null,
        );"
    };

    db.execute_batch(create_table_sql).unwrap();

    duckdb_set_threads(threads, &db);

    #[cfg(feature = "duckdb_index")]
    {
        // Index on timestamp we are querying
        let index_sql = "create index idx_timestamp ON rides(do_time);";
        db.execute_batch(index_sql).unwrap();

        // Index on key for point queries
        // let index_sql = "create index idx_location ON rides(pu_location_id);";
        // db.execute_batch(index_sql).unwrap();
    }

    (db, id)
}

pub struct DuckDBInfo {
    pub database_size: String,
    pub block_size: u64,
    pub memory_usage: String,
}

pub fn duckdb_memory_usage(conn: &duckdb::Connection) -> DuckDBInfo {
    let pragma_str = "CALL pragma_database_size()";

    let mut stmt = conn.prepare(pragma_str).unwrap();
    let mut pragma_res = stmt
        .query_map([], |row| {
            let database_size: String = row.get(1).unwrap();
            let block_size: u64 = row.get(2).unwrap();
            let memory_usage: String = row.get(7).unwrap();
            // println!(
            //     "Name: {} database_size: {} block_size: {} memory_usage: {}",
            //     name, database_size, block_size, memory_usage
            // );

            Ok(DuckDBInfo {
                database_size,
                block_size,
                memory_usage,
            })
        })
        .unwrap();

    pragma_res.next().unwrap().unwrap()
}
pub fn duckdb_set_threads(threads: usize, conn: &duckdb::Connection) {
    let thread_str = format!("SET threads TO {};", threads);
    conn.execute_batch(&thread_str).unwrap();
}

pub fn generate_seconds_range(watermark: u64) -> (u64, u64) {
    // Specify the date range (2023-10-01 to watermark)
    let start_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(START_DATE);
    let end_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(watermark / 1000);

    // Convert dates to Unix timestamps
    let start_timestamp = start_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let end_timestamp = end_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Randomly generate a start time within the specified date range
    let random_start = fastrand::u64(start_timestamp..end_timestamp);

    // Generate a random duration between 1 and (watermark - random_start_seconds) seconds
    let max_duration = end_timestamp - random_start;
    let duration_seconds = fastrand::u64(1..=max_duration);

    (random_start, random_start + duration_seconds)
}

pub fn generate_minutes_range(watermark: u64) -> (u64, u64) {
    // Specify the date range (2023-10-01 to watermark)
    let start_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(START_DATE);
    let end_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(watermark / 1000);

    // Convert dates to Unix timestamps
    let start_timestamp = start_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let end_timestamp = end_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Randomly generate a start time within the specified date range
    let random_start_minutes = fastrand::u64(start_timestamp..end_timestamp) / 60;

    // Set seconds to 00 for the start time
    let start_time_seconds = random_start_minutes * 60;

    // Generate a random duration between 1 and (10080 - random_start_minutes) minutes
    let max_duration = (end_timestamp / 60) - random_start_minutes;

    let duration_minutes = fastrand::u64(1..=max_duration);
    let end_time_seconds = start_time_seconds + duration_minutes * 60;

    let end_time_seconds = cmp::min(end_time_seconds, end_timestamp);

    (start_time_seconds, end_time_seconds)
}

pub fn generate_hours_range(watermark: u64) -> (u64, u64) {
    // Specify the date range (2023-10-01 to watermark)
    let start_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(START_DATE);
    let end_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(watermark / 1000);

    // Convert dates to Unix timestamps
    let start_timestamp = start_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let end_timestamp = end_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Randomly generate a start time within the specified date range
    let random_start_hours = fastrand::u64(start_timestamp..end_timestamp) / 3600;

    // Set seconds to 00 for the start time
    let start_time_seconds = random_start_hours * 3600;

    // Generate a random duration between 1 and (10080 - random_start_minutes) minutes
    let max_duration = (end_timestamp / 3600) - random_start_hours;

    let duration_hours = fastrand::u64(1..=max_duration);
    let end_time_seconds = start_time_seconds + duration_hours * 3600;

    let end_time_seconds = std::cmp::min(end_time_seconds, end_timestamp);

    (start_time_seconds, end_time_seconds)
}

pub fn generate_days_range(watermark: u64) -> (u64, u64) {
    // Specify the date range (2023-10-01 to watermark)
    let start_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(START_DATE);
    let end_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(watermark / 1000);

    // Convert dates to Unix timestamps
    let start_timestamp = start_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let end_timestamp = end_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Randomly generate a start time within the specified date range
    let random_start_days = fastrand::u64(start_timestamp..end_timestamp) / 86400;

    // Set seconds to 00 for the start time
    let start_time_seconds = random_start_days * 86400;

    // Generate a random duration between 1 and (10080 - random_start_minutes) minutes
    let max_duration = (end_timestamp / 86400) - random_start_days;

    let duration_days = fastrand::u64(0..=max_duration);
    let end_time_seconds = start_time_seconds + duration_days * 86400;

    let end_time_seconds = cmp::min(end_time_seconds, end_timestamp);

    (start_time_seconds, end_time_seconds)
}

pub fn into_offset_date_time_start_end(start: u64, end: u64) -> (OffsetDateTime, OffsetDateTime) {
    (
        OffsetDateTime::from_unix_timestamp(start as i64).unwrap(),
        OffsetDateTime::from_unix_timestamp(end as i64).unwrap(),
    )
}
