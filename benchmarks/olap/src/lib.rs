use duckdb::{params, Connection, DropBehavior, Result};
use haw::time;

const MAX_FARE: u64 = 1000;

// max 60 seconds ahead of watermark
const MAX_TIMESTAMP_DIFF_MS: u64 = 60000;

pub fn get_random_int_key() -> u32 {
    fastrand::u32(0..1000000)
}

// NYC Taxi
pub fn get_random_fare_amount() -> f64 {
    fastrand::u64(0..MAX_FARE) as f64
}
// generate a random timestamp above current watermark
pub fn generate_timestamp(watermark: u64) -> u64 {
    fastrand::u64(watermark..(watermark + MAX_TIMESTAMP_DIFF_MS))
}

// generate random pickup location
pub fn pu_location_id() -> u64 {
    fastrand::u64(1..263)
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
    pub fare_amount: f64,
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

    pub fn generate_query_data(events_per_min: usize) -> (u64, Vec<Vec<RideData>>) {
        let seven_days_as_mins = 10080;

        let mut watermark = 1000u64;
        let mut batches = Vec::new();
        for _min in 0..seven_days_as_mins {
            let batch = generate_ride_data(watermark, events_per_min);
            batches.push(batch);
            watermark += 60000u64; // bump by 1 min
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

#[derive(Clone)]
pub enum QueryType {
    /// Point query
    Keyed(u64),
    /// OLAP query
    All,
    Range(u64, u64),
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
    pub interval: QueryInterval,
}
impl Query {
    #[inline]
    pub fn olap() -> Self {
        Self {
            query_type: QueryType::olap(),
            interval: QueryInterval::generate_olap(),
        }
    }
    #[inline]
    pub fn mix() -> Self {
        Self {
            query_type: QueryType::new(),
            interval: QueryInterval::generate_olap(),
        }
    }
    #[inline]
    pub fn olap_high_intervals() -> Self {
        Self {
            query_type: QueryType::olap(),
            interval: QueryInterval::generate_olap(),
        }
    }
    #[inline]
    pub fn olap_range_low_intervals() -> Self {
        Self {
            query_type: QueryType::range(),
            interval: QueryInterval::generate_stream(),
        }
    }
    #[inline]
    pub fn olap_range_high_intervals() -> Self {
        Self {
            query_type: QueryType::range(),
            interval: QueryInterval::generate_olap(),
        }
    }
    #[inline]
    pub fn olap_low_intervals() -> Self {
        Self {
            query_type: QueryType::olap(),
            interval: QueryInterval::generate_stream(),
        }
    }
    #[inline]
    pub fn point_queries_high_intervals() -> Self {
        Self {
            query_type: QueryType::point(),
            interval: QueryInterval::generate_olap(),
        }
    }
    #[inline]
    pub fn random_queries_low_intervals() -> Self {
        Self {
            query_type: QueryType::random(),
            interval: QueryInterval::generate_stream(),
        }
    }
    #[inline]
    pub fn random_queries_high_intervals() -> Self {
        Self {
            query_type: QueryType::random(),
            interval: QueryInterval::generate_olap(),
        }
    }
    #[inline]
    pub fn point_queries_low_intervals() -> Self {
        Self {
            query_type: QueryType::point(),
            interval: QueryInterval::generate_stream(),
        }
    }
    #[inline]
    pub fn stream() -> Self {
        Self {
            query_type: QueryType::point(),
            interval: QueryInterval::generate_stream(),
        }
    }
}

pub struct QueryGenerator;
impl QueryGenerator {
    pub fn generate_olap(total: usize) -> Vec<Query> {
        (0..total).map(|_| Query::olap()).collect()
    }
    pub fn generate_low_interval_point_queries(total: usize) -> Vec<Query> {
        (0..total)
            .map(|_| Query::point_queries_low_intervals())
            .collect()
    }
    pub fn generate_high_interval_point_queries(total: usize) -> Vec<Query> {
        (0..total)
            .map(|_| Query::point_queries_high_intervals())
            .collect()
    }
    pub fn generate_low_interval_olap(total: usize) -> Vec<Query> {
        (0..total).map(|_| Query::olap_low_intervals()).collect()
    }
    pub fn generate_high_interval_olap(total: usize) -> Vec<Query> {
        (0..total).map(|_| Query::olap_high_intervals()).collect()
    }
    pub fn generate_streaming(total: usize) -> Vec<Query> {
        (0..total).map(|_| Query::stream()).collect()
    }

    pub fn generate_low_interval_range_queries(total: usize) -> Vec<Query> {
        (0..total)
            .map(|_| Query::olap_range_low_intervals())
            .collect()
    }
    pub fn generate_high_interval_range_queries(total: usize) -> Vec<Query> {
        (0..total)
            .map(|_| Query::olap_range_high_intervals())
            .collect()
    }
    pub fn generate_low_interval_random_queries(total: usize) -> Vec<Query> {
        (0..total)
            .map(|_| Query::random_queries_low_intervals())
            .collect()
    }
    pub fn generate_high_interval_random_queries(total: usize) -> Vec<Query> {
        (0..total)
            .map(|_| Query::random_queries_high_intervals())
            .collect()
    }
}

#[derive(Clone)]
pub enum QueryInterval {
    Seconds(u32),
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Landmark,
}

impl QueryInterval {
    pub fn max_seconds() -> u64 {
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
            QueryInterval::Landmark
        }
    }
    pub fn generate_random() -> Self {
        let gran = fastrand::usize(0..5);
        if gran == 0 {
            QueryInterval::Seconds(fastrand::u32(1..60))
        } else if gran == 1 {
            QueryInterval::Minutes(fastrand::u32(1..60))
        } else if gran == 2 {
            QueryInterval::Hours(fastrand::u32(1..24))
        } else if gran == 3 {
            QueryInterval::Days(fastrand::u32(1..7))
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

pub fn duckdb_append_batch(batch: Vec<RideData>, db: &mut Connection) -> Result<()> {
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
    }
    app.flush();
    Ok(())
}
pub fn duckdb_query_sum(query: &str, db: &Connection) -> Result<()> {
    let mut stmt = db.prepare(query)?;
    let _sum_res = stmt.query_map([], |row| {
        let sum: f64 = row.get(0).unwrap_or(0.0);
        Ok(sum)
    })?;
    for res in _sum_res {
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

pub fn duckdb_setup(disk: bool) -> (duckdb::Connection, &'static str) {
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
    let create_table_sql = "
        create table IF NOT EXISTS rides
        (
            id INTEGER not null, -- primary key,
            pu_location_id UBIGINT not null,
            do_time UBIGINT not null,
            fare_amount FLOAT8 not null,
        );";
    db.execute_batch(create_table_sql).unwrap();
    (db, id)
}
