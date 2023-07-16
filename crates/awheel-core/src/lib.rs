#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

use core::{
    fmt,
    fmt::{Debug, Display},
    matches,
    write,
};

/// Aggregation Interface adopted from the work of [Tangwongsan et al.](http://www.vldb.org/pvldb/vol8/p702-tangwongsan.pdf)
///
/// This module also contains a number of pre-defined aggregators (e.g., SUM, ALL, TopK)
pub mod aggregator;
/// Reader-Writer Wheel
pub mod rw_wheel;
/// Time utilities
///
/// Heavily borrowed from the [time](https://docs.rs/time/latest/time/) crate
pub mod time;

pub use aggregator::Aggregator;
pub use rw_wheel::{
    read::{
        DaysWheel,
        HoursWheel,
        MinutesWheel,
        ReadWheel,
        SecondsWheel,
        WeeksWheel,
        YearsWheel,
        DAYS,
        HOURS,
        MINUTES,
        SECONDS,
        WEEKS,
        YEARS,
    },
    write::WriteAheadWheel,
    Options,
    RwWheel,
};

/// A type containing error variants that may arise when using a wheel
#[derive(Debug)]
pub enum Error<T: Debug> {
    Late {
        entry: Entry<T>,
        watermark: u64,
    },
    Overflow {
        entry: Entry<T>,
        max_write_ahead_ts: u64,
    },
}
impl<T: Debug> Display for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Late { entry, watermark } => {
                write!(f, "late entry {entry} current watermark is {watermark}")
            }
            Error::Overflow {
                entry,
                max_write_ahead_ts,
            } => {
                write!(f, "entry {entry} does not fit within wheel, expected timestamp below {max_write_ahead_ts}")
            }
        }
    }
}

impl<T: Debug> Error<T> {
    pub fn is_late(&self) -> bool {
        matches!(self, Error::Late { .. })
    }
    pub fn is_overflow(&self) -> bool {
        matches!(self, Error::Overflow { .. })
    }
}

/// Entry that can be inserted into the Wheel
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Entry<T: Debug> {
    /// Data to be lifted by the aggregator
    pub data: T,
    /// Event timestamp of this entry
    pub timestamp: u64,
}
impl<T: Debug> Entry<T> {
    pub fn new(data: T, timestamp: u64) -> Self {
        Self { data, timestamp }
    }
}
impl<T: Debug> fmt::Display for Entry<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(data: {:?}, timestamp: {})", self.data, self.timestamp)
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! cfg_rkyv {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "rkyv")]
            $item
        )*
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! assert_capacity {
    ($capacity:tt) => {
        assert!($capacity != 0, "Capacity is not allowed to be zero");
        assert!(
            $capacity.is_power_of_two(),
            "Capacity must be a power of two"
        );
    };
}
