#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]
#![feature(slice_range)]

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

#[doc(hidden)]
#[macro_use]
pub mod macros;

use core::{
    fmt,
    fmt::{Debug, Display},
    matches,
    option::Option,
    write,
};

/// Aggregation Wheel based on a fixed-sized circular buffer
///
/// This is the core data structure that is reused between different hierarchies (e.g., seconds, minutes, hours, days)
pub mod agg_wheel;
/// Aggregation Interface adopted from the work of [Tangwongsan et al.](http://www.vldb.org/pvldb/vol8/p702-tangwongsan.pdf)
///
/// This module also contains a number of pre-defined aggregators (e.g., SUM)
pub mod aggregator;
/// A Map maintaining a [Wheel] per key
pub mod map;
/// Time utilities
///
/// Heavily borrowed from the [time](https://docs.rs/time/latest/time/) crate
pub mod time;
/// Data types used in this crate
pub mod types;

use aggregator::Aggregator;

mod wheel;

pub use wheel::{
    DaysWheel,
    HoursWheel,
    MinutesWheel,
    SecondsWheel,
    WeeksWheel,
    Wheel,
    YearsWheel,
    DAYS,
    HOURS,
    MINUTES,
    SECONDS,
    WEEKS,
    YEARS,
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
