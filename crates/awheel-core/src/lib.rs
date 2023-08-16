//! awheel-core is a sub-crate of ahweel that contains core functionality
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]
#![deny(nonstandard_style, missing_copy_implementations, missing_docs)]
#![forbid(unsafe_code)]

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
    read::{ReadWheel, DAYS, HOURS, MINUTES, SECONDS, WEEKS, YEARS},
    write::WriteAheadWheel,
    Options,
    RwWheel,
};

/// A type containing error variants that may arise when using a wheel
#[derive(Debug)]
pub enum Error<T: Debug> {
    /// The timestamp of the entry is below the watermark and is rejected
    Late {
        /// Owned entry to be returned to the caller
        entry: Entry<T>,
        /// The current watermark
        watermark: u64,
    },
    /// The timestamp of the entry is too far ahead of the watermark
    Overflow {
        /// Owned entry to be returned to the caller
        entry: Entry<T>,
        /// Timestamp signaling the maximum write ahead
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
    /// Returns `true` if the error represents [Error::Late]
    pub fn is_late(&self) -> bool {
        matches!(self, Error::Late { .. })
    }
    /// Returns `true` if the error represents [Error::Overflow]
    pub fn is_overflow(&self) -> bool {
        matches!(self, Error::Overflow { .. })
    }
}

/// Entry that can be inserted into the Wheel
#[repr(C)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Copy, Clone)]
pub struct Entry<T: Debug> {
    /// Data to be lifted by the aggregator
    pub data: T,
    /// Event timestamp of this entry
    pub timestamp: u64,
}
impl<T: Debug> Entry<T> {
    /// Creates a new entry with given data and timestamp
    pub fn new(data: T, timestamp: u64) -> Self {
        Self { data, timestamp }
    }
}
impl<T: Debug> fmt::Display for Entry<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(data: {:?}, timestamp: {})", self.data, self.timestamp)
    }
}
impl<T: Debug> From<(T, u64)> for Entry<T> {
    fn from(val: (T, u64)) -> Self {
        Entry::new(val.0, val.1)
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! capacity_to_slots {
    ($cap:tt) => {
        if $cap.is_power_of_two() {
            $cap
        } else {
            $cap.next_power_of_two()
        }
    };
}
