//! µWheel is an embeddable aggregate management system for streams and queries.
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(feature = "simd", feature(portable_simd))]
#![cfg_attr(not(feature = "std"), no_std)]
#![deny(nonstandard_style, missing_copy_implementations, missing_docs)]
#![forbid(unsafe_code)]
#![allow(clippy::large_enum_variant, clippy::enum_variant_names)]

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

use core::{fmt, fmt::Debug, write};

mod delta;
mod window;

/// Aggregation interface used by µWheel
///
/// This module also contains a number of pre-defined aggregators (e.g., SUM, ALL, TopK)
pub mod aggregator;
/// Duration of time for µWheel intervals
pub mod duration;
/// Various wheels used by µWheel
pub mod wheels;

pub use delta::DeltaState;
pub use duration::{Duration, NumericalDuration};

#[macro_use]
#[doc(hidden)]
mod macros;

pub use aggregator::Aggregator;
#[doc(hidden)]
pub use time::OffsetDateTime;
pub use wheels::{
    read::{
        hierarchical::{Haw, HawConf, WheelRange},
        ReaderWheel,
        DAYS,
        HOURS,
        MINUTES,
        SECONDS,
        WEEKS,
        YEARS,
    },
    write::WriterWheel,
    Conf,
    RwWheel,
};
pub use window::Window;

/// Entry that can be inserted into µWheel
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
