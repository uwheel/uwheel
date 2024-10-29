//! µWheel is an embeddable aggregate management system for streams and queries.
//!
//! Learn more about the project [here](https://github.com/uwheel/uwheel).
//!
//! ## Feature Flags
//! - `std` (_enabled by default_)
//!    - Enables features that rely on the standard library
//! - `sum` (_enabled by default_)
//!    - Enables sum aggregation
//! - `avg` (_enabled by default_)
//!     - Enables avg aggregation
//! - `min` (_enabled by default_)
//!    - Enables min aggregation
//! - `max` (_enabled by default_)
//!    - Enables max aggregation
//! - `min_max` (_enabled by default_)
//!    - Enables min-max aggregation
//! - `all` (_enabled by default_)
//!    - Enables all aggregation
//! - `top_n`
//!    - Enables Top-N aggregation
//! - `simd` (_requires `nightly`_)
//!    - Enables support to speed up aggregation functions with SIMD operations
//! - `sync` (_implicitly enables `std`_)
//!    - Enables a sync version of ``ReaderWheel`` that can be shared and queried across threads
//! - `profiler` (_implicitly enables `std`_)
//!    - Enables recording of latencies for various operations
//! - `serde`
//!    - Enables serde support
//! - `timer`
//!    - Enables scheduling user-defined functions
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(feature = "simd", feature(portable_simd))]
#![cfg_attr(not(feature = "std"), no_std)]
#![deny(nonstandard_style, missing_copy_implementations, missing_docs)]
#![forbid(unsafe_code)]
#![allow(clippy::large_enum_variant, clippy::enum_variant_names)]
#![doc(html_logo_url = "https://avatars.githubusercontent.com/u/167914012")]
#![doc(html_favicon_url = "https://avatars.githubusercontent.com/u/167914012")]

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

use core::{fmt, fmt::Debug, write};

mod delta;
mod window;

/// Aggregation interface and pre-defined aggregators (e.g., SUM, AVG, Top-N)
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

pub use wheels::{
    read::{
        aggregation::conf::{CompressionPolicy, RetentionPolicy, WheelConf},
        hierarchical::{Haw, HawConf, WheelRange},
    },
    Conf,
    RwWheel,
};
pub use window::{Window, WindowAggregate};

#[doc(hidden)]
pub use time::OffsetDateTime;
#[doc(hidden)]
pub use wheels::read::{DAYS, HOURS, MINUTES, SECONDS, WEEKS, YEARS};

/// Timestamped Entry that can be inserted into µWheel
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
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::Entry;
    ///
    /// let data = 10u32;
    /// let timestamp = 100000;
    /// let entry = Entry::new(data, timestamp);
    /// ```
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
        if $cap.get().is_power_of_two() {
            $cap.get()
        } else {
            $cap.get().next_power_of_two()
        }
    };
}
