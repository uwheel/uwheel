//! µWheel is an embeddable aggregate management system for hybrid stream and analytical processing.
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

pub use uwheel_core::*;

#[cfg(feature = "tree")]
pub use uwheel_tree as tree;

#[cfg(feature = "stats")]
pub use uwheel_stats as stats;
