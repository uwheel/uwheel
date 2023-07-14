#![doc = include_str!("../../../README.md")]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

pub use awheel_core::*;

#[cfg(feature = "window")]
pub use awheel_window as window;

#[cfg(feature = "tree")]
pub use awheel_tree as tree;
