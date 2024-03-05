//! This crate contains various data structures useful for quickly performing interval
//! queries or modifications in some array.
//!
//! The data structures in this crate are all trees. The trees are represented using an
//! array for high performance and low memory usage.
//! Despite the name of the crate, not every tree in this crate is a segment tree.
//!
//! The [`SegmentPoint`] data structure allows interval queries and point updates to an
//! array in logaritmic time. It is well known for solving the [range minimum query][1]
//! problem. This is the data structure traditionally known as a segment tree.
//!
//! The [`PointSegment`] data structure allows point queries and interval updates to an
//! array in logaritmic time.
//!
//! The [`PrefixPoint`] data structure is a weaker version of the [`SegmentPoint`] data
//! structure, since the intervals must start at the index `0` and the operator must be
//! [commutative][2].  However it has the advantage that it uses half the memory and the
//! size of the array can be changed. This data structure is also known as a
//! [fenwick tree][3].
//!
//! The segment tree in this crate is inspired by [this blog post][4].
//!
//! The similar crate [`prefix-sum`] might also be of interest.
//!
//! This crate has the optional feature [`num-bigint`], which implements the operations in
//! [`ops`] for [`BigInt`] and [`BigUint`].
//!
//! [1]: https://en.wikipedia.org/wiki/Range_minimum_query
//! [2]: ops/trait.Commutative.html
//! [3]: https://en.wikipedia.org/wiki/Fenwick_tree
//! [4]: https://codeforces.com/blog/entry/18051
//! [`SegmentPoint`]: struct.SegmentPoint.html
//! [`PointSegment`]: struct.PointSegment.html
//! [`PrefixPoint`]: struct.PrefixPoint.html
//! [`num-bigint`]: https://crates.io/crates/num-bigint
//! [`BigInt`]: https://docs.rs/num-bigint/0.2/num_bigint/struct.BigInt.html
//! [`BigUint`]: https://docs.rs/num-bigint/0.2/num_bigint/struct.BigUint.html
//! [`prefix-sum`]: https://crates.io/crates/prefix-sum
//! [`ops`]: ops/index.html

#![allow(clippy::all)] // Ignores all Clippy lints

pub mod maybe_owned;
pub mod ops;
pub use crate::{fenwick::PrefixPoint, propagating::PointSegment, segment_tree::SegmentPoint};

mod fenwick;
mod propagating;
mod segment_tree;
