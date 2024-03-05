# segment-tree

[![Documentation](https://docs.rs/segment-tree/badge.svg)](https://docs.rs/segment-tree)
[![Cargo](https://img.shields.io/crates/v/segment-tree.svg)](https://crates.io/crates/segment-tree)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/Darksonn/segment-tree/blob/master/LICENSE)

**NOTE:** This repo has been forked and modified to include the calculation of aggregate operations for the benchmarks

See original repo [here](https://github.com/Darksonn/segment-tree/blob/master).

This crate contains various data structures useful for quickly performing
interval queries or modifications in some array.

The data structures in this crate are all trees. The trees are represented using
an array for high performance and low memory usage.  Despite the name of the
crate, not every tree in this crate is a segment tree.

## Cargo.toml

```toml
[dependencies]
segment-tree = "2"
```

## Example

The example below shows a segment tree, which allows queries to any interval in logaritmic
time.

```rust
use segment_tree::SegmentPoint;
use segment_tree::ops::Min;

let array = vec![4, 3, 2, 1, 2, 3, 4];
let mut tree = SegmentPoint::build(array, Min);

// Compute the minimum of the whole array.
assert_eq!(tree.query(0, tree.len()), 1);

// Compute the minimum of part of the array.
assert_eq!(tree.query(0, 3), 2);

// Change the 1 into a 10.
tree.modify(3, 10);

// The minimum of the whole array is now 2.
assert_eq!(tree.query(0, tree.len()), 2);
```

This crate also has a [`PointSegment`][1] type, which instead allows modifications to
intervals.

The related crate [`prefix_sum`][2] might also be of interest.

  [1]: https://docs.rs/segment-tree/2/segment_tree/struct.PointSegment.html
  [2]: https://crates.io/crates/prefix-sum
