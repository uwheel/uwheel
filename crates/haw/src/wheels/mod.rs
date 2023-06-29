/// Aggregation Wheel based on a fixed-sized circular buffer
///
/// This is the core data structure that is reused between different hierarchies (e.g., seconds, minutes, hours, days)
pub mod aggregation;
/// Hierarchical Aggregation Wheel (HAW)
pub mod wheel;
/// Wheels that are tailored for Sliding Window Aggregation
pub mod window;
/// Write-ahead Wheel
pub mod write_ahead;

pub use aggregation::{AggregationWheel, MaybeWheel};
pub use wheel::Wheel;

// Helper functions for common wheel operations

/// Returns the index in the underlying buffer for a given logical element index.
#[inline]
fn wrap_index(index: usize, size: usize) -> usize {
    // size is always a power of 2
    debug_assert!(size.is_power_of_two());
    index & (size - 1)
}

/// Calculate the number of elements left to be read in the buffer
#[inline]
fn count(tail: usize, head: usize, size: usize) -> usize {
    // size is always a power of 2
    (head.wrapping_sub(tail)) & (size - 1)
}
/// Locate slot id `addend` forward
#[inline]
fn slot_idx_forward_from_head(head: usize, addend: usize, capacity: usize) -> usize {
    wrap_add(head, addend, capacity)
}

/// Locate slot id `subtrahend` backward
#[inline]
fn slot_idx_backward_from_head(head: usize, subtrahend: usize, capacity: usize) -> usize {
    wrap_sub(head, subtrahend, capacity)
}

/// Returns the index in the underlying buffer for a given logical element
/// index + addend.
#[inline]
fn wrap_add(idx: usize, addend: usize, capacity: usize) -> usize {
    wrap_index(idx.wrapping_add(addend), capacity)
}

/// Returns the index in the underlying buffer for a given logical element
/// index - subtrahend.
#[inline]
fn wrap_sub(idx: usize, subtrahend: usize, capacity: usize) -> usize {
    wrap_index(idx.wrapping_sub(subtrahend), capacity)
}

/// Returns the current number of used slots (includes empty NONE slots as well)
fn len(tail: usize, head: usize, capacity: usize) -> usize {
    count(tail, head, capacity)
}
