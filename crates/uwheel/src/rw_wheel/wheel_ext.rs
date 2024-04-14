/// Extension trait for becoming a wheel
pub trait WheelExt {
    /// Returns the current head of the wheel
    fn head(&self) -> usize;
    /// Returns the current tail of the wheel
    fn tail(&self) -> usize;
    /// Returns the total number of slots which may be bigger than `Self::capacity`
    fn num_slots(&self) -> usize;
    /// Returns the number of wheel slots
    fn capacity(&self) -> usize;

    /// Returns the size of the wheel in bytes or `None` if it cannot be computed
    fn size_bytes(&self) -> Option<usize> {
        None
    }

    /// Returns `true` if the wheel is empty or `false` if it contains slots
    fn is_empty(&self) -> bool {
        self.tail() == self.head()
    }

    /// Returns the current number of used slots
    fn len(&self) -> usize {
        count(self.tail(), self.head(), self.num_slots())
    }

    /// Locate slot id `addend` forward
    #[inline]
    fn slot_idx_forward_from_head(&self, addend: usize) -> usize {
        self.wrap_add(self.head(), addend)
    }

    /// Locate slot id `subtrahend` backward
    #[inline]
    fn slot_idx_backward_from_head(&self, subtrahend: usize) -> usize {
        self.wrap_sub(self.head(), subtrahend)
    }

    /// Returns the index in the underlying buffer for a given logical element
    /// index + addend.
    #[inline]
    fn wrap_add(&self, idx: usize, addend: usize) -> usize {
        wrap_index(idx.wrapping_add(addend), self.num_slots())
    }

    /// Returns the index in the underlying buffer for a given logical element
    /// index - subtrahend.
    #[inline]
    fn wrap_sub(&self, idx: usize, subtrahend: usize) -> usize {
        wrap_index(idx.wrapping_sub(subtrahend), self.num_slots())
    }
}

/// Returns the index in the underlying buffer for a given logical element index.
#[inline]
pub(crate) fn wrap_index(index: usize, size: usize) -> usize {
    // size is always a power of 2
    debug_assert!(size.is_power_of_two());
    index & (size - 1)
}

/// Calculate the number of elements left to be read in the buffer
#[inline]
pub(crate) fn count(tail: usize, head: usize, size: usize) -> usize {
    // size is always a power of 2
    (head.wrapping_sub(tail)) & (size - 1)
}
