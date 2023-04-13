use rocksdb::{WriteBatch, WriteOptions, DB};
use std::{cell::RefCell, rc::Rc};

const TOTAL_PREFIX: &str = "total_";

// Disable WAL for now
fn default_write_opts() -> WriteOptions {
    let mut res = WriteOptions::default();
    res.disable_wal(true);
    res
}

/// A Log-structured Merge Wheel
#[derive(Clone)]
pub struct LSMWheel {
    cf: String,
    db: Rc<RefCell<DB>>,
    capacity: usize,
    num_slots: usize,
    rotation_count: usize,
    head: usize,
    tail: usize,
}

impl LSMWheel {
    pub fn new(len: usize, capacity: usize, db: Rc<RefCell<DB>>, cf: String) -> Self {
        Self {
            cf,
            db,
            capacity,
            num_slots: len,
            rotation_count: 0,
            head: 0,
            tail: 0,
        }
    }
    pub fn get(&self, key: impl AsRef<[u8]>, slot: usize) -> Option<Vec<u8>> {
        let db = self.db.borrow();
        let cf = db.cf_handle(&self.cf).unwrap();
        let index = self.slot_idx_from_head(slot);
        let mut slot_key = Vec::new();
        slot_key.extend_from_slice(&index.to_le_bytes());
        slot_key.extend_from_slice(key.as_ref());
        db.get_cf(cf, &slot_key).unwrap()
    }

    /// Returns `true` if the wheel is empty or `false` if it contains slots
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tail == self.head
    }

    /// Returns the current rotation position in the wheel
    pub fn rotation_count(&self) -> usize {
        self.rotation_count
    }
    fn remove_prefix(&self, prefix: impl AsRef<[u8]>) {
        let prefix = prefix.as_ref();
        let db = self.db.borrow();
        let cf = db.cf_handle(&self.cf).unwrap();

        // NOTE: this only works assuming the column family is lexicographically ordered (which is
        // the default, so we don't explicitly set it, see Options::set_comparator)
        let start = prefix;
        // delete_range deletes all the entries in [start, end) range, so we can just increment the
        // least significant byte of the prefix
        let mut end = start.to_vec();
        *end.last_mut()
            .expect("unreachable, the empty case is covered a few lines above") += 1;

        let mut wb = WriteBatch::default();
        wb.delete_range_cf(cf, start, &end);

        // TODO: handle errors
        db.write_opt(wb, &default_write_opts()).unwrap();
    }

    /// Shift the tail and clear any old entry
    #[inline]
    fn clear_tail(&mut self) {
        if !self.is_empty() {
            let tail = self.tail;
            self.tail = self.wrap_add(self.tail, 1);
            self.remove_prefix(tail.to_le_bytes());
        }
    }

    /// Ticks left until the wheel fully rotates
    #[inline]
    pub fn ticks_remaining(&self) -> usize {
        self.num_slots - self.rotation_count
    }
    /// Check whether this wheel is utilising all its slots
    pub fn is_full(&self) -> bool {
        // + 1 as we want to maintain num_slots of history at all times
        (self.num_slots + 1) - self.len() == 1
    }

    /// Tick the wheel by 1 slot
    #[inline]
    pub fn tick(&mut self) -> Option<String> {
        // If the wheel is full, we clear the oldest slot
        if self.is_full() {
            self.clear_tail();
        }

        // shift head of slots
        self.head = self.wrap_add(self.head, 1);

        self.rotation_count += 1;

        if self.rotation_count == self.num_slots {
            self.rotation_count = 0;
            //let db = self.db.borrow();
            //let cf = db.cf_handle(&self.cf).unwrap();
            //let iter = db.prefix_iterator_cf(cf, TOTAL_PREFIX);
            Some(self.cf.to_owned())
        } else {
            None
        }
    }

    pub fn merge_at(&self, slot_idx: usize, key: &[u8], entry: &[u8]) {
        let db = self.db.borrow();
        let cf = db.cf_handle(&self.cf).unwrap();
        let mut prefix_key = Vec::new();
        prefix_key.extend_from_slice(&slot_idx.to_le_bytes());
        prefix_key.extend_from_slice(key.as_ref());
        db.merge_cf(cf, &prefix_key, entry.as_ref()).unwrap();
    }
    pub fn merge_total(&self, key: &[u8], entry: &[u8]) {
        let db = self.db.borrow();
        let cf = db.cf_handle(&self.cf).unwrap();
        let mut prefix_key = Vec::new();
        prefix_key.extend_from_slice(TOTAL_PREFIX.as_bytes());
        prefix_key.extend_from_slice(key.as_ref());
        db.merge_cf(cf, &prefix_key, entry.as_ref()).unwrap();
    }

    /// Attempts to write `entry` into the Wheel
    #[inline]
    pub fn write_ahead(&self, addend: usize, key: impl AsRef<[u8]>, entry: impl AsRef<[u8]>) {
        let slot_idx = self.slot_idx_forward_from_head(addend);
        // merge at correct wheel slot
        self.merge_at(slot_idx, key.as_ref(), entry.as_ref());
        // merge the total of this key
        self.merge_total(key.as_ref(), entry.as_ref());
    }
    pub(crate) fn slot_idx_from_head(&self, subtrahend: usize) -> usize {
        self.wrap_sub(self.head, subtrahend)
    }

    /// How many write ahead slots are available
    #[inline]
    pub(crate) fn write_ahead_len(&self) -> usize {
        let diff = self.len();
        self.capacity - diff
    }

    /// Check whether this wheel can write ahead by ´addend` slots
    pub(crate) fn can_write_ahead(&self, addend: u64) -> bool {
        // Problem: if the wheel is full length and addend slots wraps around the tail, then
        // we will update valid historic slots with future aggregates. We should not allow this.
        addend as usize <= self.write_ahead_len()
    }

    /// Locate slot id `addend` forward
    fn slot_idx_forward_from_head(&self, addend: usize) -> usize {
        self.wrap_add(self.head, addend)
    }

    /// Returns the index in the underlying buffer for a given logical element
    /// index + addend.
    #[inline]
    fn wrap_add(&self, idx: usize, addend: usize) -> usize {
        wrap_index(idx.wrapping_add(addend), self.capacity)
    }

    /// Returns the index in the underlying buffer for a given logical element
    /// index - subtrahend.
    #[inline]
    fn wrap_sub(&self, idx: usize, subtrahend: usize) -> usize {
        wrap_index(idx.wrapping_sub(subtrahend), self.capacity)
    }
    pub fn len(&self) -> usize {
        count(self.tail, self.head, self.capacity)
    }
}

// Functions below are adapted from Rust's VecDeque impl

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
