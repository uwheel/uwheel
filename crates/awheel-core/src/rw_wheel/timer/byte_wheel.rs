//! This is the fundamental abstraction of this crate: a single hash wheel with 256 slots
//! addressed with a single byte. Each slot stores a list of a generic event type.
//!
//! The whole wheel can be [ticked](ByteWheel::tick) causing entries in the slots that are being moved over
//! to expire. With every tick, all expired event entries are returned for handling.
//!
//! In addition to its entry type, the byte wheel also carries a rest type.
//! This type is used to track how much time remains for this entry in lower hierachy wheels.
//! For example, the highest order wheel in four-level hierarchical hash wheel would carry a rest type
//! of `[u8; 3]`, indicating that there are 3 lower level byte wheels that this entry must still go
//! through before expiring.
//!
//! # Example
//! Example usage of this abstraction can be seen in the source code of the [quad_wheel](crate::wheels::quad_wheel::QuadWheelWithOverflow).
//!
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// A single entry in a slot
pub struct WheelEntry<EntryType, RestType> {
    /// The actual entry
    pub entry: EntryType,
    /// The rest delay associated with the entry
    pub rest: RestType,
}

/// Just a convenience type alias for the list type used in each slot
pub type WheelEntryList<EntryType, RestType> = Vec<WheelEntry<EntryType, RestType>>;

/// Number of slots for each ByteWheel
const NUM_SLOTS: usize = 256;

/// A single wheel with 256 slots of for elements of `EntryType`
///
/// The `RestType` us used to store an array of bytes that are the rest of the delay.
/// This way the same wheel structure can be used at different hierarchical levels.
pub struct ByteWheel<EntryType, RestType> {
    slots: [Option<WheelEntryList<EntryType, RestType>>; NUM_SLOTS],
    count: u64,
    current: u8,
}

impl<EntryType, RestType> ByteWheel<EntryType, RestType> {
    const INIT_VALUE: Option<WheelEntryList<EntryType, RestType>> = None;

    /// Create a new empty ByteWheel
    pub fn new() -> Self {
        let slots: [Option<WheelEntryList<EntryType, RestType>>; NUM_SLOTS] =
            [Self::INIT_VALUE; NUM_SLOTS];

        ByteWheel {
            slots,
            count: 0,
            current: 0,
        }
    }

    /// Returns the index of the current slot
    pub fn current(&self) -> u8 {
        self.current
    }

    /// Advances the wheel pointer to the target index without executing anything
    ///
    /// Used for implementing "skip"-behaviours.
    pub fn advance(&mut self, to: u8) {
        self.current = to;
    }

    /// Insert an entry at `pos` into the wheel and store the rest `r` with it
    pub fn insert(&mut self, pos: u8, e: EntryType, r: RestType) {
        let index = pos as usize;
        let we = WheelEntry { entry: e, rest: r };
        if self.slots[index].is_none() {
            let l = Vec::new();
            let bl = Some(l);
            self.slots[index] = bl;
        }
        if let Some(ref mut l) = &mut self.slots[index] {
            l.push(we);
            self.count += 1;
        }
    }

    /// True if the number of entries is 0
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Move the wheel by one tick and return all entries in the current slot together with the index of the next slot
    pub fn tick(&mut self) -> (Option<WheelEntryList<EntryType, RestType>>, u8) {
        self.current = self.current.wrapping_add(1u8);
        let index = self.current as usize;
        let cur = self.slots[index].take(); //mem::replace(&mut self.slots[index], None);
        if let Some(ref l) = cur {
            self.count -= l.len() as u64;
        }
        (cur, self.current)
    }
}

impl<EntryType, RestType> Default for ByteWheel<EntryType, RestType> {
    fn default() -> Self {
        Self::new()
    }
}
