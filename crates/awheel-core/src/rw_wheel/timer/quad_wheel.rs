//! An implementation of a four-level hierarchical hash wheel with overflow.
//!
//! Combining four [byte wheels](crate::wheels::byte_wheel) we get a hierachical timer
//! that can represent timeouts up to [`u32::MAX`](std::u32::MAX) time units into the future.
//!
//! In order to support timeouts of up to [`u64::MAX`](std::u64::MAX) time units,
//! this implementation also keeps an overflow list, which stores all timers that didn't fit
//! into any slot in the four wheels. Additions into this list happens in (amortised) constant time
//! but movement from the list into the timer array is linear in the number of overflow items.
//!
//! Our design assumes that the vast majority of timers are going be scheduled less than
//! [`u32::MAX`](std::u32::MAX) time units into the future. However, as movement from the overflow list
//! still happens at a rate of over 6mio entries per second (on a 2019 16"MBP) for most applications
//! there should be no large issues even if this assumption is not correct.

use super::{byte_wheel::*, *};
use core::{fmt::Debug, time::Duration};

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

struct OverflowEntry<EntryType> {
    entry: EntryType,
    remaining_delay: Duration,
}
impl<EntryType> OverflowEntry<EntryType> {
    fn new(entry: EntryType, remaining_delay: Duration) -> Self {
        OverflowEntry {
            entry,
            remaining_delay,
        }
    }
}

/// Indicates whether an entry should be moved into the next wheel, or dropped
///
/// Use this for implementing logic for cancellable timers.
#[derive(PartialEq, Eq, Debug)]
pub enum PruneDecision {
    /// Move the entry into the next wheel
    Keep,
    /// Drop the entry
    ///
    /// Usually indicates that the entry has already been cancelled
    Drop,
}
impl PruneDecision {
    /// `true` if this is a `PruneDecision::Keep`
    #[inline(always)]
    pub fn should_keep(&self) -> bool {
        self == &PruneDecision::Keep
    }

    /// `true` if this is a `PruneDecision::Drop`
    #[inline(always)]
    pub fn should_drop(&self) -> bool {
        self == &PruneDecision::Drop
    }
}

/// A simple pruner implementation that never drops any value
///
/// This is the default pruner for the [QuadWheelWithOverflow](QuadWheelWithOverflow)
pub fn no_prune<E>(_e: &E) -> PruneDecision {
    PruneDecision::Keep
}

/// An implementation of four-level byte-sized wheel
///
/// Any value scheduled so far off that it doesn't fit into the wheel
/// is stored in an overflow `Vec` and added to the wheel, once time as advanced enough
/// that it actually fits.
/// In this design the maximum schedule duration for the wheel itself is [`u32::MAX`](std::u32::MAX) units (typically ms),
/// everything else goes into the overflow `Vec`.
pub struct QuadWheelWithOverflow<EntryType> {
    primary: Box<ByteWheel<EntryType, [u8; 0]>>,
    secondary: Box<ByteWheel<EntryType, [u8; 1]>>,
    tertiary: Box<ByteWheel<EntryType, [u8; 2]>>,
    quarternary: Box<ByteWheel<EntryType, [u8; 3]>>,
    overflow: Vec<OverflowEntry<EntryType>>,
    pruner: fn(&EntryType) -> PruneDecision,
}

const MAX_SCHEDULE_DUR: Duration = Duration::from_millis(u32::MAX as u64);
const CYCLE_LENGTH: u64 = 1 << 32; // 2^32
const PRIMARY_LENGTH: u32 = 1 << 8; // 2^8
const SECONDARY_LENGTH: u32 = 1 << 16; // 2^16
const TERTIARY_LENGTH: u32 = 1 << 24; // 2^24

impl<EntryType> Default for QuadWheelWithOverflow<EntryType> {
    fn default() -> Self {
        QuadWheelWithOverflow::new(no_prune::<EntryType>)
    }
}

impl<EntryType> QuadWheelWithOverflow<EntryType>
where
    EntryType: TimerEntryWithDelay,
{
    /// Insert a new timeout into the wheel
    pub fn insert(&mut self, e: EntryType) -> Result<(), TimerError<EntryType>> {
        let delay = e.delay();
        self.insert_with_delay(e, delay)
    }
}

impl<EntryType> QuadWheelWithOverflow<EntryType> {
    /// Create a new wheel
    pub fn new(pruner: fn(&EntryType) -> PruneDecision) -> Self {
        QuadWheelWithOverflow {
            primary: Box::new(ByteWheel::new()),
            secondary: Box::new(ByteWheel::new()),
            tertiary: Box::new(ByteWheel::new()),
            quarternary: Box::new(ByteWheel::new()),
            overflow: Vec::new(),
            pruner,
        }
    }

    /// Described how many ticks are left before the timer has wrapped around completely
    pub fn remaining_time_in_cycle(&self) -> u64 {
        CYCLE_LENGTH - (self.current_time_in_cycle() as u64)
    }

    /// Produces a 32-bit timestamp including the current index of every wheel
    pub fn current_time_in_cycle(&self) -> u32 {
        let time_bytes = [
            self.quarternary.current(),
            self.tertiary.current(),
            self.secondary.current(),
            self.primary.current(),
        ];
        let mut result: u32 = 0;
        for &byte in &time_bytes {
            result = (result << 8) | (byte as u32);
        }
        result
    }

    /// Insert a new timeout into the wheel to be returned after `delay` ticks
    pub fn insert_with_delay(
        &mut self,
        e: EntryType,
        delay: Duration,
    ) -> Result<(), TimerError<EntryType>> {
        if delay >= MAX_SCHEDULE_DUR {
            let remaining_delay = Duration::from_millis(self.remaining_time_in_cycle());
            let new_delay = delay - remaining_delay;
            let overflow_e = OverflowEntry::new(e, new_delay);
            self.overflow.push(overflow_e);
            Ok(())
        } else {
            let delay = {
                let s = (delay.as_secs() * 1000) as u32;
                let ms = delay.subsec_millis();
                s + ms
            };
            let current_time = self.current_time_in_cycle();
            let absolute_time = delay.wrapping_add(current_time);
            let absolute_bytes: [u8; 4] = absolute_time.to_be_bytes();
            let zero_time = absolute_time ^ current_time; // a-b%2
            let zero_bytes: [u8; 4] = zero_time.to_be_bytes();
            match zero_bytes {
                [0, 0, 0, 0] => Err(TimerError::Expired(e)),
                [0, 0, 0, _] => {
                    self.primary.insert(absolute_bytes[3], e, []);
                    Ok(())
                }
                [0, 0, _, _] => {
                    self.secondary
                        .insert(absolute_bytes[2], e, [absolute_bytes[3]]);
                    Ok(())
                }
                [0, _, _, _] => {
                    self.tertiary.insert(
                        absolute_bytes[1],
                        e,
                        [absolute_bytes[2], absolute_bytes[3]],
                    );
                    Ok(())
                }
                [_, _, _, _] => {
                    self.quarternary.insert(
                        absolute_bytes[0],
                        e,
                        [absolute_bytes[1], absolute_bytes[2], absolute_bytes[3]],
                    );
                    Ok(())
                }
            }
        }
    }

    /// Move the wheel forward by a single unit (ms)
    ///
    /// Returns a list of all timers that expire during this tick.
    pub fn tick(&mut self) -> Vec<EntryType> {
        let mut res: Vec<EntryType> = Vec::new();
        // primary
        let (move0_opt, current0) = self.primary.tick();
        if let Some(move0) = move0_opt {
            res.reserve(move0.len());
            for we in move0 {
                if (self.pruner)(&we.entry).should_keep() {
                    res.push(we.entry);
                }
            }
        }
        if current0 == 0u8 {
            // secondary
            let (move1_opt, current1) = self.secondary.tick();
            if let Some(move1) = move1_opt {
                // Don't bother reserving, as most of the values will likely be redistributed over the primary wheel instead of being returned
                for we in move1 {
                    if (self.pruner)(&we.entry).should_keep() {
                        if we.rest[0] == 0u8 {
                            res.push(we.entry);
                        } else {
                            self.primary.insert(we.rest[0], we.entry, []);
                        }
                    }
                }
            }
            if current1 == 0u8 {
                // tertiary
                let (move2_opt, current2) = self.tertiary.tick();
                if let Some(move2) = move2_opt {
                    // Don't bother reserving, as most of the values will likely be redistributed over the primary wheel instead of being returned
                    for we in move2 {
                        if (self.pruner)(&we.entry).should_keep() {
                            match we.rest {
                                [0, 0] => {
                                    res.push(we.entry);
                                }
                                [0, b0] => {
                                    self.primary.insert(b0, we.entry, []);
                                }
                                [b1, b0] => {
                                    self.secondary.insert(b1, we.entry, [b0]);
                                }
                            }
                        }
                    }
                }
                if current2 == 0u8 {
                    // quaternary
                    let (move3_opt, current3) = self.quarternary.tick();
                    if let Some(move3) = move3_opt {
                        // Don't bother reserving, as most of the values will likely be redistributed over the primary wheel instead of being returned
                        for we in move3 {
                            if (self.pruner)(&we.entry).should_keep() {
                                match we.rest {
                                    [0, 0, 0] => {
                                        res.push(we.entry);
                                    }
                                    [0, 0, b0] => {
                                        self.primary.insert(b0, we.entry, []);
                                    }
                                    [0, b1, b0] => {
                                        self.secondary.insert(b1, we.entry, [b0]);
                                    }
                                    [b2, b1, b0] => {
                                        self.tertiary.insert(b2, we.entry, [b1, b0]);
                                    }
                                }
                            }
                        }
                    }
                    if current3 == 0u8 {
                        // overflow list
                        if !self.overflow.is_empty() {
                            let mut ol = Vec::with_capacity(self.overflow.len() / 2); // assume that about half are going to be scheduled now
                            core::mem::swap(&mut self.overflow, &mut ol);
                            for overflow_e in ol {
                                if (self.pruner)(&overflow_e.entry).should_keep() {
                                    match self.insert_with_delay(
                                        overflow_e.entry,
                                        overflow_e.remaining_delay,
                                    ) {
                                        Ok(()) => (), // ignore
                                        Err(TimerError::Expired(e)) => res.push(e),
                                        Err(_f) => panic!("Unexpected error during insert"),
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        res
    }

    /// Skip a certain `amount` of units (ms)
    ///
    /// No timers will be executed for the skipped time.
    /// Only use this after determining that it's actually
    /// valid with [can_skip](QuadWheelWithOverflow::can_skip)!
    pub fn skip(&mut self, amount: u32) {
        let new_time = self.current_time_in_cycle().wrapping_add(amount);
        let new_time_bytes: [u8; 4] = new_time.to_be_bytes();
        self.primary.advance(new_time_bytes[3]);
        self.secondary.advance(new_time_bytes[2]);
        self.tertiary.advance(new_time_bytes[1]);
        self.quarternary.advance(new_time_bytes[0]);
    }

    /// Determine if and how many ticks can be skipped
    pub fn can_skip(&self) -> Skip {
        if self.primary.is_empty() {
            if self.secondary.is_empty() {
                if self.tertiary.is_empty() {
                    if self.quarternary.is_empty() {
                        if self.overflow.is_empty() {
                            Skip::Empty
                        } else {
                            Skip::from_millis((self.remaining_time_in_cycle() - 1u64) as u32)
                        }
                    } else {
                        let tertiary_current =
                            self.current_time_in_cycle() & (TERTIARY_LENGTH - 1u32); // just zero highest byte
                        let rem = TERTIARY_LENGTH - tertiary_current;
                        Skip::from_millis(rem - 1u32)
                    }
                } else {
                    let secondary_current =
                        self.current_time_in_cycle() & (SECONDARY_LENGTH - 1u32); // zero highest 2 bytes
                    let rem = SECONDARY_LENGTH - secondary_current;
                    Skip::from_millis(rem - 1u32)
                }
            } else {
                let primary_current = self.primary.current() as u32;
                let rem = PRIMARY_LENGTH - primary_current;
                Skip::from_millis(rem - 1u32)
            }
        } else {
            Skip::None
        }
    }
}
