use super::{byte_wheel::Bounds, quad_wheel::QuadWheelWithOverflow, Skip, TimerError};
use core::time::Duration;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(
    feature = "serde",
    serde(bound = "A: serde::Serialize + for<'a> serde::Deserialize<'a>")
)]
pub struct RawTimerWheel<A: Bounds> {
    timer: QuadWheelWithOverflow<A>,
    time: u64,
}

impl<A: Bounds> RawTimerWheel<A> {
    pub fn new(time: u64) -> Self {
        Self {
            timer: QuadWheelWithOverflow::default(),
            time,
        }
    }
    #[inline(always)]
    pub fn schedule_at(&mut self, time: u64, entry: A) -> Result<(), TimerError<A>> {
        let curr_time = self.time();
        let delay = time - curr_time;

        self.timer
            .insert_with_delay(entry, Duration::from_millis(delay))
    }

    #[inline]
    pub(crate) fn time(&self) -> u64 {
        self.time
    }

    #[inline(always)]
    pub fn add_time(&mut self, by: u64) {
        self.time += by;
    }

    #[inline(always)]
    pub fn tick_and_collect(&mut self, res: &mut Vec<A>, mut time_left: u32) {
        while time_left > 0 {
            match self.timer.can_skip() {
                Skip::Empty => {
                    // Timer is empty, no point in ticking it
                    self.add_time(time_left as u64);
                    return;
                }
                Skip::Millis(skip_ms) => {
                    // Skip forward
                    if skip_ms >= time_left {
                        // No more ops to gather, skip the remaining time_left and return
                        self.timer.skip(time_left);
                        self.add_time(time_left as u64);
                        return;
                    } else {
                        // Skip lower than time-left:
                        self.timer.skip(skip_ms);
                        self.add_time(skip_ms as u64);
                        time_left -= skip_ms;
                    }
                }
                Skip::None => {
                    for entry in self.timer.tick() {
                        res.push(entry);
                    }
                    self.add_time(1u64);
                    time_left -= 1u32;
                }
            }
        }
    }

    #[inline]
    pub fn advance_to(&mut self, ts: u64) -> Vec<A> {
        let curr_time = self.time();
        if ts < curr_time {
            // advance_to called with lower timestamp than current time
            return Vec::new();
        }

        let mut res = Vec::new();
        let mut time_left = ts - curr_time;

        while time_left > u32::MAX as u64 {
            self.tick_and_collect(&mut res, u32::MAX);
            time_left -= u32::MAX as u64;
        }
        // this cast must be safe now
        self.tick_and_collect(&mut res, time_left as u32);

        res
    }
}
