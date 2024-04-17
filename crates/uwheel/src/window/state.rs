use super::util::{create_pair_type, PairType};
use crate::duration::Duration;

/// Internal windowing state including stream slices
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug)]
#[allow(dead_code)]
pub struct State {
    pub(crate) range: usize,
    pub slide: usize,
    /// Type of Pair (Uneven, Even).
    pub pair_type: PairType,
    /// How many ticks are left until the next pair is complete
    pub pair_ticks_remaining: usize,
    /// The length of the current pair
    pub current_pair_len: usize,
    // When the next window ends
    pub next_window_end: u64,
    /// Timestamp when the next pair ends
    pub next_pair_end: u64,
    /// A flag indicating whether we are in pair p1.
    pub in_p1: bool,
}
impl State {
    pub fn new(time: u64, range: usize, slide: usize) -> Self {
        let pair_type = create_pair_type(range, slide);
        let current_pair_len = match pair_type {
            PairType::Even(slide) => slide,
            PairType::Uneven(p1, _) => p1,
        };
        let next_pair_end = time + current_pair_len as u64;
        Self {
            range,
            slide,
            current_pair_len,
            pair_ticks_remaining: current_pair_len / 1000, // to seconds
            pair_type,
            next_window_end: time + range as u64,
            next_pair_end,
            in_p1: true,
        }
    }
    pub fn current_pair_duration(&self) -> Duration {
        Duration::milliseconds(self.current_pair_len as i64)
    }
    pub fn total_pairs(&self) -> usize {
        match self.pair_type {
            PairType::Even(_) => 1,
            PairType::Uneven(_, _) => 2,
        }
    }
    pub fn update_pair_len(&mut self) {
        if let PairType::Uneven(p1, p2) = self.pair_type {
            if self.in_p1 {
                self.current_pair_len = p2;
                self.in_p1 = false;
            } else {
                self.current_pair_len = p1;
                self.in_p1 = true;
            }
        }
    }
}
