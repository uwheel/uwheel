use super::util::{create_pair_type, PairType};
use crate::time_internal::Duration;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug)]
pub struct State {
    pub range: usize,
    pub slide: usize,
    pub pair_type: PairType,
    pub pair_ticks_remaining: usize,
    pub current_pair_len: usize,
    // When the next window ends
    pub next_window_end: u64,
    pub next_pair_end: u64,
    pub in_p1: bool,
}
impl State {
    pub fn new(time: u64, range: usize, slide: usize) -> Self {
        let pair_type = create_pair_type(range, slide);
        let current_pair_len = match pair_type {
            PairType::Even(slide) => slide,
            PairType::Uneven(_, p2) => p2,
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
            in_p1: false,
        }
    }
    pub fn current_pair_duration(&self) -> Duration {
        Duration::milliseconds(self.current_pair_len as i64)
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
