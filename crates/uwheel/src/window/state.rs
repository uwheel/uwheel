use super::util::{create_pair_type, PairType};
use crate::duration::Duration;

/// Stream Slicing State using the pairs technique
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SlicingState {
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
impl SlicingState {
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
    pub fn update_state(&mut self, watermark: u64) {
        self.update_pair_len();
        self.next_pair_end = watermark + self.current_pair_len as u64;
        self.pair_ticks_remaining = self.current_pair_duration().whole_seconds() as usize;
    }
}

/// Internal state for session windowing
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone)]
pub struct SessionState {
    /// Static session gap
    session_gap: Duration,
    /// Current time of inactivity
    pub(crate) inactive_period: Duration,
    /// Timestamp of the last window start
    last_window_start: Option<u64>,
}

impl SessionState {
    pub fn new(session_gap: Duration) -> Self {
        SessionState {
            session_gap,
            inactive_period: Duration::ZERO,
            last_window_start: None,
        }
    }

    pub fn has_active_session(&self) -> bool {
        self.last_window_start.is_some()
    }

    pub fn is_inactive(&self) -> bool {
        self.inactive_period >= self.session_gap
    }

    /// Bumps the current inactive period by the given duration
    pub fn bump_inactive_period(&mut self, duration: Duration) {
        self.inactive_period = self.inactive_period.checked_add(duration).unwrap();
    }
    /// Resets the inactive period
    pub fn reset_inactive_period(&mut self) {
        self.inactive_period = Duration::ZERO;
    }

    /// Resets the session state and returns the last window start
    pub fn reset(&mut self) -> Option<u64> {
        let last_window_start = self.last_window_start.take();
        self.inactive_period = Duration::ZERO;
        last_window_start
    }
    /// Activates the session by setting the last window start
    pub fn activate_session(&mut self, window_start: u64) {
        self.last_window_start = Some(window_start);
    }
}
