#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone)]
/// Delta State that may be used to update or initiate a `ReaderWheel`
pub struct DeltaState<T> {
    /// Oldest timestamp for the set deltas
    ///
    /// May be used to initate a new wheel somewhere else
    pub oldest_ts: u64,
    /// Deltas ordered from Oldest to newest
    pub deltas: Vec<Option<T>>,
}

impl<T> DeltaState<T> {
    /// Creats a new delta state
    pub fn new(oldest_ts: u64, deltas: Vec<Option<T>>) -> Self {
        Self { oldest_ts, deltas }
    }

    /// Pushes a delta into the Delta State
    pub fn push(&mut self, delta: Option<T>) {
        self.deltas.push(delta);
    }

    /// Merge another DeltaState into this one
    ///
    /// Note that this assumes the states are sequential and the current wheel must contain the
    /// oldest timestamp of the delta states.
    pub fn merge(&mut self, mut other: Self) {
        assert!(
            self.oldest_ts < other.oldest_ts,
            "Trying to merge a state with lower timestamp"
        );
        self.deltas.append(&mut other.deltas);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{aggregator::sum::U32SumAggregator, wheels::read::ReaderWheel, NumericalDuration};

    #[test]
    fn build_wheel_from_delta_state_test() {
        let init_time = 10000;
        let deltas = vec![Some(10u32), None, Some(5u32), None, Some(15u32)];

        let state = DeltaState::new(init_time, deltas);

        let read_wheel: ReaderWheel<U32SumAggregator> = ReaderWheel::from_delta_state(state);

        assert_eq!(read_wheel.watermark(), 15000);
        assert_eq!(read_wheel.interval(1.seconds()), Some(15));
        assert_eq!(read_wheel.interval(5.seconds()), Some(30));
    }
    #[test]
    fn delta_state_merge_test() {
        let init_time = 10000;
        let deltas = vec![Some(10u32), None, Some(5u32), None, Some(15u32)];

        let mut state_1 = DeltaState::new(init_time, deltas);

        let init_time = 15000;
        let deltas = vec![Some(13u32), None, Some(1000u32)];
        let state_2 = DeltaState::new(init_time, deltas);

        state_1.merge(state_2);

        // check that it still contains the oldest timestamp so far
        assert_eq!(state_1.oldest_ts, 10000);
        // verify the deltas and their order
        assert_eq!(
            state_1.deltas,
            vec![
                Some(10u32),
                None,
                Some(5u32),
                None,
                Some(15u32),
                Some(13u32),
                None,
                Some(1000u32)
            ]
        );
    }
}
