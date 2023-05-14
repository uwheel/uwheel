#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

use crate::{types::PartialAggregateType, Aggregator, Option::Some};
use core::{
    clone::Clone,
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    default::Default,
    fmt::Debug,
    marker::Copy,
    option::Option,
};

#[repr(C)]
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
pub struct AggState {
    min: f64,
    max: f64,
    count: u64,
    sum: f64,
}
impl AggState {
    #[inline]
    pub fn new(value: f64) -> Self {
        Self {
            min: value,
            max: value,
            count: 1,
            sum: value,
        }
    }
    #[inline]
    pub fn merge(&mut self, other: Self) {
        self.min = f64::min(self.min, other.min);
        self.max = f64::max(self.max, other.max);
        self.count += other.count;
        self.sum += other.sum;
    }
    pub fn min_value(&self) -> f64 {
        self.min
    }
    pub fn max_value(&self) -> f64 {
        self.max
    }
    pub fn sum(&self) -> f64 {
        self.sum
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    pub fn avg(&self) -> f64 {
        self.sum / self.count as f64
    }
}
impl PartialAggregateType for AggState {
    type Bytes = [u8; 32];
    fn to_be_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 32];
        bytes[0..8].copy_from_slice(&self.min.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.max.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.count.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.sum.to_be_bytes());
        bytes
    }
    fn to_le_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 32];
        bytes[0..8].copy_from_slice(&self.min.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.max.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.count.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.sum.to_le_bytes());
        bytes
    }
    #[inline]
    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        let min = f64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let max = f64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let sum = f64::from_le_bytes(bytes[24..32].try_into().unwrap());
        AggState {
            min,
            max,
            count,
            sum,
        }
    }
    fn from_be_bytes(bytes: Self::Bytes) -> Self {
        let min = f64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let max = f64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let count = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
        let sum = f64::from_be_bytes(bytes[24..32].try_into().unwrap());
        AggState {
            min,
            max,
            count,
            sum,
        }
    }
}

impl Ord for AggState {
    fn cmp(&self, other: &Self) -> Ordering {
        f64::total_cmp(&self.sum, &other.sum)
    }
}

impl PartialOrd for AggState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for AggState {}
impl PartialEq for AggState {
    fn eq(&self, other: &Self) -> bool {
        self.sum == other.sum
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AllAggregator;

impl Aggregator for AllAggregator {
    type Input = f64;
    type Aggregate = AggState;
    type PartialAggregate = AggState;

    #[inline]
    fn lift(&self, input: Self::Input) -> Self::PartialAggregate {
        AggState::new(input)
    }
    #[inline]
    fn combine(
        &self,
        mut a: Self::PartialAggregate,
        b: Self::PartialAggregate,
    ) -> Self::PartialAggregate {
        a.merge(b);
        a
    }
    #[inline]
    fn lower(&self, a: Self::PartialAggregate) -> Self::Aggregate {
        a
    }
}

#[cfg(test)]
mod tests {
    use crate::{Wheel, SECONDS};

    use super::*;

    #[test]
    fn all_test() {
        let mut time = 0u64;
        let mut wheel = Wheel::<AllAggregator>::new(time);

        for _ in 0..SECONDS + 1 {
            wheel.advance_to(time);
            let entry = crate::Entry::new(1.0, time);
            wheel.insert(entry).unwrap();
            time += 1000; // increase by 1 second
        }

        let all: AggState = wheel.minutes().unwrap().lower(1).unwrap();
        assert_eq!(all.sum(), 60.0);
        assert_eq!(all.count(), 60);
        assert_eq!(all.max_value(), 1.0);
        assert_eq!(all.min_value(), 1.0);
        assert_eq!(all.avg(), 1.0);
    }
    #[test]
    fn test_be_bytes() {
        let agg_state = AggState {
            min: 0.0,
            max: 1.0,
            count: 10,
            sum: 5.0,
        };
        let bytes = agg_state.to_be_bytes();
        let decoded = AggState::from_be_bytes(bytes);
        assert_eq!(agg_state, decoded);
    }
    #[test]
    fn test_le_bytes() {
        let agg_state = AggState {
            min: 0.0,
            max: 1.0,
            count: 10,
            sum: 5.0,
        };
        let bytes = agg_state.to_le_bytes();
        let decoded = AggState::from_le_bytes(bytes);
        assert_eq!(agg_state, decoded);
    }
}
