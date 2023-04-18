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
        let count = self.count.to_be_bytes();
        let sum = self.sum.to_be_bytes();
        let min = self.min.to_be_bytes();
        let max = self.max.to_be_bytes();
        let mut result = [0; 32];
        result[0] = count[0];
        result[1] = count[1];
        result[2] = count[2];
        result[3] = count[3];
        result[4] = sum[0];
        result[5] = sum[1];
        result[6] = sum[2];
        result[7] = sum[3];
        result[8] = min[0];
        result[9] = min[1];
        result[10] = min[2];
        result[11] = min[3];
        result[12] = max[0];
        result[13] = max[1];
        result[14] = max[2];
        result[15] = max[3];
        //result[0] =
        result
    }
    fn to_le_bytes(&self) -> Self::Bytes {
        //let count = self.count.to_le_bytes();
        //let sum = self.sum.to_le_bytes();
        //let min = self.min.to_le_bytes();
        //let max = self.max.to_le_bytes();
        //let result: [u8; 32] = array::concat([&arr1, &arr2, &arr3, &arr4]);
        //use core::array;
        //array::from_fn([&count, &sum, &min, &max].concat())
        //let mut result = [0; 32];
        unimplemented!();
    }
    #[inline]
    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        let mut count = [0; 8];
        count[0] = bytes[0];
        count[1] = bytes[1];
        count[2] = bytes[2];
        count[3] = bytes[3];
        count[4] = bytes[4];
        count[5] = bytes[5];
        count[6] = bytes[6];
        count[7] = bytes[7];
        let _c = u64::from_be(unsafe { core::mem::transmute(count) });
        unimplemented!();
    }
    fn from_be_bytes(_bytes: Self::Bytes) -> Self {
        unimplemented!();
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
        let aggregator = AllAggregator::default();
        let mut time = 0u64;
        let mut wheel = Wheel::new(time);

        for _ in 0..SECONDS + 1 {
            wheel.advance_to(time);
            let entry = crate::Entry::new(1.0, time);
            wheel.insert(entry).unwrap();
            time += 1000; // increase by 1 second
        }

        let all = wheel.minutes_wheel().lower(1, &aggregator).unwrap();
        assert_eq!(all.sum(), 60.0);
        assert_eq!(all.count(), 60);
        assert_eq!(all.max_value(), 1.0);
        assert_eq!(all.min_value(), 1.0);
        assert_eq!(all.avg(), 1.0);
    }
}
