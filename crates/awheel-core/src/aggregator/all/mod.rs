use crate::{aggregator::PartialAggregateType, Aggregator};

use core::{
    clone::Clone,
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    default::Default,
    fmt::Debug,
    marker::Copy,
    option::{Option, Option::Some},
};

/// Aggregate State for the [AllAggregator]
#[repr(C)]
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct AggState {
    /// Minimum value seen
    min: f64,
    /// Maxiumum value seen
    max: f64,
    /// Current count of records that have been added
    count: u64,
    /// Total sum of values added
    sum: f64,
}
impl AggState {
    #[inline]
    fn new(value: f64) -> Self {
        Self {
            min: value,
            max: value,
            count: 1,
            sum: value,
        }
    }
    #[inline]
    fn merge(&mut self, other: Self) {
        self.min = f64::min(self.min, other.min);
        self.max = f64::max(self.max, other.max);
        self.count += other.count;
        self.sum += other.sum;
    }
    /// Returns the minimum value
    pub fn min_value(&self) -> f64 {
        self.min
    }
    /// Returns the maximum value
    pub fn max_value(&self) -> f64 {
        self.max
    }
    /// Returns the total sum
    pub fn sum(&self) -> f64 {
        self.sum
    }
    /// Returns the count of records
    pub fn count(&self) -> u64 {
        self.count
    }
    /// Returns the average mean using sum and count
    pub fn avg(&self) -> f64 {
        self.sum / self.count as f64
    }
}
impl PartialAggregateType for AggState {}

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

/// An All Aggregator the pre-computes MIN, MAX, SUM, COUNT, and AVG.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllAggregator;

impl Aggregator for AllAggregator {
    type Input = f64;
    type Aggregate = AggState;
    type PartialAggregate = AggState;
    type MutablePartialAggregate = AggState;

    #[inline]
    fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
        AggState::new(input)
    }

    #[inline]
    fn combine_mutable(a: &mut Self::MutablePartialAggregate, input: Self::Input) {
        a.merge(Self::lift(input))
    }
    fn freeze(mutable: Self::MutablePartialAggregate) -> Self::PartialAggregate {
        mutable
    }

    #[inline]
    fn combine(mut a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a.merge(b);
        a
    }
    #[inline]
    fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
        a
    }
}

#[cfg(test)]
mod tests {
    use crate::{time::Duration, RwWheel, SECONDS};

    use super::*;

    #[test]
    fn all_test() {
        let mut time = 0u64;
        let mut wheel = RwWheel::<AllAggregator>::new(time);

        for _ in 0..SECONDS {
            let entry = crate::Entry::new(1.0, time);
            wheel.insert(entry);
            time += 1000; // increase by 1 second
            wheel.advance_to(time);
        }

        let all: AggState = wheel.read().interval(Duration::minutes(1i64)).unwrap();
        assert_eq!(all.sum(), 60.0);
        assert_eq!(all.count(), 60);
        assert_eq!(all.max_value(), 1.0);
        assert_eq!(all.min_value(), 1.0);
        assert_eq!(all.avg(), 1.0);
    }
}
