use crate::aggregator::{Aggregator, PartialAggregateType};
use roaring::{RoaringBitmap, RoaringTreemap};

/// Partial aggregate wrapping an optional `RoaringBitmap`.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RoaringPartial32 {
    inner: Option<RoaringBitmap>,
}

impl RoaringPartial32 {
    const EMPTY: Self = Self { inner: None };

    /// Creates a partial aggregate from a bitmap.
    pub fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        Self {
            inner: Some(bitmap),
        }
    }

    /// Consumes the partial aggregate and returns its bitmap.
    pub fn into_bitmap(self) -> RoaringBitmap {
        self.inner.unwrap_or_default()
    }

    /// Returns a bitmap clone without consuming the partial aggregate.
    pub fn as_bitmap(&self) -> RoaringBitmap {
        self.inner.clone().unwrap_or_default()
    }

    /// Returns the inner roaring bitmap as reference.
    pub fn as_ref(&self) -> Option<&RoaringBitmap> {
        self.inner.as_ref()
    }

    /// Checks if the inner bitmap contains a specific value.
    #[inline]
    pub fn contains(&self, value: u32) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|bitmap| bitmap.contains(value))
    }

    fn union(self, other: Self) -> Self {
        match (self.inner, other.inner) {
            (Some(mut left), Some(right)) => {
                left |= &right;
                Self { inner: Some(left) }
            }
            (Some(left), None) => Self { inner: Some(left) },
            (None, Some(right)) => Self { inner: Some(right) },
            (None, None) => Self::EMPTY,
        }
    }
}

impl From<RoaringPartial32> for RoaringBitmap {
    fn from(value: RoaringPartial32) -> Self {
        value.into_bitmap()
    }
}

impl From<&RoaringPartial32> for RoaringBitmap {
    fn from(value: &RoaringPartial32) -> Self {
        value.as_bitmap()
    }
}

impl PartialAggregateType for RoaringPartial32 {}

/// A Roaring Bitmap Aggregator for u32 values
///
/// Uses a `RoaringBitmap` to efficiently hold values over time.
#[derive(Default, Copy, Debug, Clone)]
pub struct RoaringAggregator32;

impl Aggregator for RoaringAggregator32 {
    const IDENTITY: Self::PartialAggregate = RoaringPartial32::EMPTY;

    type Input = u32;
    type MutablePartialAggregate = RoaringBitmap;
    type PartialAggregate = RoaringPartial32;
    type Aggregate = RoaringBitmap;

    fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(input);
        bitmap
    }

    fn combine_mutable(mutable: &mut Self::MutablePartialAggregate, input: Self::Input) {
        mutable.insert(input);
    }

    fn freeze(mutable: Self::MutablePartialAggregate) -> Self::PartialAggregate {
        RoaringPartial32::from_bitmap(mutable)
    }

    fn combine(a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a.union(b)
    }

    fn lower(partial: Self::PartialAggregate) -> Self::Aggregate {
        partial.into_bitmap()
    }
}

/// Partial aggregate wrapping an optional `RoaringTreemap`.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RoaringPartial64 {
    inner: Option<RoaringTreemap>,
}

impl RoaringPartial64 {
    const EMPTY: Self = Self { inner: None };

    /// Creates a partial aggregate from a treemap.
    pub fn from_treemap(treemap: RoaringTreemap) -> Self {
        Self {
            inner: Some(treemap),
        }
    }

    /// Consumes the partial aggregate and returns its treemap.
    pub fn into_treemap(self) -> RoaringTreemap {
        self.inner.unwrap_or_default()
    }

    /// Returns a treemap clone without consuming the partial aggregate.
    pub fn as_treemap(&self) -> RoaringTreemap {
        self.inner.clone().unwrap_or_default()
    }

    /// Returns the inner roaring bitmap as reference.
    pub fn as_ref(&self) -> Option<&RoaringTreemap> {
        self.inner.as_ref()
    }

    /// Checks if the inner treemap contains a specific value.
    #[inline]
    pub fn contains(&self, value: u64) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|bitmap| bitmap.contains(value))
    }

    fn union(self, other: Self) -> Self {
        match (self.inner, other.inner) {
            (Some(mut left), Some(right)) => {
                left |= &right;
                Self { inner: Some(left) }
            }
            (Some(left), None) => Self { inner: Some(left) },
            (None, Some(right)) => Self { inner: Some(right) },
            (None, None) => Self::EMPTY,
        }
    }
}

impl From<RoaringPartial64> for RoaringTreemap {
    fn from(value: RoaringPartial64) -> Self {
        value.into_treemap()
    }
}

impl From<&RoaringPartial64> for RoaringTreemap {
    fn from(value: &RoaringPartial64) -> Self {
        value.as_treemap()
    }
}

impl PartialAggregateType for RoaringPartial64 {}

/// A Roaring Bitmap Aggregator for u64 values
///
/// Uses a `RoaringTreemap` to efficiently hold values over time.
#[derive(Default, Debug, Copy, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RoaringAggregator64;

impl Aggregator for RoaringAggregator64 {
    const IDENTITY: Self::PartialAggregate = RoaringPartial64::EMPTY;

    type Input = u64;
    type MutablePartialAggregate = RoaringTreemap;
    type PartialAggregate = RoaringPartial64;
    type Aggregate = RoaringTreemap;

    fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
        let mut treemap = RoaringTreemap::new();
        treemap.insert(input);
        treemap
    }

    fn combine_mutable(mutable: &mut Self::MutablePartialAggregate, input: Self::Input) {
        mutable.insert(input);
    }

    fn freeze(mutable: Self::MutablePartialAggregate) -> Self::PartialAggregate {
        RoaringPartial64::from_treemap(mutable)
    }

    fn combine(a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a.union(b)
    }

    fn lower(partial: Self::PartialAggregate) -> Self::Aggregate {
        partial.into_treemap()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Entry, RwWheel, WheelRange, aggregator::Aggregator};

    use super::{RoaringAggregator32, RoaringAggregator64};

    #[derive(Clone, Debug)]
    struct SensorEvent32 {
        timestamp_ms: u64,
        value: u32,
    }

    #[derive(Clone, Debug)]
    struct SensorEvent64 {
        timestamp_ms: u64,
        value: u64,
    }

    fn bucket32(timestamp_ms: u64) -> u32 {
        (timestamp_ms / 1_000) as u32
    }

    fn bucket64(timestamp_ms: u64) -> u64 {
        timestamp_ms / 1_000
    }

    #[test]
    fn bitmap_reports_temporal_membership() {
        const THRESHOLD: u32 = 70;

        let events = vec![
            SensorEvent32 {
                timestamp_ms: 1_000,
                value: 65,
            },
            SensorEvent32 {
                timestamp_ms: 2_000,
                value: 72,
            },
            SensorEvent32 {
                timestamp_ms: 3_000,
                value: 88,
            },
            SensorEvent32 {
                timestamp_ms: 6_000,
                value: 63,
            },
            SensorEvent32 {
                timestamp_ms: 7_000,
                value: 95,
            },
            SensorEvent32 {
                timestamp_ms: 9_000,
                value: 77,
            },
        ];

        let mut wheel: RwWheel<RoaringAggregator32> = RwWheel::new(0);

        for event in &events {
            if event.value >= THRESHOLD {
                wheel.insert(Entry::new(bucket32(event.timestamp_ms), event.timestamp_ms));
            }
        }

        let _ = wheel.advance_to(12_000);

        let ranges = [
            (0, 4_000, vec![2, 3]),
            (4_000, 8_000, vec![7]),
            (8_000, 12_000, vec![9]),
        ];

        for (start, end, buckets) in ranges {
            let bitmap = wheel
                .read()
                .combine_range(WheelRange::new_unchecked(start, end))
                .expect("range should produce aggregate")
                .into_bitmap();
            assert_eq!(bitmap.iter().collect::<Vec<_>>(), buckets);
        }
    }

    #[test]
    fn treemap_reports_temporal_membership() {
        const THRESHOLD: u64 = 70;

        let events = vec![
            SensorEvent64 {
                timestamp_ms: 1_000,
                value: 65,
            },
            SensorEvent64 {
                timestamp_ms: 2_000,
                value: 72,
            },
            SensorEvent64 {
                timestamp_ms: 3_000,
                value: 88,
            },
            SensorEvent64 {
                timestamp_ms: 6_000,
                value: 63,
            },
            SensorEvent64 {
                timestamp_ms: 7_000,
                value: 95,
            },
            SensorEvent64 {
                timestamp_ms: 9_000,
                value: 77,
            },
        ];

        let mut wheel: RwWheel<RoaringAggregator64> = RwWheel::new(0);

        for event in &events {
            if event.value >= THRESHOLD {
                wheel.insert(Entry::new(bucket64(event.timestamp_ms), event.timestamp_ms));
            }
        }

        let _ = wheel.advance_to(12_000);

        let ranges = [
            (0, 4_000, vec![2_u64, 3]),
            (4_000, 8_000, vec![7]),
            (8_000, 12_000, vec![9]),
        ];

        for (start, end, buckets) in ranges {
            let treemap = wheel
                .read()
                .combine_range(WheelRange::new_unchecked(start, end))
                .expect("range should produce aggregate")
                .into_treemap();
            assert_eq!(treemap.iter().collect::<Vec<_>>(), buckets);
        }
    }

    #[test]
    fn bitmap_combines_partials_using_union() {
        let left_partial = RoaringAggregator32::freeze({
            let mut bitmap = RoaringAggregator32::lift(1);
            RoaringAggregator32::combine_mutable(&mut bitmap, 3);
            bitmap
        });

        let right_partial = RoaringAggregator32::freeze({
            let mut bitmap = RoaringAggregator32::lift(2);
            RoaringAggregator32::combine_mutable(&mut bitmap, 4);
            bitmap
        });

        let combined = RoaringAggregator32::combine(left_partial, right_partial);
        let bitmap = RoaringAggregator32::lower(combined);

        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn treemap_combines_partials_using_union() {
        let left_partial = RoaringAggregator64::freeze({
            let mut treemap = RoaringAggregator64::lift(1);
            RoaringAggregator64::combine_mutable(&mut treemap, 3);
            treemap
        });

        let right_partial = RoaringAggregator64::freeze({
            let mut treemap = RoaringAggregator64::lift(2);
            RoaringAggregator64::combine_mutable(&mut treemap, 4);
            treemap
        });

        let combined = RoaringAggregator64::combine(left_partial, right_partial);
        let treemap = RoaringAggregator64::lower(combined);

        assert_eq!(treemap.iter().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    }
}
