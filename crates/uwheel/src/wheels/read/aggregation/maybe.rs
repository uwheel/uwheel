use core::ops::RangeBounds;

use time::OffsetDateTime;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::{conf::WheelConf, Wheel};
use crate::{
    aggregator::Aggregator,
    wheels::read::{
        hierarchical::{Granularity, WheelRange},
        plan::{Aggregation, WheelAggregation},
    },
};

// An internal wrapper Struct that containing a possible [Wheel]
#[repr(C)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Debug, Clone)]
pub(crate) struct MaybeWheel<A: Aggregator> {
    conf: WheelConf,
    inner: Option<Wheel<A>>,
}
impl<A: Aggregator> MaybeWheel<A> {
    pub fn new(conf: WheelConf) -> Self {
        Self { conf, inner: None }
    }
    pub fn clear(&mut self) {
        if let Some(wheel) = self.inner.as_mut() {
            wheel.clear();
        }
    }
    pub fn merge(&mut self, other: &Self) {
        if let Some(wheel) = self.inner.as_mut() {
            // TODO: fix better checks
            wheel.merge(other.as_ref().unwrap());
        }
    }

    #[inline]
    pub fn range(
        &self,
        start: OffsetDateTime,
        slots: usize,
        gran: Granularity,
    ) -> Option<Vec<(u64, A::PartialAggregate)>> {
        let watermark_date =
            |wm: u64| OffsetDateTime::from_unix_timestamp((wm as i64) / 1000).unwrap();
        self.inner.as_ref().map(|wheel| {
            let watermark = watermark_date(wheel.watermark());
            let distance = watermark - start;

            let start_ts = start.unix_timestamp() as u64 * 1000; // to ms
            use time::Duration;

            let (slot_distance, interval) = match gran {
                Granularity::Second => (
                    distance.whole_seconds(),
                    Duration::SECOND.whole_milliseconds(),
                ),
                Granularity::Minute => (
                    distance.whole_minutes(),
                    Duration::MINUTE.whole_milliseconds(),
                ),
                Granularity::Hour => (distance.whole_hours(), Duration::HOUR.whole_milliseconds()),
                Granularity::Day => (distance.whole_days(), Duration::DAY.whole_milliseconds()),
            };
            let slot_distance = slot_distance as usize;
            let interval = interval as u64;
            let start_slot = slot_distance - slots;
            let end_slot = start_slot + slots;

            wheel
                .range(start_slot..end_slot)
                .into_iter()
                .enumerate()
                .map(|(index, aggregate)| {
                    let ts = start_ts + (index as u64 * interval);
                    (ts, aggregate)
                })
                .collect::<_>()
        })
    }
    #[inline]
    pub fn combine_range<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        self.inner
            .as_ref()
            .and_then(|wheel| wheel.combine_range(range))
    }

    #[doc(hidden)]
    #[allow(dead_code)]
    #[inline]
    pub fn head(&self) -> Option<A::PartialAggregate> {
        self.inner.as_ref().and_then(|w| w.at(0)).copied()
    }

    #[inline]
    pub fn total(&self) -> Option<A::PartialAggregate> {
        if let Some(wheel) = self.inner.as_ref() {
            wheel.total()
        } else {
            None
        }
    }

    pub fn plan(
        &self,
        start: OffsetDateTime,
        slots: usize,
        range: WheelRange,
        gran: Granularity,
    ) -> Option<WheelAggregation> {
        self.calculate_slots(start, slots, gran)
            .map(|(start, end)| {
                let agg = self.aggregate_plan(&range, gran);
                WheelAggregation::new(range, agg, (start, end), gran)
            })
    }
    /// Returns `None` if wheel is not initialized or if the range of slots is out of bounds.
    pub fn calculate_slots(
        &self,
        start: OffsetDateTime,
        slots: usize,
        gran: Granularity,
    ) -> Option<(usize, usize)> {
        let watermark_date =
            |wm: u64| OffsetDateTime::from_unix_timestamp((wm as i64) / 1000).unwrap();
        self.inner.as_ref().and_then(|wheel| {
            let watermark = watermark_date(wheel.watermark());
            let distance = watermark - start;
            let slot_distance = match gran {
                Granularity::Second => distance.whole_seconds(),
                Granularity::Minute => distance.whole_minutes(),
                Granularity::Hour => distance.whole_hours(),
                Granularity::Day => distance.whole_days(),
            } as usize;
            let start_slot = slot_distance - slots;
            let end_slot = start_slot + slots;
            // SAFETY: bounds check whether the calculated start and end slots are valid
            if wheel.len() < end_slot {
                None
            } else {
                Some((start_slot, end_slot))
            }
        })
    }

    /// Returns estimated cost for performing the given aggregate on this wheel
    pub fn aggregate_plan(&self, range: &WheelRange, gran: Granularity) -> Aggregation {
        if self.prefix_support() {
            Aggregation::Prefix
        } else {
            let diff = range.end - range.start;
            let slots = (match gran {
                Granularity::Second => diff.whole_seconds(),
                Granularity::Minute => diff.whole_minutes(),
                Granularity::Hour => diff.whole_hours(),
                Granularity::Day => diff.whole_days(),
            }) as usize;

            Aggregation::Scan(slots)
        }
    }

    /// Returns whether this wheel supports prefix-sum range queries
    #[inline]
    pub fn prefix_support(&self) -> bool {
        self.inner.as_ref().map(|w| w.is_prefix()).unwrap_or(false)
    }

    pub fn size_bytes(&self) -> usize {
        if let Some(inner) = self.inner.as_ref() {
            inner.size_bytesz().unwrap() // safe as we know its implemented for Wheel
        } else {
            0
        }
    }
    #[inline]
    pub fn rotation_count(&self) -> usize {
        self.inner.as_ref().map(|w| w.rotation_count()).unwrap_or(0)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.as_ref().map(|w| w.total_slots()).unwrap_or(0)
    }

    pub(crate) fn as_mut(&mut self) -> Option<&mut Wheel<A>> {
        self.inner.as_mut()
    }
    pub fn as_ref(&self) -> Option<&Wheel<A>> {
        self.inner.as_ref()
    }
    #[inline]
    pub fn get_or_insert(&mut self) -> &mut Wheel<A> {
        if self.inner.is_none() {
            let agg_wheel = Wheel::new(self.conf);
            self.inner = Some(agg_wheel);
        }
        self.inner.as_mut().unwrap()
    }
}
