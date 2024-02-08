use time::OffsetDateTime;

use super::{conf::WheelConf, AggregationWheel};
use crate::{
    aggregator::Aggregator,
    rw_wheel::read::{
        hierarchical::{Granularity, WheelRange},
        plan::Aggregation,
    },
};

// An internal wrapper Struct that containing a possible AggregationWheel
#[repr(C)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Debug, Clone)]
pub(crate) struct MaybeWheel<A: Aggregator> {
    conf: WheelConf,
    inner: Option<AggregationWheel<A>>,
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
    pub fn aggregate(
        &self,
        start: OffsetDateTime,
        slots: usize,
        gran: Granularity,
    ) -> Option<A::PartialAggregate> {
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
            wheel.aggregate(start_slot..end_slot)
        })
    }
    #[inline]
    pub fn head(&self) -> Option<A::PartialAggregate> {
        self.inner.as_ref().and_then(|w| w.at(0)).copied()
    }

    #[inline]
    pub fn interval(&self, interval: usize) -> (Option<A::PartialAggregate>, usize) {
        if let Some(wheel) = self.inner.as_ref() {
            wheel.interval(interval)
        } else {
            (None, 0)
        }
    }
    #[inline]
    pub fn interval_or_total(&self, interval: usize) -> (Option<A::PartialAggregate>, usize) {
        if let Some(wheel) = self.inner.as_ref() {
            wheel.interval_or_total(interval)
        } else {
            (None, 0)
        }
    }
    #[inline]
    pub fn total(&self) -> Option<A::PartialAggregate> {
        if let Some(wheel) = self.inner.as_ref() {
            wheel.total()
        } else {
            None
        }
    }

    /// Returns estimated cost for performing the given aggregate on this wheel
    #[allow(unused_mut)]
    pub fn aggregate_plan(&self, range: &WheelRange, gran: Granularity) -> Aggregation {
        if self.prefix_support() {
            Aggregation::Prefix
        } else {
            let diff = range.end - range.start;
            let mut slots = (match gran {
                Granularity::Second => diff.whole_seconds(),
                Granularity::Minute => diff.whole_minutes(),
                Granularity::Hour => diff.whole_hours(),
                Granularity::Day => diff.whole_days(),
            }) as usize;

            // TODO: may not be fully correct depending on alignment
            // #[cfg(feature = "simd")]
            // {
            //     let remainder = slots % A::SIMD_LANES;
            //     slots = (slots / A::SIMD_LANES) + remainder;
            // }
            Aggregation::Scan(slots)
        }
    }

    /// Returns whether this wheel supports prefix-sum range queries
    #[inline]
    pub fn prefix_support(&self) -> bool {
        self.inner.as_ref().map(|w| w.is_prefix()).unwrap_or(false)
        // A::PREFIX_SUPPORT && self.conf.prefix_sum
    }

    pub fn size_bytes(&self) -> usize {
        if let Some(inner) = self.inner.as_ref() {
            inner.size_bytesz().unwrap() // safe as we know its implemented for AggregationWheel
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

    pub(crate) fn as_mut(&mut self) -> Option<&mut AggregationWheel<A>> {
        self.inner.as_mut()
    }
    pub fn as_ref(&self) -> Option<&AggregationWheel<A>> {
        self.inner.as_ref()
    }
    #[inline]
    pub fn get_or_insert(&mut self) -> &mut AggregationWheel<A> {
        if self.inner.is_none() {
            let agg_wheel = AggregationWheel::new(self.conf);
            self.inner = Some(agg_wheel);
        }
        self.inner.as_mut().unwrap()
    }
}
