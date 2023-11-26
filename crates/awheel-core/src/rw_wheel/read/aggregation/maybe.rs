use core::ops::RangeBounds;

use time::OffsetDateTime;

use super::{conf::WheelConf, AggregationWheel};
use crate::{aggregator::Aggregator, rw_wheel::WheelExt};

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
    pub fn head(&self) -> Option<A::PartialAggregate> {
        self.inner.as_ref().and_then(|w| w.at(0)).copied()
    }

    #[inline]
    pub fn combine_range<R>(&self, range: R) -> Option<A::PartialAggregate>
    where
        R: RangeBounds<usize>,
    {
        self.inner.as_ref().and_then(|w| w.combine_range(range))
    }
    #[inline]
    pub fn interval(&self, interval: usize) -> Option<A::PartialAggregate> {
        if let Some(wheel) = self.inner.as_ref() {
            wheel.interval(interval)
        } else {
            None
        }
    }
    #[inline]
    pub fn interval_or_total(&self, interval: usize) -> Option<A::PartialAggregate> {
        if let Some(wheel) = self.inner.as_ref() {
            wheel.interval_or_total(interval)
        } else {
            None
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
    pub fn size_bytes(&self) -> usize {
        if let Some(inner) = self.inner.as_ref() {
            inner.size_bytes().unwrap() // safe as we know its implemented for AggregationWheel
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
        self.inner.as_ref().map(|w| w.len()).unwrap_or(0)
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
