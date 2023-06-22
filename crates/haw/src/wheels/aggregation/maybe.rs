use crate::aggregator::Aggregator;

use super::AggregationWheel;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

// An internal wrapper Struct that containing a possible AggregationWheel
#[repr(C)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[derive(Debug, Clone)]
pub struct MaybeWheel<const CAP: usize, A: Aggregator> {
    slots: usize,
    drill_down: bool,
    pub(crate) wheel: Option<Box<AggregationWheel<CAP, A>>>,
}
impl<const CAP: usize, A: Aggregator> MaybeWheel<CAP, A> {
    pub fn with_capacity_and_drill_down(slots: usize) -> Self {
        Self {
            slots,
            drill_down: true,
            wheel: None,
        }
    }
    pub fn with_capacity(slots: usize) -> Self {
        Self {
            slots,
            drill_down: false,
            wheel: None,
        }
    }
    pub fn clear(&mut self) {
        if let Some(ref mut wheel) = self.wheel {
            wheel.clear();
        }
    }
    pub fn merge(&mut self, other: &Self) {
        if let Some(ref mut wheel) = self.wheel {
            wheel.merge(other.as_deref().unwrap());
        }
    }
    #[inline]
    pub fn interval(&self, interval: usize) -> Option<A::PartialAggregate> {
        if let Some(w) = self.wheel.as_ref() {
            w.interval(interval)
        } else {
            None
        }
    }
    #[inline]
    pub fn interval_or_total(&self, interval: usize) -> Option<A::PartialAggregate> {
        if let Some(w) = self.wheel.as_ref() {
            w.interval_or_total(interval)
        } else {
            None
        }
    }
    #[inline]
    pub fn total(&self) -> Option<A::PartialAggregate> {
        if let Some(w) = self.wheel.as_ref() {
            w.total()
        } else {
            None
        }
    }
    #[inline]
    pub fn as_deref(&self) -> Option<&AggregationWheel<CAP, A>> {
        self.wheel.as_deref()
    }
    #[inline]
    pub fn rotation_count(&self) -> usize {
        self.wheel.as_ref().map(|w| w.rotation_count()).unwrap_or(0)
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.wheel.as_ref().map(|w| w.len()).unwrap_or(0)
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline]
    pub fn get_or_insert(&mut self) -> &mut AggregationWheel<CAP, A> {
        if self.wheel.is_none() {
            let agg_wheel = {
                if self.drill_down {
                    AggregationWheel::with_capacity_and_drill_down(self.slots)
                } else {
                    AggregationWheel::with_capacity(self.slots)
                }
            };

            self.wheel = Some(Box::new(agg_wheel));
        }
        self.wheel.as_mut().unwrap().as_mut()
    }
}
