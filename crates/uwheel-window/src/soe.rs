use crate::Window;
use uwheel_core::Aggregator;

#[cfg(not(feature = "std"))]
use alloc::vec::VecDeque;
#[cfg(feature = "std")]
use std::collections::VecDeque;

impl<A: Aggregator> Default for SubtractOnEvict<A> {
    fn default() -> Self {
        assert!(
            A::combine_inverse().is_some(),
            "SubtractOnEvict requires inverse_combine"
        );
        Self {
            stack: Default::default(),
            agg: A::IDENTITY,
        }
    }
}

pub struct SubtractOnEvict<A: Aggregator> {
    stack: VecDeque<A::PartialAggregate>,
    agg: A::PartialAggregate,
}

impl<A: Aggregator> SubtractOnEvict<A> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            A::combine_inverse().is_some(),
            "SubtractOnEvict requires inverse_combine"
        );
        Self {
            stack: VecDeque::with_capacity(capacity),
            agg: A::IDENTITY,
        }
    }
}

impl<A: Aggregator> Window<A> for SubtractOnEvict<A> {
    fn pop(&mut self) {
        if let Some(top) = self.stack.pop_front() {
            let inverse_combine = A::combine_inverse().unwrap();
            self.agg = inverse_combine(self.agg, top);
        }
    }
    fn query(&self) -> A::PartialAggregate {
        self.agg
    }
    fn push(&mut self, agg: A::PartialAggregate) {
        self.agg = A::combine(self.agg, agg);
        self.stack.push_back(agg);
    }
}
