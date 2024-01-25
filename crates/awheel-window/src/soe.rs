use awheel_core::Aggregator;

#[cfg(not(feature = "std"))]
use alloc::vec::VecDeque;
#[cfg(feature = "std")]
use std::collections::VecDeque;

pub trait Window<A: Aggregator> {
    fn push(&mut self, agg: A::PartialAggregate);
    fn pop(&mut self);
    fn query(&self) -> A::PartialAggregate;
}

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

#[derive(Copy, Clone)]
pub struct Value<A> {
    agg: A,
    val: A,
}
impl<A> Value<A> {
    #[inline]
    pub fn new(agg: A, val: A) -> Self {
        Self { agg, val }
    }
}

#[derive(Default)]
pub struct TwoStacks<A: Aggregator> {
    front: Vec<Value<A::PartialAggregate>>,
    back: Vec<Value<A::PartialAggregate>>,
}

impl<A: Aggregator> TwoStacks<A> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            front: Vec::with_capacity(capacity),
            back: Vec::with_capacity(capacity),
        }
    }
    #[inline(always)]
    fn agg(stack: &[Value<A::PartialAggregate>]) -> A::PartialAggregate {
        if let Some(top) = stack.last() {
            top.agg
        } else {
            A::IDENTITY
        }
    }
}

impl<A: Aggregator> Window<A> for TwoStacks<A> {
    fn pop(&mut self) {
        if self.front.is_empty() {
            while let Some(top) = self.back.pop() {
                self.front.push(Value::new(
                    A::combine(top.val, Self::agg(&self.front)),
                    top.val,
                ));
            }
        }
        self.front.pop();
    }
    #[inline]
    fn query(&self) -> A::PartialAggregate {
        A::combine(Self::agg(&self.front), Self::agg(&self.back))
    }
    #[inline]
    fn push(&mut self, agg: A::PartialAggregate) {
        self.back
            .push(Value::new(A::combine(Self::agg(&self.back), agg), agg));
    }
}
