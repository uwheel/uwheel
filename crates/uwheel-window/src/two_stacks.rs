use crate::Window;
use uwheel_core::Aggregator;

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
