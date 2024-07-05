mod state;
mod util;

use crate::{
    aggregator::Aggregator,
    duration::Duration,
    wheels::read::hierarchical::WindowAggregate,
};
use state::State;

#[cfg(not(feature = "std"))]
use alloc::collections::VecDeque;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "std")]
use std::collections::VecDeque;

use self::util::pairs_space;

/// A builder used to define µWheel windows
///
/// # Example
///
/// ```
/// use uwheel::{Window, NumericalDuration};
///
/// // define a sliding window with 10 sec range and 3 sec slide.
/// let window = Window::default().with_range(10.seconds()).with_slide(3.seconds());
/// ```
#[derive(Copy, Clone)]
pub struct Window {
    /// Defines the window range
    pub range: Duration,
    /// Defines the window slide
    pub slide: Duration,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            range: Duration::seconds(0),
            slide: Duration::seconds(0),
        }
    }
}

impl Window {
    /// Configures the builder to create a window with the given range
    pub fn with_range(mut self, range: Duration) -> Self {
        self.range = range;
        self
    }
    /// Configures the builder to create a window with the given slide
    pub fn with_slide(mut self, slide: Duration) -> Self {
        self.slide = slide;
        self
    }
}

impl From<(Duration, Duration)> for Window {
    fn from(val: (Duration, Duration)) -> Self {
        Window::default().with_range(val.0).with_slide(val.1)
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
pub struct WindowManager<A: Aggregator> {
    pub(crate) window: WindowAggregator<A>,
    pub(crate) state: State,
}
impl<A: Aggregator> WindowManager<A> {
    pub fn new(watermark: u64, range: usize, slide: usize) -> Self {
        let state = State::new(watermark, range, slide);
        let pairs = pairs_space(range, slide);
        let window = WindowAggregator::with_capacity(pairs);

        Self { window, state }
    }
    pub fn handle_window(
        &mut self,
        watermark: u64,
        windows: &mut Vec<WindowAggregate<A::PartialAggregate>>,
    ) {
        if watermark == self.state.next_window_end {
            windows.push((watermark, self.window.query()));
            self.clean_up_pairs();
            self.state.next_window_end += self.state.slide as u64;
        }
    }

    pub fn update_state(&mut self, watermark: u64) {
        self.state.update_pair_len();
        self.state.next_pair_end = watermark + self.state.current_pair_len as u64;
        self.state.pair_ticks_remaining =
            self.state.current_pair_duration().whole_seconds() as usize;
    }

    pub fn clean_up_pairs(&mut self) {
        for _i in 0..self.state.total_pairs() {
            self.window.pop();
        }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
pub enum WindowAggregator<A: Aggregator> {
    Soe(SubtractOnEvict<A>),
    TwoStacks(TwoStacks<A>),
}
impl<A: Aggregator> Default for WindowAggregator<A> {
    fn default() -> Self {
        if A::invertible() {
            WindowAggregator::Soe(SubtractOnEvict::new())
        } else {
            WindowAggregator::TwoStacks(TwoStacks::new())
        }
    }
}

impl<A: Aggregator> WindowAggregator<A> {
    pub fn with_capacity(capacity: usize) -> Self {
        if A::invertible() {
            WindowAggregator::Soe(SubtractOnEvict::with_capacity(capacity))
        } else {
            WindowAggregator::TwoStacks(TwoStacks::with_capacity(capacity))
        }
    }
    pub fn push(&mut self, agg: A::PartialAggregate) {
        match self {
            WindowAggregator::Soe(soe) => soe.push(agg),
            WindowAggregator::TwoStacks(stacks) => stacks.push(agg),
        }
    }

    pub fn query(&self) -> A::PartialAggregate {
        match self {
            WindowAggregator::Soe(soe) => soe.query(),
            WindowAggregator::TwoStacks(stacks) => stacks.query(),
        }
    }

    pub fn pop(&mut self) {
        match self {
            WindowAggregator::Soe(soe) => soe.pop(),
            WindowAggregator::TwoStacks(stacks) => stacks.pop(),
        }
    }
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

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
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

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Copy, Clone)]
pub struct Value<A: Aggregator> {
    agg: A::PartialAggregate,
    val: A::PartialAggregate,
}
impl<A: Aggregator> Value<A> {
    #[inline]
    pub fn new(agg: A::PartialAggregate, val: A::PartialAggregate) -> Self {
        Self { agg, val }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Default)]
pub struct TwoStacks<A: Aggregator> {
    front: Vec<Value<A>>,
    back: Vec<Value<A>>,
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
    fn agg(stack: &[Value<A>]) -> A::PartialAggregate {
        if let Some(top) = stack.last() {
            top.agg
        } else {
            A::IDENTITY
        }
    }
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

#[cfg(test)]
mod tests {
    use super::Window;
    use crate::{aggregator::sum::U64SumAggregator, Duration, Entry, NumericalDuration, RwWheel};

    #[test]
    fn window_30_sec_range_10_sec_slide_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(
            Window::default()
                .with_range(Duration::seconds(30))
                .with_slide(Duration::seconds(10)),
        );

        window_30_sec_range_10_sec_slide(wheel);
    }

    fn window_30_sec_range_10_sec_slide(mut wheel: RwWheel<U64SumAggregator>) {
        wheel.insert(Entry::new(681, 1533081607321));
        wheel.insert(Entry::new(625, 1533081619748));
        wheel.insert(Entry::new(1319, 1533081621175));
        wheel.insert(Entry::new(220, 1533081626470));
        wheel.insert(Entry::new(398, 1533081630291));
        wheel.insert(Entry::new(2839, 1533081662717));
        wheel.insert(Entry::new(172, 1533081663534));
        wheel.insert(Entry::new(1133, 1533081664024));
        wheel.insert(Entry::new(1417, 1533081678095));
        wheel.insert(Entry::new(195, 1533081679609));

        let results = wheel.advance_to(1533081630000);
        assert_eq!(results, [(1533081630000, 2845)])
    }

    fn window_60_sec_range_10_sec_slide(mut wheel: RwWheel<U64SumAggregator>) {
        wheel.insert(Entry::new(1, 9000));
        wheel.insert(Entry::new(1, 15000));
        wheel.insert(Entry::new(1, 25000));
        wheel.insert(Entry::new(1, 35000));
        wheel.insert(Entry::new(1, 59000));

        assert!(wheel.advance_to(59000).is_empty());

        wheel.insert(Entry::new(3, 69000));
        wheel.insert(Entry::new(5, 75000));
        wheel.insert(Entry::new(10, 110000));

        let results = wheel.advance_to(130000);
        assert_eq!(
            results,
            [
                (60000, 5),
                (70000, 7),
                (80000, 11),
                (90000, 10),
                (100000, 9),
                (110000, 9),
                (120000, 18),
                (130000, 15)
            ]
        );
    }
    #[test]
    fn window_60_sec_range_10_sec_slide_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(0);
        wheel.window(
            Window::default()
                .with_range(Duration::seconds(60))
                .with_slide(Duration::seconds(10)),
        );
        window_60_sec_range_10_sec_slide(wheel);
    }

    fn window_120_sec_range_10_sec_slide(mut wheel: RwWheel<U64SumAggregator>) {
        wheel.insert(Entry::new(1, 9000));
        wheel.insert(Entry::new(1, 15000));
        wheel.insert(Entry::new(1, 25000));
        wheel.insert(Entry::new(1, 35000));
        wheel.insert(Entry::new(1, 59000));

        assert!(wheel.advance_to(60000).is_empty());

        wheel.insert(Entry::new(3, 69000));
        wheel.insert(Entry::new(5, 75000));
        wheel.insert(Entry::new(10, 110000));

        assert!(wheel.advance_to(100000).is_empty());

        wheel.insert(Entry::new(3, 125000));

        // 1 window triggered [0-120] -> should be 23
        // 2nd window triggered [10-130] -> should be (23 - 1) + 3 = 25
        // 3nd window triggered [20-140] -> should be (25 -1)
        // 4nd window triggered [30-150] -> should be (24-1) = 23
        // 5nd window triggered [40-160] -> should be (23 -1 ) = 22
        let results = wheel.advance_to(160000);
        assert_eq!(
            results,
            [
                (120000, 23),
                (130000, 25),
                (140000, 24),
                (150000, 23),
                (160000, 22)
            ]
        );
    }

    #[test]
    fn window_2_min_range_10_sec_slide_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(0);
        wheel.window(
            Window::default()
                .with_range(Duration::minutes(2))
                .with_slide(Duration::seconds(10)),
        );
        window_120_sec_range_10_sec_slide(wheel);
    }
    #[test]
    fn window_10_sec_range_3_sec_slide_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(0);
        wheel.window(
            Window::default()
                .with_range(Duration::seconds(10))
                .with_slide(Duration::seconds(3)),
        );
        window_10_sec_range_3_sec_slide(wheel);
    }

    fn window_10_sec_range_3_sec_slide(mut wheel: RwWheel<U64SumAggregator>) {
        // Based on Figure 4 in https://asterios.katsifodimos.com/assets/publications/window-semantics-encyclopediaBigDAta18.pdf
        for i in 1..=22 {
            wheel.insert(Entry::new(i, i * 1000 - 1));
        }
        let results = wheel.advance(22.seconds());

        // w1: reduce[1..=10] = 55
        // w2: reduce[4..=13] = 85
        // w3: reduce[7..=16] = 115
        // w4: reduce[10..=19] = 145
        // w5: reduce[13..=22] = 175
        assert_eq!(
            results,
            [
                (10000, 55),
                (13000, 85),
                (16000, 115),
                (19000, 145),
                (22000, 175)
            ]
        );
    }

    #[test]
    fn out_of_order_inserts_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(
            Window::default()
                .with_range(Duration::seconds(30))
                .with_slide(Duration::seconds(10)),
        );
        wheel.insert(Entry::new(300, 1533081625000));
        wheel.insert(Entry::new(100, 1533081605000));
        wheel.insert(Entry::new(200, 1533081615000));
        let results = wheel.advance_to(1533081630000);
        assert_eq!(results, [(1533081630000, 600)]);
    }

    #[test]
    fn edge_case_window_boundaries_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(
            Window::default()
                .with_range(Duration::seconds(10))
                .with_slide(Duration::seconds(10)),
        );
        wheel.insert(Entry::new(100, 1533081609999)); // Just inside the window
        wheel.insert(Entry::new(200, 1533081610000)); // On the boundary, start of new window (should not be included)
        wheel.insert(Entry::new(300, 1533081610001)); // Just outside the window
        let results = wheel.advance_to(1533081610000);
        assert_eq!(results, [(1533081610000, 100)]);
    }
    #[test]
    fn empty_window_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(
            Window::default()
                .with_range(Duration::seconds(30))
                .with_slide(Duration::seconds(10)),
        );
        let results = wheel.advance_to(1533081630000);
        assert_eq!(results, [(1533081630000, 0)]);
    }

    #[test]
    #[should_panic]
    fn invalid_window_spec() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(
            Window::default()
                .with_range(Duration::seconds(10))
                .with_slide(Duration::seconds(30)),
        );
    }
}
