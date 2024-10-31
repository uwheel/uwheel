mod circular_queue;
mod hammer_slide;
pub mod state;
mod util;

use crate::{aggregator::Aggregator, duration::Duration};
use hammer_slide::HammerSlide;
use state::{SessionState, SlicingState};

#[cfg(not(feature = "std"))]
use alloc::collections::VecDeque;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "std")]
use std::collections::VecDeque;

use self::util::pairs_space;

/// Window Aggregation Result
#[derive(PartialEq, Debug, Clone, Copy)]
pub struct WindowAggregate<T> {
    /// The start time of the window in milliseconds since unix epoch
    pub window_start_ms: u64,
    /// The end time of the window in milliseconds since unix epoch
    pub window_end_ms: u64,
    /// The aggregate result for the window
    pub aggregate: T,
}

/// Different window variants supported by µWheel
///
/// # Example
///
/// ```
/// use uwheel::{Window, RwWheel, aggregator::sum::U32SumAggregator, NumericalDuration};
///
/// let mut wheel: RwWheel<U32SumAggregator> = RwWheel::new(0);
/// wheel.window(Window::tumbling(10.seconds()));
/// ```
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Copy, Clone, Debug)]
pub enum Window {
    /// A tumbling window with a fixed range
    Tumbling {
        /// The range of the window given using [Duration]
        range: Duration,
    },
    /// A sliding window with a fixed range and slide
    Sliding {
        /// The range of the window given using [Duration]
        range: Duration,
        /// The slide of the window given using [Duration]
        slide: Duration,
    },
    /// A session window with a fixed timeout
    Session {
        /// Session gap timeout given using [Duration]
        timeout: Duration,
    },
}

impl Window {
    /// Creates a tumbling window with the given range
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Window, NumericalDuration};
    ///
    /// let window = Window::tumbling(10.seconds());
    /// ```
    pub fn tumbling(range: Duration) -> Self {
        Self::Tumbling { range }
    }

    /// Creates a sliding window with the given range and slide
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Window, NumericalDuration};
    ///
    /// let window = Window::sliding(10.seconds(), 3.seconds());
    ///
    /// ```
    pub fn sliding(range: Duration, slide: Duration) -> Self {
        assert!(
            range >= slide,
            "Window range must be larger or equal to slide"
        );
        Self::Sliding { range, slide }
    }

    /// Creates a session window with the given timeout
    ///
    /// # Example
    ///
    /// ```
    /// use uwheel::{Window, NumericalDuration};
    ///
    /// let window = Window::session(10.seconds());
    /// ```
    pub fn session(timeout: Duration) -> Self {
        Self::Session { timeout }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
pub struct WindowManager<A: Aggregator> {
    pub(crate) aggregator: WindowAggregator<A>,
    pub(crate) window: Window,
}
impl<A: Aggregator> WindowManager<A> {
    /// Creates a new window manager with the given watermark and window type
    pub fn new(watermark: u64, window: Window) -> Self {
        let to_ms = |d: Duration| d.whole_milliseconds() as usize;

        // Create window aggregator and state based on the window type
        let aggregator = match window {
            Window::Tumbling { range } => {
                let pairs = pairs_space(to_ms(range), to_ms(range));
                WindowAggregator::Slicing {
                    state: SlicingState::new(watermark, to_ms(range), to_ms(range)),
                    aggregator: SlicingAggregator::with_capacity(pairs),
                }
            }
            Window::Sliding { range, slide } => {
                let pairs = pairs_space(to_ms(range), to_ms(slide));
                WindowAggregator::Slicing {
                    state: SlicingState::new(watermark, to_ms(range), to_ms(slide)),
                    aggregator: SlicingAggregator::with_capacity(pairs),
                }
            }
            Window::Session { timeout } => WindowAggregator::Session {
                state: SessionState::new(timeout),
                aggregator: SessionAggregator::default(),
            },
        };
        Self { aggregator, window }
    }
}
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
pub enum WindowAggregator<A: Aggregator> {
    Slicing {
        state: SlicingState,
        aggregator: SlicingAggregator<A>,
    },
    Session {
        state: SessionState,
        aggregator: SessionAggregator<A>,
    },
}

impl<A: Aggregator> WindowAggregator<A> {
    pub fn session_as_mut(&mut self) -> (&mut SessionState, &mut SessionAggregator<A>) {
        match self {
            WindowAggregator::Session { state, aggregator } => (state, aggregator),
            _ => panic!("Not a session window"),
        }
    }
    pub fn slicing_as_mut(&mut self) -> (&mut SlicingState, &mut SlicingAggregator<A>) {
        match self {
            WindowAggregator::Slicing { state, aggregator } => (state, aggregator),
            _ => panic!("Not a slicing window"),
        }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
pub struct SessionAggregator<A: Aggregator> {
    current: A::PartialAggregate,
}

impl<A: Aggregator> Default for SessionAggregator<A> {
    fn default() -> Self {
        Self {
            current: A::PartialAggregate::default(),
        }
    }
}
impl<A: Aggregator> SessionAggregator<A> {
    /// Aggregate a partial aggregate into the current session aggregate
    pub fn aggregate_session(&mut self, partial: A::PartialAggregate) {
        self.current = A::combine(self.current, partial);
    }
    pub fn get_and_reset(&mut self) -> A::PartialAggregate {
        let current = self.current;
        self.current = A::PartialAggregate::default();
        current
    }
}

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
pub enum SlicingAggregator<A: Aggregator> {
    Soe(SubtractOnEvict<A>),
    TwoStacks(TwoStacks<A>),
    HammerSlide(HammerSlide<A>),
}
impl<A: Aggregator> Default for SlicingAggregator<A> {
    fn default() -> Self {
        if A::invertible() {
            Self::Soe(SubtractOnEvict::new())
        } else {
            // Self::TwoStacks(TwoStacks::new())
            Self::HammerSlide(HammerSlide::new(0, 1))
        }
    }
}

impl<A: Aggregator> SlicingAggregator<A> {
    pub fn with_capacity(capacity: usize) -> Self {
        if A::invertible() {
            Self::Soe(SubtractOnEvict::with_capacity(capacity))
        } else {
            // Self::TwoStacks(TwoStacks::with_capacity(capacity))
            Self::HammerSlide(HammerSlide::new(capacity, 1))
        }
    }
    pub fn push(&mut self, agg: A::PartialAggregate) {
        match self {
            Self::Soe(soe) => soe.push(agg),
            Self::TwoStacks(stacks) => stacks.push(agg),
            Self::HammerSlide(hammer_slide) => hammer_slide.insert(agg),
        }
    }

    pub fn query(&mut self) -> A::PartialAggregate {
        match self {
            Self::Soe(soe) => soe.query(),
            Self::TwoStacks(stacks) => stacks.query(),
            Self::HammerSlide(hammer_slide) => hammer_slide.query(false),
        }
    }

    pub fn pop(&mut self) {
        match self {
            Self::Soe(soe) => soe.pop(),
            Self::TwoStacks(stacks) => stacks.pop(),
            Self::HammerSlide(hammer_slide) => hammer_slide.evict(1),
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
    use crate::{
        aggregator::sum::U64SumAggregator,
        window::WindowAggregate,
        Duration,
        Entry,
        NumericalDuration,
        RwWheel,
    };

    #[test]
    fn window_30_sec_tumbling_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(Window::tumbling(Duration::seconds(30)));

        window_30_sec_tumbling(wheel);
    }

    fn window_30_sec_tumbling(mut wheel: RwWheel<U64SumAggregator>) {
        wheel.insert(Entry::new(100, 1533081605000));
        wheel.insert(Entry::new(200, 1533081615000));
        wheel.insert(Entry::new(300, 1533081625000));
        wheel.insert(Entry::new(400, 1533081635000));
        wheel.insert(Entry::new(500, 1533081645000));

        let results = wheel.advance_to(1533081660000);
        assert_eq!(
            results,
            [
                WindowAggregate {
                    window_start_ms: 1533081600000,
                    window_end_ms: 1533081630000,
                    aggregate: 600
                }, // Sum of entries at 1533081605000, 1533081615000, 1533081625000
                WindowAggregate {
                    window_start_ms: 1533081630000,
                    window_end_ms: 1533081660000,
                    aggregate: 900
                }, // Sum of entries at 1533081635000, 1533081645000
            ]
        );
    }

    #[test]
    fn window_1_min_tumbling_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(0);
        wheel.window(Window::tumbling(Duration::minutes(1)));

        window_1_min_tumbling(wheel);
    }

    fn window_1_min_tumbling(mut wheel: RwWheel<U64SumAggregator>) {
        wheel.insert(Entry::new(10, 15000));
        wheel.insert(Entry::new(20, 45000));
        wheel.insert(Entry::new(30, 75000));
        wheel.insert(Entry::new(40, 105000));

        let results = wheel.advance_to(120000);
        assert_eq!(
            results,
            [
                WindowAggregate {
                    window_start_ms: 0,
                    window_end_ms: 60000,
                    aggregate: 30,
                },
                WindowAggregate {
                    window_start_ms: 60000,
                    window_end_ms: 120000,
                    aggregate: 70,
                },
            ]
        );
    }

    #[test]
    fn window_2_min_tumbling_with_gap_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(0);
        wheel.window(Window::tumbling(Duration::minutes(2)));

        window_2_min_tumbling_with_gap(wheel);
    }

    fn window_2_min_tumbling_with_gap(mut wheel: RwWheel<U64SumAggregator>) {
        wheel.insert(Entry::new(100, 30000));
        wheel.insert(Entry::new(200, 90000));
        wheel.insert(Entry::new(300, 150000));
        wheel.insert(Entry::new(400, 270000));
        wheel.insert(Entry::new(500, 330000));

        let results = wheel.advance_to(360000);
        assert_eq!(
            results,
            [
                WindowAggregate {
                    window_start_ms: 0,
                    window_end_ms: 120000,
                    aggregate: 300
                }, // Sum of entries at 30000 and 90000
                WindowAggregate {
                    window_start_ms: 120000,
                    window_end_ms: 240000,
                    aggregate: 300
                }, // Sum of entries at 150000 and 210000
                WindowAggregate {
                    window_start_ms: 240000,
                    window_end_ms: 360000,
                    aggregate: 900
                }, // Sum of entries at 270000 and 330000
            ]
        );
    }

    #[test]
    fn window_30_sec_range_10_sec_slide_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(Window::sliding(
            Duration::seconds(30),
            Duration::seconds(10),
        ));

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
        assert_eq!(
            results,
            [WindowAggregate {
                window_start_ms: 1533081600000,
                window_end_ms: 1533081630000,
                aggregate: 2845
            }]
        )
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
                WindowAggregate {
                    window_start_ms: 0,
                    window_end_ms: 60000,
                    aggregate: 5
                },
                WindowAggregate {
                    window_start_ms: 10000,
                    window_end_ms: 70000,
                    aggregate: 7
                },
                WindowAggregate {
                    window_start_ms: 20000,
                    window_end_ms: 80000,
                    aggregate: 11
                },
                WindowAggregate {
                    window_start_ms: 30000,
                    window_end_ms: 90000,
                    aggregate: 10
                },
                WindowAggregate {
                    window_start_ms: 40000,
                    window_end_ms: 100000,
                    aggregate: 9
                },
                WindowAggregate {
                    window_start_ms: 50000,
                    window_end_ms: 110000,
                    aggregate: 9
                },
                WindowAggregate {
                    window_start_ms: 60000,
                    window_end_ms: 120000,
                    aggregate: 18
                },
                WindowAggregate {
                    window_start_ms: 70000,
                    window_end_ms: 130000,
                    aggregate: 15
                }
            ]
        );
    }
    #[test]
    fn window_60_sec_range_10_sec_slide_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(0);
        wheel.window(Window::sliding(
            Duration::seconds(60),
            Duration::seconds(10),
        ));
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
                WindowAggregate {
                    window_start_ms: 0,
                    window_end_ms: 120000,
                    aggregate: 23,
                },
                WindowAggregate {
                    window_start_ms: 10000,
                    window_end_ms: 130000,
                    aggregate: 25,
                },
                WindowAggregate {
                    window_start_ms: 20000,
                    window_end_ms: 140000,
                    aggregate: 24,
                },
                WindowAggregate {
                    window_start_ms: 30000,
                    window_end_ms: 150000,
                    aggregate: 23,
                },
                WindowAggregate {
                    window_start_ms: 40000,
                    window_end_ms: 160000,
                    aggregate: 22,
                },
            ]
        );
    }

    #[test]
    fn window_2_min_range_10_sec_slide_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(0);
        wheel.window(Window::sliding(Duration::minutes(2), Duration::seconds(10)));
        window_120_sec_range_10_sec_slide(wheel);
    }
    #[test]
    fn window_10_sec_range_3_sec_slide_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(0);
        wheel.window(Window::sliding(Duration::seconds(10), Duration::seconds(3)));
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
                WindowAggregate {
                    window_start_ms: 0,
                    window_end_ms: 10000,
                    aggregate: 55
                },
                WindowAggregate {
                    window_start_ms: 3000,
                    window_end_ms: 13000,
                    aggregate: 85
                },
                WindowAggregate {
                    window_start_ms: 6000,
                    window_end_ms: 16000,
                    aggregate: 115
                },
                WindowAggregate {
                    window_start_ms: 9000,
                    window_end_ms: 19000,
                    aggregate: 145
                },
                WindowAggregate {
                    window_start_ms: 12000,
                    window_end_ms: 22000,
                    aggregate: 175
                },
            ]
        );
    }

    #[test]
    fn out_of_order_inserts_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(Window::sliding(
            Duration::seconds(30),
            Duration::seconds(10),
        ));
        wheel.insert(Entry::new(300, 1533081625000));
        wheel.insert(Entry::new(100, 1533081605000));
        wheel.insert(Entry::new(200, 1533081615000));
        let results = wheel.advance_to(1533081630000);
        assert_eq!(
            results,
            [WindowAggregate {
                window_start_ms: 1533081600000,
                window_end_ms: 1533081630000,
                aggregate: 600
            }]
        );
    }

    #[test]
    fn edge_case_window_boundaries_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(Window::sliding(
            Duration::seconds(10),
            Duration::seconds(10),
        ));
        wheel.insert(Entry::new(100, 1533081609999)); // Just inside the window
        wheel.insert(Entry::new(200, 1533081610000)); // On the boundary, start of new window (should not be included)
        wheel.insert(Entry::new(300, 1533081610001)); // Just outside the window
        let results = wheel.advance_to(1533081610000);
        assert_eq!(
            results,
            [WindowAggregate {
                window_start_ms: 1533081600000,
                window_end_ms: 1533081610000,
                aggregate: 100
            }]
        );
    }
    #[test]
    fn empty_window_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(Window::sliding(
            Duration::seconds(30),
            Duration::seconds(10),
        ));
        let results = wheel.advance_to(1533081630000);
        assert_eq!(
            results,
            [WindowAggregate {
                window_start_ms: 1533081600000,
                window_end_ms: 1533081630000,
                aggregate: 0
            }]
        );
    }

    #[test]
    #[should_panic]
    fn invalid_window_spec() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1533081600000);
        wheel.window(Window::sliding(
            Duration::seconds(10),
            Duration::seconds(30),
        ));
    }

    #[test]
    fn window_session_10_sec_timeout_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1000);
        wheel.window(Window::session(Duration::seconds(10)));

        window_session_10_sec_timeout(wheel);
    }

    fn window_session_10_sec_timeout(mut wheel: RwWheel<U64SumAggregator>) {
        // First session
        wheel.insert(Entry::new(100, 1000));
        wheel.insert(Entry::new(200, 5000));
        wheel.insert(Entry::new(300, 8000));

        // Gap > 10 seconds, should start a new session
        wheel.insert(Entry::new(150, 20000));
        wheel.insert(Entry::new(250, 25000));

        // Another gap > 10 seconds, should start a third session
        wheel.insert(Entry::new(500, 40000));

        let results = wheel.advance_to(51000);
        assert_eq!(
            results,
            [
                WindowAggregate {
                    window_start_ms: 1000,
                    window_end_ms: 18000,
                    aggregate: 600
                },
                WindowAggregate {
                    window_start_ms: 20000,
                    window_end_ms: 35000,
                    aggregate: 400
                },
                WindowAggregate {
                    window_start_ms: 40000,
                    window_end_ms: 50000,
                    aggregate: 500
                },
            ]
        );

        // Ensure no more results after advancing again
        let no_results = wheel.advance_to(60000);
        assert!(no_results.is_empty());
    }

    #[test]
    fn window_session_multiple_sessions_same_time_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1000);
        wheel.window(Window::session(Duration::seconds(5)));

        // First session
        wheel.insert(Entry::new(100, 1000));
        wheel.insert(Entry::new(200, 3000));

        // Second session (gap > 5 seconds)
        wheel.insert(Entry::new(300, 10000));
        wheel.insert(Entry::new(400, 12000));

        // Third session (gap > 5 seconds)
        wheel.insert(Entry::new(500, 20000));

        // Advance to close all sessions
        let results = wheel.advance_to(26000);
        assert_eq!(
            results,
            [
                // First session
                WindowAggregate {
                    window_start_ms: 1000,
                    window_end_ms: 8000,
                    aggregate: 300
                },
                WindowAggregate {
                    window_start_ms: 10000,
                    window_end_ms: 17000,
                    aggregate: 700,
                },
                WindowAggregate {
                    window_start_ms: 20000,
                    window_end_ms: 25000,
                    aggregate: 500
                },
            ]
        );
    }

    #[test]
    fn window_session_single_entry_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1000);
        wheel.window(Window::session(Duration::seconds(5)));

        wheel.insert(Entry::new(100, 1000));

        // Advance past the session timeout
        let results = wheel.advance_to(7000);
        assert_eq!(
            results,
            [WindowAggregate {
                window_start_ms: 1000,
                window_end_ms: 6000,
                aggregate: 100
            }]
        );
    }
    #[test]
    fn window_session_empty_wheel_test() {
        let mut wheel: RwWheel<U64SumAggregator> = RwWheel::new(1000);
        wheel.window(Window::session(Duration::seconds(5)));

        // Advance an empty wheel
        let results = wheel.advance_to(10000);
        assert!(results.is_empty());
    }
}
