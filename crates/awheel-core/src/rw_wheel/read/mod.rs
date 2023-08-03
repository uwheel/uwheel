/// Aggregation Wheel based on a fixed-sized circular buffer
///
/// This is the core data structure that is reused between different hierarchies (e.g., seconds, minutes, hours, days)
pub mod aggregation;
/// Hierarchical Aggregation Wheel (HAW)
pub mod hierarchical;

use core::ops::Deref;

pub use hierarchical::{Haw, DAYS, HOURS, MINUTES, SECONDS, WEEKS, YEARS};

use crate::aggregator::Aggregator;

/// A read wheel with hierarchical aggregation wheels backed by interior mutability.
///
/// By default allows a single reader using `RefCell`, and multiple-readers with `sync` flag enabled using `parking_lot`
///
/// `ReadWheel<A: Aggregator>` maintains a [Haw] which is accesible through `Deref<Target = Haw<A>>`
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "A: Default"))]
#[derive(Clone, Debug)]
pub struct ReadWheel<A: Aggregator> {
    inner: Haw<A>,
}
impl<A> Deref for ReadWheel<A>
where
    A: Aggregator,
{
    type Target = Haw<A>;

    fn deref(&self) -> &Haw<A> {
        &self.inner
    }
}
impl<A: Aggregator> ReadWheel<A> {
    /// Creates a new Wheel starting from the given time and with drill down enabled
    ///
    /// Time is represented as milliseconds
    #[doc(hidden)]
    pub fn with_drill_down(time: u64) -> Self {
        Self {
            inner: Haw::with_drill_down(time),
        }
    }

    /// Creates a new Wheel starting from the given time
    ///
    /// Time is represented as milliseconds
    #[doc(hidden)]
    pub fn new(time: u64) -> Self {
        Self {
            inner: Haw::new(time),
        }
    }
}
