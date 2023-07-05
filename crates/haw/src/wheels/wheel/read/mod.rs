/// Aggregation Wheel based on a fixed-sized circular buffer
///
/// This is the core data structure that is reused between different hierarchies (e.g., seconds, minutes, hours, days)
pub mod aggregation;
pub mod inner;

pub use inner::{
    DaysWheel,
    HoursWheel,
    InnerRW,
    MinutesWheel,
    Options,
    SecondsWheel,
    WeeksWheel,
    YearsWheel,
    DAYS,
    HOURS,
    MINUTES,
    SECONDS,
    WEEKS,
    YEARS,
};

use crate::{aggregator::Aggregator, time, wheels::wheel::write::WriteAheadWheel};

pub trait ReadWheelOps<A: Aggregator> {
    /// Creates a new Wheel starting from the given time with drill down enabled
    ///
    /// Time is represented as milliseconds
    fn with_drill_down(time: u64) -> Self;

    /// Creates a new Wheel starting from the given time
    ///
    /// Time is represented as milliseconds
    fn new(time: u64) -> Self;

    fn len(&self) -> usize;

    /// Returns true if the internal wheel time has never been advanced
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if all slots in the hierarchy are utilised
    fn is_full(&self) -> bool;

    /// Returns how many ticks (seconds) are left until the wheel is fully utilised
    fn remaining_ticks(&self) -> u64;

    /// Returns Duration that represents where the wheel currently is in its cycle
    fn current_time_in_cycle(&self) -> time::Duration;

    /// Advance the watermark of the wheel by the given [time::Duration]
    fn advance(&self, duration: time::Duration, waw: &mut WriteAheadWheel<A>);

    /// Advances the time of the wheel aligned by the lowest unit (Second)
    fn advance_to(&self, watermark: u64, waw: &mut WriteAheadWheel<A>);

    /// Clears the state of all wheels
    fn clear(&self);

    /// Return the current watermark as milliseconds for this wheel
    fn watermark(&self) -> u64;
    /// Returns the aggregate in the given time interval
    fn interval_and_lower(&self, dur: time::Duration) -> Option<A::Aggregate>;

    /// Returns the partial aggregate in the given time interval
    fn interval(&self, dur: time::Duration) -> Option<A::PartialAggregate>;

    /// Executes a Landmark Window that combines total partial aggregates across all wheels
    fn landmark(&self) -> Option<A::Aggregate>;

    fn merge(&self, other: &Self);
}

#[cfg(not(feature = "sync"))]
pub mod rw_impl {
    use super::{time, Aggregator, InnerRW, Options, WriteAheadWheel};
    use crate::wheels::wheel::read::ReadWheelOps;
    use core::cell::{Ref, RefCell};

    #[derive(Clone, Debug)]
    pub struct ReadWheel<A: Aggregator> {
        inner: RefCell<InnerRW<A>>,
    }
    impl<A: Aggregator> ReadWheel<A> {
        pub fn raw(&self) -> Ref<InnerRW<A>> {
            self.inner.borrow()
        }
    }

    impl<A: Aggregator> ReadWheelOps<A> for ReadWheel<A> {
        fn with_drill_down(time: u64) -> Self {
            let opts = Options::default().with_drill_down();
            Self {
                inner: RefCell::new(InnerRW::with_options(time, opts)),
            }
        }

        /// Creates a new Wheel starting from the given time
        ///
        /// Time is represented as milliseconds
        fn new(time: u64) -> Self {
            Self {
                inner: RefCell::new(InnerRW::new(time)),
            }
        }
        fn len(&self) -> usize {
            self.inner.borrow().len()
        }

        /// Returns true if the internal wheel time has never been advanced
        fn is_empty(&self) -> bool {
            self.inner.borrow().is_empty()
        }

        /// Returns true if all slots in the hierarchy are utilised
        fn is_full(&self) -> bool {
            self.inner.borrow().is_full()
        }

        /// Returns how many ticks (seconds) are left until the wheel is fully utilised
        fn remaining_ticks(&self) -> u64 {
            self.inner.borrow().remaining_ticks()
        }

        /// Returns Duration that represents where the wheel currently is in its cycle
        #[inline]
        fn current_time_in_cycle(&self) -> time::Duration {
            self.inner.borrow().current_time_in_cycle()
        }

        /// Advance the watermark of the wheel by the given [time::Duration]
        #[inline]
        fn advance(&self, duration: time::Duration, waw: &mut WriteAheadWheel<A>) {
            self.inner.borrow_mut().advance(duration, waw);
        }

        /// Advances the time of the wheel aligned by the lowest unit (Second)
        #[inline]
        fn advance_to(&self, watermark: u64, waw: &mut WriteAheadWheel<A>) {
            self.inner.borrow_mut().advance_to(watermark, waw);
        }

        /// Clears the state of all wheels
        fn clear(&self) {
            self.inner.borrow_mut().clear();
        }

        /// Return the current watermark as milliseconds for this wheel
        #[inline]
        fn watermark(&self) -> u64 {
            self.inner.borrow().watermark()
        }
        /// Returns the aggregate in the given time interval
        fn interval_and_lower(&self, dur: time::Duration) -> Option<A::Aggregate> {
            self.interval(dur).map(|partial| A::lower(partial))
        }

        /// Returns the partial aggregate in the given time interval
        #[inline]
        fn interval(&self, dur: time::Duration) -> Option<A::PartialAggregate> {
            self.inner.borrow().interval(dur)
        }

        /// Executes a Landmark Window that combines total partial aggregates across all wheels
        #[inline]
        fn landmark(&self) -> Option<A::Aggregate> {
            self.inner.borrow().landmark()
        }
        fn merge(&self, other: &Self) {
            self.inner.borrow_mut().merge(&mut other.inner.borrow_mut());
        }
    }
}

#[cfg(feature = "sync")]
pub mod rw_impl {
    use super::{time, Aggregator, InnerRW, Options, WriteAheadWheel};
    use crate::wheels::wheel::read::ReadWheelOps;
    use parking_lot::{lock_api::RwLockReadGuard, RawRwLock, RwLock};
    use std::sync::Arc;

    /// A ReadWheel that can be accessed concurrently
    #[derive(Clone, Debug)]
    pub struct ReadWheel<A: Aggregator> {
        inner: Arc<RwLock<InnerRW<A>>>,
    }
    impl<A: Aggregator> ReadWheelOps<A> for ReadWheel<A> {
        fn with_drill_down(time: u64) -> Self {
            let opts = Options::default().with_drill_down();
            Self {
                inner: Arc::new(RwLock::new(InnerRW::with_options(time, opts))),
            }
        }

        /// Creates a new Wheel starting from the given time
        ///
        /// Time is represented as milliseconds
        fn new(time: u64) -> Self {
            Self {
                inner: Arc::new(RwLock::new(InnerRW::new(time))),
            }
        }

        fn len(&self) -> usize {
            self.inner.read().len()
        }

        /// Returns true if the internal wheel time has never been advanced
        fn is_empty(&self) -> bool {
            self.inner.read().is_empty()
        }

        /// Returns true if all slots in the hierarchy are utilised
        fn is_full(&self) -> bool {
            self.inner.read().is_full()
        }

        /// Returns how many ticks (seconds) are left until the wheel is fully utilised
        fn remaining_ticks(&self) -> u64 {
            self.inner.read().remaining_ticks()
        }

        /// Returns Duration that represents where the wheel currently is in its cycle
        #[inline]
        fn current_time_in_cycle(&self) -> time::Duration {
            self.inner.read().current_time_in_cycle()
        }

        /// Advance the watermark of the wheel by the given [time::Duration]
        #[inline(always)]
        fn advance(&self, duration: time::Duration, waw: &mut WriteAheadWheel<A>) {
            self.inner.write().advance(duration, waw);
        }

        /// Advances the time of the wheel aligned by the lowest unit (Second)
        #[inline(always)]
        fn advance_to(&self, watermark: u64, waw: &mut WriteAheadWheel<A>) {
            self.inner.write().advance_to(watermark, waw);
        }

        /// Clears the state of all wheels
        fn clear(&self) {
            self.inner.write().clear();
        }

        /// Return the current watermark as milliseconds for this wheel
        #[inline]
        fn watermark(&self) -> u64 {
            self.inner.read().watermark()
        }
        /// Returns the aggregate in the given time interval
        fn interval_and_lower(&self, dur: time::Duration) -> Option<A::Aggregate> {
            self.interval(dur).map(|partial| A::lower(partial))
        }

        /// Returns the partial aggregate in the given time interval
        #[inline]
        fn interval(&self, dur: time::Duration) -> Option<A::PartialAggregate> {
            self.inner.read().interval(dur)
        }

        /// Executes a Landmark Window that combines total partial aggregates across all wheels
        #[inline]
        fn landmark(&self) -> Option<A::Aggregate> {
            self.inner.read().landmark()
        }
        fn merge(&self, other: &Self) {
            self.inner.write().merge(&mut other.inner.write());
        }
    }
    impl<A: Aggregator> ReadWheel<A> {
        pub fn raw(&self) -> RwLockReadGuard<'_, RawRwLock, InnerRW<A>> {
            self.inner.read()
        }
    }

    #[allow(unsafe_code)]
    unsafe impl<A: Aggregator> Send for ReadWheel<A> {}

    #[allow(unsafe_code)]
    unsafe impl<A: Aggregator> Sync for ReadWheel<A> {}
}
