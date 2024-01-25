//! ahweel-window is a sub-crate that contains wheels specialised for periodic window aggregation
#![cfg_attr(not(feature = "std"), no_std)]
//#![deny(missing_docs)]
#![forbid(unsafe_code)]

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

use awheel_core::{aggregator::Aggregator, time_internal::Duration, Entry, ReadWheel};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// A Eager Window Wheel which utilises higher-order aggregates more efficiently (requires invertable aggregation function)
pub mod eager;
/// A Lazy Window Wheel which uses a Pairs Wheel + RwWheel to compute periodic window aggregation
pub mod lazy;
#[doc(hidden)]
pub mod state;
/// Contains functions to help create window wheels
pub mod util;
pub mod wheels;

pub mod soe;

/// Distributed Window Aggregation
pub mod distributed;

/// Various window stats
#[cfg(feature = "stats")]
pub mod stats;

pub use util::{eager_window_query_cost, lazy_window_query_cost, window_wheel};

/// Extension trait for becoming a window wheel
pub trait WindowExt<A: Aggregator> {
    /// Inserts an entry to the Window Wheel
    fn insert(&mut self, entry: Entry<A::Input>);
    /// Advances time by the given duration
    ///
    /// Returns window computations if they have been triggered
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)>;
    /// Advances time to the specified watermark
    ///
    /// Returns window computations if they have been triggered
    fn advance_to(&mut self, watermark: u64) -> Vec<(u64, Option<A::Aggregate>)>;
    /// Returns a reference to the underlying HAW
    fn wheel(&self) -> &ReadWheel<A>;
    /// Returns the current recorded window stats
    #[cfg(feature = "stats")]
    fn stats(&self) -> &stats::Stats;
}

#[cfg(test)]
mod tests {
    use crate::{lazy::LazyWindowWheel, wheels::WindowWheel};
    use awheel_core::{aggregator::sum::U64SumAggregator, Duration, NumericalDuration};
    use eager::{Builder, EagerWindowWheel};

    use super::*;

    #[test]
    fn window_30_sec_range_10_sec_slide_lazy_test() {
        let wheel: LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(Duration::seconds(30))
            .with_slide(Duration::seconds(10))
            .with_watermark(1533081600000)
            .build();
        window_30_sec_range_10_sec_slide(wheel);
    }
    #[test]
    fn window_30_sec_range_10_sec_slide_eager_test() {
        let wheel: EagerWindowWheel<U64SumAggregator> = eager::Builder::default()
            .with_range(Duration::seconds(30))
            .with_slide(Duration::seconds(10))
            .with_watermark(1533081600000)
            .build();
        window_30_sec_range_10_sec_slide(wheel);
    }

    #[test]
    fn window_30_sec_range_10_sec_slide_wheels_test() {
        let wheel: WindowWheel<U64SumAggregator> = wheels::Builder::default()
            .with_range(Duration::seconds(30))
            .with_slide(Duration::seconds(10))
            .with_watermark(1533081600000)
            .build();
        window_30_sec_range_10_sec_slide(wheel);
    }
    fn window_30_sec_range_10_sec_slide(mut wheel: impl WindowExt<U64SumAggregator>) {
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
        assert_eq!(results, [(1533081630000, Some(2845))])
    }

    fn window_60_sec_range_10_sec_slide(mut wheel: impl WindowExt<U64SumAggregator>) {
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
                (60000, Some(5)),
                (70000, Some(7)),
                (80000, Some(11)),
                (90000, Some(10)),
                (100000, Some(9)),
                (110000, Some(9)),
                (120000, Some(18)),
                (130000, Some(15))
            ]
        );
    }
    #[test]
    fn window_60_sec_range_10_sec_slide_lazy_test() {
        let wheel: LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(Duration::minutes(1))
            .with_slide(Duration::seconds(10))
            .build();
        window_60_sec_range_10_sec_slide(wheel);
    }
    #[test]
    fn window_60_sec_range_10_sec_slide_eager_test() {
        let wheel: EagerWindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::minutes(1))
            .with_slide(Duration::seconds(10))
            .build();
        window_60_sec_range_10_sec_slide(wheel);
    }

    #[test]
    fn window_60_sec_range_10_sec_slide_wheels_test() {
        let wheel: WindowWheel<U64SumAggregator> = wheels::Builder::default()
            .with_range(Duration::minutes(1))
            .with_slide(Duration::seconds(10))
            .build();
        window_60_sec_range_10_sec_slide(wheel);
    }

    fn window_120_sec_range_10_sec_slide(mut wheel: impl WindowExt<U64SumAggregator>) {
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
                (120000, Some(23)),
                (130000, Some(25)),
                (140000, Some(24)),
                (150000, Some(23)),
                (160000, Some(22))
            ]
        );
    }

    #[test]
    fn window_2_min_range_10_sec_slide_eager_test() {
        let wheel: EagerWindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::minutes(2))
            .with_slide(Duration::seconds(10))
            .build();

        window_120_sec_range_10_sec_slide(wheel);
    }
    #[test]
    fn window_2_min_range_10_sec_slide_lazy_test() {
        let wheel: LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(Duration::minutes(2))
            .with_slide(Duration::seconds(10))
            .build();

        window_120_sec_range_10_sec_slide(wheel);
    }
    #[test]
    fn window_10_sec_range_3_sec_slide_lazy_test() {
        let wheel: LazyWindowWheel<U64SumAggregator> = lazy::Builder::default()
            .with_range(Duration::seconds(10))
            .with_slide(Duration::seconds(3))
            .build();
        window_10_sec_range_3_sec_slide(wheel);
    }

    #[should_panic]
    #[test]
    fn window_10_sec_range_3_sec_slide_wheels_test() {
        let wheel: WindowWheel<U64SumAggregator> = wheels::Builder::default()
            .with_range(Duration::seconds(10))
            .with_slide(Duration::seconds(3))
            .build();
        window_10_sec_range_3_sec_slide(wheel);
    }
    #[should_panic]
    #[test]
    fn window_10_sec_range_3_sec_slide_eager_test() {
        let wheel: EagerWindowWheel<U64SumAggregator> = Builder::default()
            .with_range(Duration::seconds(10))
            .with_slide(Duration::seconds(3))
            .build();
        window_10_sec_range_3_sec_slide(wheel);
    }

    fn window_10_sec_range_3_sec_slide(mut wheel: impl WindowExt<U64SumAggregator>) {
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
                (10000, Some(55)),
                (13000, Some(85)),
                (16000, Some(115)),
                (19000, Some(145)),
                (22000, Some(175))
            ]
        );
    }
}
