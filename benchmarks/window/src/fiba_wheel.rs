use crate::*;

use awheel::{
    aggregator::sum::U64SumAggregator,
    stats::Measure,
    time::Duration,
    window::{
        lazy::PairsWheel,
        stats::Stats,
        util::{create_pair_type, pairs_capacity, pairs_space, PairType},
        WindowExt,
    },
};
pub struct BFingerTwoWheel {
    slide: Duration,
    watermark: u64,
    next_window_end: u64,
    fiba: UniquePtr<crate::bfinger_two::FiBA_SUM>,
    stats: Stats,
}
impl BFingerTwoWheel {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        Self {
            slide,
            watermark,
            next_window_end: watermark + range.whole_milliseconds() as u64,
            fiba: crate::bfinger_two::create_fiba_with_sum(),
            stats: Default::default(),
        }
    }
}
impl WindowExt<U64SumAggregator> for BFingerTwoWheel {
    fn advance(
        &mut self,
        _duration: awheel::time::Duration,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as awheel::aggregator::Aggregator>::Aggregate>,
    )> {
        Vec::new()
    }
    fn advance_to(
        &mut self,
        watermark: u64,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as awheel::aggregator::Aggregator>::Aggregate>,
    )> {
        let _adv_measure = Measure::new(&self.stats.advance_ns);
        let mut res = Vec::new();

        self.watermark = watermark;

        if self.watermark == self.next_window_end {
            let _measure = Measure::new(&self.stats.window_computation_ns);
            let from = self.fiba.oldest();
            let to = self.watermark;
            let window = self.fiba.range(from, to);
            let evicts = self.slide.whole_seconds();
            for _i in 0..evicts {
                self.fiba.pin_mut().evict();
            }
            res.push((watermark, Some(window)));
            self.next_window_end = self.watermark + self.slide.whole_milliseconds() as u64;
        }
        res
    }
    #[inline]
    fn insert(
        &mut self,
        entry: awheel::Entry<<U64SumAggregator as awheel::aggregator::Aggregator>::Input>,
    ) {
        let _measure = Measure::new(&self.stats.insert_ns);
        self.fiba.pin_mut().insert(&entry.timestamp, &entry.data);
    }
    fn wheel(&self) -> &awheel::ReadWheel<U64SumAggregator> {
        unimplemented!();
    }
    fn stats(&self) -> &Stats {
        self.stats.size_bytes.set(self.fiba.size());
        &self.stats
    }
}

pub struct BFingerFourWheel {
    range: Duration,
    slide: Duration,
    watermark: u64,
    next_window_end: u64,
    fiba: UniquePtr<crate::bfinger_four::FiBA_SUM_4>,
    stats: Stats,
}
impl BFingerFourWheel {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        Self {
            range,
            slide,
            watermark,
            next_window_end: watermark + range.whole_milliseconds() as u64,
            fiba: crate::bfinger_four::create_fiba_4_with_sum(),
            stats: Default::default(),
        }
    }
}
impl WindowExt<U64SumAggregator> for BFingerFourWheel {
    fn advance(
        &mut self,
        _duration: awheel::time::Duration,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as awheel::aggregator::Aggregator>::Aggregate>,
    )> {
        Vec::new()
    }
    fn advance_to(
        &mut self,
        watermark: u64,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as awheel::aggregator::Aggregator>::Aggregate>,
    )> {
        let diff = watermark.saturating_sub(self.watermark);
        let seconds = Duration::milliseconds(diff as i64).whole_seconds() as u64;
        let _adv_measure = Measure::new(&self.stats.advance_ns);
        let mut res = Vec::new();

        for _tick in 0..seconds {
            self.watermark += 1000;
            if self.watermark == self.next_window_end {
                let from = self.watermark - self.range.whole_milliseconds() as u64;
                let to = self.watermark;
                {
                    let _measure = Measure::new(&self.stats.window_computation_ns);
                    let window = self.fiba.range(from, to - 1);
                    res.push((self.watermark, Some(window)));
                }
                let _measure = Measure::new(&self.stats.cleanup_ns);
                let evict_point = (self.watermark - self.range.whole_milliseconds() as u64)
                    + self.slide.whole_milliseconds() as u64;
                //println!(
                //"watermark {} evict_point {} from {} to {}",
                //self.watermark, evict_point, from, to
                //);
                self.fiba.pin_mut().bulk_evict(&(evict_point - 1));
                self.next_window_end = self.watermark + self.slide.whole_milliseconds() as u64;
            }
        }
        res
    }
    #[inline]
    fn insert(
        &mut self,
        entry: awheel::Entry<<U64SumAggregator as awheel::aggregator::Aggregator>::Input>,
    ) {
        let _measure = Measure::new(&self.stats.insert_ns);
        if entry.timestamp > self.watermark {
            let diff = entry.timestamp - self.watermark;
            let seconds = std::time::Duration::from_millis(diff).as_secs();
            let ts = self.watermark + seconds * 1000;
            println!(
                "Original ts {} new ts {} watermark {}",
                entry.timestamp, ts, self.watermark
            );
            self.fiba.pin_mut().insert(&ts, &entry.data);
        }
    }
    fn wheel(&self) -> &awheel::ReadWheel<U64SumAggregator> {
        unimplemented!();
    }
    fn stats(&self) -> &Stats {
        self.stats.size_bytes.set(self.fiba.size());
        &self.stats
    }
}

pub struct BFingerEightWheel {
    slide: Duration,
    watermark: u64,
    next_window_end: u64,
    fiba: UniquePtr<crate::bfinger_eight::FiBA_SUM_8>,
    stats: Stats,
}
impl BFingerEightWheel {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        Self {
            slide,
            watermark,
            next_window_end: watermark + range.whole_milliseconds() as u64,
            fiba: crate::bfinger_eight::create_fiba_8_with_sum(),
            stats: Default::default(),
        }
    }
}
impl WindowExt<U64SumAggregator> for BFingerEightWheel {
    fn advance(
        &mut self,
        _duration: awheel::time::Duration,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as awheel::aggregator::Aggregator>::Aggregate>,
    )> {
        Vec::new()
    }
    fn advance_to(
        &mut self,
        watermark: u64,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as awheel::aggregator::Aggregator>::Aggregate>,
    )> {
        let _adv_measure = Measure::new(&self.stats.advance_ns);
        let mut res = Vec::new();

        self.watermark = watermark;

        if self.watermark == self.next_window_end {
            let from = self.fiba.oldest();
            let to = self.watermark;
            {
                let _measure = Measure::new(&self.stats.window_computation_ns);
                let window = self.fiba.range(from, to);
                res.push((watermark, Some(window)));
            }
            let _measure = Measure::new(&self.stats.cleanup_ns);
            self.fiba.pin_mut().bulk_evict(&self.watermark);
            self.next_window_end = self.watermark + self.slide.whole_milliseconds() as u64;
        }
        res
    }
    #[inline]
    fn insert(
        &mut self,
        entry: awheel::Entry<<U64SumAggregator as awheel::aggregator::Aggregator>::Input>,
    ) {
        let _measure = Measure::new(&self.stats.insert_ns);
        if entry.timestamp > self.watermark {
            self.fiba.pin_mut().insert(&entry.timestamp, &entry.data);
        }
    }
    fn wheel(&self) -> &awheel::ReadWheel<U64SumAggregator> {
        unimplemented!();
    }
    fn stats(&self) -> &Stats {
        self.stats.size_bytes.set(self.fiba.size());
        &self.stats
    }
}

pub struct PairsFiBA {
    slide: Duration,
    query_pairs: usize,
    watermark: u64,
    pair_ticks_remaining: usize,
    current_pair_len: usize,
    pair_type: PairType,
    // When the next window ends
    next_window_end: u64,
    next_pair_end: u64,
    in_p1: bool,
    pairs_wheel: PairsWheel<U64SumAggregator>,
    fiba: UniquePtr<crate::bfinger_eight::FiBA_SUM_8>,
    stats: Stats,
}
impl PairsFiBA {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        let range_ms = range.whole_milliseconds() as usize;
        let slide_ms = slide.whole_milliseconds() as usize;
        let pair_type = create_pair_type(range_ms, slide_ms);
        let current_pair_len = match pair_type {
            PairType::Even(slide) => slide,
            PairType::Uneven(_, p2) => p2,
        };
        let next_pair_end = watermark + current_pair_len as u64;
        Self {
            slide,
            watermark,
            query_pairs: pairs_space(range_ms, slide_ms),
            next_window_end: watermark + range.whole_milliseconds() as u64,
            pairs_wheel: PairsWheel::with_capacity(pairs_capacity(range_ms, slide_ms)),
            current_pair_len,
            pair_ticks_remaining: current_pair_len / 1000,
            pair_type,
            next_pair_end,
            in_p1: false,
            fiba: crate::bfinger_eight::create_fiba_8_with_sum(),
            stats: Default::default(),
        }
    }
    fn current_pair_duration(&self) -> Duration {
        Duration::milliseconds(self.current_pair_len as i64)
    }
    fn update_pair_len(&mut self) {
        if let PairType::Uneven(p1, p2) = self.pair_type {
            if self.in_p1 {
                self.current_pair_len = p2;
                self.in_p1 = false;
            } else {
                self.current_pair_len = p1;
                self.in_p1 = true;
            }
        }
    }
    // Combines aggregates from the Pairs Wheel in worst-case [2r/s] or best-case [r/s]
    #[inline]
    fn compute_window(&self) -> u64 {
        let _measure = Measure::new(&self.stats.window_computation_ns);
        self.pairs_wheel
            .interval(self.query_pairs)
            .unwrap_or_default()
    }
}
impl WindowExt<U64SumAggregator> for PairsFiBA {
    fn advance(
        &mut self,
        duration: awheel::time::Duration,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as awheel::aggregator::Aggregator>::Aggregate>,
    )> {
        let ticks = duration.whole_seconds();
        let mut window_results = Vec::new();
        for _tick in 0..ticks {
            self.watermark += 1000; // hardcoded for now
            self.pair_ticks_remaining -= 1;

            if self.pair_ticks_remaining == 0 {
                // pair ended
                // query FIBA:
                let from = self.watermark - self.current_pair_len as u64;
                let partial = self.fiba.range(from, self.watermark);

                self.pairs_wheel.push(partial);

                // Update pair metadata
                self.update_pair_len();

                self.next_pair_end = self.watermark + self.current_pair_len as u64;
                self.pair_ticks_remaining = self.current_pair_duration().whole_seconds() as usize;

                if self.watermark == self.next_window_end {
                    // Window computation:
                    let window = self.compute_window();
                    use awheel::aggregator::Aggregator;
                    window_results.push((self.watermark, Some(U64SumAggregator::lower(window))));

                    let _measure = Measure::new(&self.stats.cleanup_ns);

                    // how many "pairs" we need to pop off from the Pairs wheel
                    let removals = match self.pair_type {
                        PairType::Even(_) => 1,
                        PairType::Uneven(_, _) => 2,
                    };
                    for _i in 0..removals {
                        let _ = self.pairs_wheel.tick();
                        self.fiba.pin_mut().evict();
                    }

                    // next window ends at next slide (p1+p2)
                    self.next_window_end += self.slide.whole_milliseconds() as u64;
                }
            }
        }
        window_results
    }
    fn advance_to(
        &mut self,
        watermark: u64,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as awheel::aggregator::Aggregator>::Aggregate>,
    )> {
        let diff = watermark.saturating_sub(self.watermark);
        let _measure = Measure::new(&self.stats.advance_ns);
        self.advance(Duration::milliseconds(diff as i64))
    }
    #[inline]
    fn insert(
        &mut self,
        entry: awheel::Entry<<U64SumAggregator as awheel::aggregator::Aggregator>::Input>,
    ) {
        self.fiba.pin_mut().insert(&entry.timestamp, &entry.data);
    }
    fn wheel(&self) -> &awheel::ReadWheel<U64SumAggregator> {
        unimplemented!();
    }
    fn stats(&self) -> &Stats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use awheel::{
        aggregator::sum::U64SumAggregator,
        time::{Duration, NumericalDuration},
        window::{
            eager::{Builder, EagerWindowWheel},
            lazy,
            lazy::LazyWindowWheel,
        },
        Entry,
    };

    fn window_60_sec_range_10_sec_slide(mut wheel: impl WindowExt<U64SumAggregator>) {
        wheel.insert(Entry::new(1, 9000));
        wheel.insert(Entry::new(1, 15500));
        wheel.insert(Entry::new(1, 25030));
        wheel.insert(Entry::new(1, 35500));
        wheel.insert(Entry::new(1, 59535));

        assert!(wheel.advance_to(59000).is_empty());

        wheel.insert(Entry::new(3, 69000));
        wheel.insert(Entry::new(5, 75000));
        wheel.insert(Entry::new(10, 110000));

        // w1: 5 [0-60]
        // w2: 7 [10-70]
        // w3: 7-1 +5 = 11 [20-80]
        // w4: 10  [30-90]
        // w5: 9 [40-100]
        // w5: 1+3+5+10 [50-110]

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
    fn window_60_sec_range_10_sec_slide_fiba_b4_test() {
        let wheel = BFingerFourWheel::new(0, Duration::minutes(1), Duration::seconds(10));
        window_60_sec_range_10_sec_slide(wheel);
    }
    #[test]
    #[should_panic]
    fn window_60_sec_range_10_sec_slide_fiba_b8_test() {
        let wheel = BFingerEightWheel::new(0, Duration::minutes(1), Duration::seconds(10));
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
