use fiba_rs::*;

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
    fiba: UniquePtr<fiba_rs::bfinger_two::FiBA_SUM>,
    stats: Stats,
}
impl BFingerTwoWheel {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        Self {
            slide,
            watermark,
            next_window_end: watermark + range.whole_milliseconds() as u64,
            fiba: fiba_rs::bfinger_two::create_fiba_with_sum(),
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
    ) -> Result<(), awheel::Error<<U64SumAggregator as awheel::aggregator::Aggregator>::Input>>
    {
        let _measure = Measure::new(&self.stats.insert_ns);
        self.fiba.pin_mut().insert(&entry.timestamp, &entry.data);
        Ok(())
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
    slide: Duration,
    watermark: u64,
    next_window_end: u64,
    fiba: UniquePtr<fiba_rs::bfinger_four::FiBA_SUM_4>,
    stats: Stats,
}
impl BFingerFourWheel {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        Self {
            slide,
            watermark,
            next_window_end: watermark + range.whole_milliseconds() as u64,
            fiba: fiba_rs::bfinger_four::create_fiba_4_with_sum(),
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
    ) -> Result<(), awheel::Error<<U64SumAggregator as awheel::aggregator::Aggregator>::Input>>
    {
        let _measure = Measure::new(&self.stats.insert_ns);
        self.fiba.pin_mut().insert(&entry.timestamp, &entry.data);
        Ok(())
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
    fiba: UniquePtr<fiba_rs::bfinger_eight::FiBA_SUM_8>,
    stats: Stats,
}
impl BFingerEightWheel {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        Self {
            slide,
            watermark,
            next_window_end: watermark + range.whole_milliseconds() as u64,
            fiba: fiba_rs::bfinger_eight::create_fiba_8_with_sum(),
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
            let evicts = self.slide.whole_seconds();
            for _i in 0..evicts {
                // TODO: bulk evict?
                self.fiba.pin_mut().evict();
            }
            self.next_window_end = self.watermark + self.slide.whole_milliseconds() as u64;
        }
        res
    }
    #[inline]
    fn insert(
        &mut self,
        entry: awheel::Entry<<U64SumAggregator as awheel::aggregator::Aggregator>::Input>,
    ) -> Result<(), awheel::Error<<U64SumAggregator as awheel::aggregator::Aggregator>::Input>>
    {
        let _measure = Measure::new(&self.stats.insert_ns);
        self.fiba.pin_mut().insert(&entry.timestamp, &entry.data);
        Ok(())
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
    fiba: UniquePtr<fiba_rs::bfinger_eight::FiBA_SUM_8>,
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
            fiba: fiba_rs::bfinger_eight::create_fiba_8_with_sum(),
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
    ) -> Result<(), awheel::Error<<U64SumAggregator as awheel::aggregator::Aggregator>::Input>>
    {
        self.fiba.pin_mut().insert(&entry.timestamp, &entry.data);
        Ok(())
    }
    fn wheel(&self) -> &awheel::ReadWheel<U64SumAggregator> {
        unimplemented!();
    }
    fn stats(&self) -> &Stats {
        &self.stats
    }
}
