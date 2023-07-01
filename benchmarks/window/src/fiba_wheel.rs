use fiba_rs::*;

use haw::{aggregator::U64SumAggregator, time::Duration, wheels::window::WindowWheel};
pub struct BFingerTwoWheel {
    slide: Duration,
    watermark: u64,
    next_window_end: u64,
    fiba: UniquePtr<fiba_rs::bfinger_two::FiBA_SUM>,
}
impl BFingerTwoWheel {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        Self {
            slide,
            watermark,
            next_window_end: watermark + range.whole_milliseconds() as u64,
            fiba: fiba_rs::bfinger_two::create_fiba_with_sum(),
        }
    }
}
impl WindowWheel<U64SumAggregator> for BFingerTwoWheel {
    fn advance(
        &mut self,
        _duration: haw::time::Duration,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as haw::aggregator::Aggregator>::Aggregate>,
    )> {
        Vec::new()
    }
    fn advance_to(
        &mut self,
        watermark: u64,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as haw::aggregator::Aggregator>::Aggregate>,
    )> {
        let mut res = Vec::new();

        self.watermark = watermark;

        if self.watermark == self.next_window_end {
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
        entry: haw::Entry<<U64SumAggregator as haw::aggregator::Aggregator>::Input>,
    ) -> Result<(), haw::Error<<U64SumAggregator as haw::aggregator::Aggregator>::Input>> {
        self.fiba.pin_mut().insert(&entry.timestamp, &entry.data);
        Ok(())
    }
    fn wheel(&self) -> &haw::Wheel<U64SumAggregator> {
        unimplemented!();
    }
}

pub struct BFingerFourWheel {
    slide: Duration,
    watermark: u64,
    next_window_end: u64,
    fiba: UniquePtr<fiba_rs::bfinger_four::FiBA_SUM_4>,
}
impl BFingerFourWheel {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        Self {
            slide,
            watermark,
            next_window_end: watermark + range.whole_milliseconds() as u64,
            fiba: fiba_rs::bfinger_four::create_fiba_4_with_sum(),
        }
    }
}
impl WindowWheel<U64SumAggregator> for BFingerFourWheel {
    fn advance(
        &mut self,
        _duration: haw::time::Duration,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as haw::aggregator::Aggregator>::Aggregate>,
    )> {
        Vec::new()
    }
    fn advance_to(
        &mut self,
        watermark: u64,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as haw::aggregator::Aggregator>::Aggregate>,
    )> {
        let mut res = Vec::new();

        self.watermark = watermark;

        if self.watermark == self.next_window_end {
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
        entry: haw::Entry<<U64SumAggregator as haw::aggregator::Aggregator>::Input>,
    ) -> Result<(), haw::Error<<U64SumAggregator as haw::aggregator::Aggregator>::Input>> {
        self.fiba.pin_mut().insert(&entry.timestamp, &entry.data);
        Ok(())
    }
    fn wheel(&self) -> &haw::Wheel<U64SumAggregator> {
        unimplemented!();
    }
}

pub struct BFingerEightWheel {
    slide: Duration,
    watermark: u64,
    next_window_end: u64,
    fiba: UniquePtr<fiba_rs::bfinger_eight::FiBA_SUM_8>,
}
impl BFingerEightWheel {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        Self {
            slide,
            watermark,
            next_window_end: watermark + range.whole_milliseconds() as u64,
            fiba: fiba_rs::bfinger_eight::create_fiba_8_with_sum(),
        }
    }
}
impl WindowWheel<U64SumAggregator> for BFingerEightWheel {
    fn advance(
        &mut self,
        _duration: haw::time::Duration,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as haw::aggregator::Aggregator>::Aggregate>,
    )> {
        Vec::new()
    }
    fn advance_to(
        &mut self,
        watermark: u64,
    ) -> Vec<(
        u64,
        Option<<U64SumAggregator as haw::aggregator::Aggregator>::Aggregate>,
    )> {
        let mut res = Vec::new();

        self.watermark = watermark;

        if self.watermark == self.next_window_end {
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
        entry: haw::Entry<<U64SumAggregator as haw::aggregator::Aggregator>::Input>,
    ) -> Result<(), haw::Error<<U64SumAggregator as haw::aggregator::Aggregator>::Input>> {
        self.fiba.pin_mut().insert(&entry.timestamp, &entry.data);
        Ok(())
    }
    fn wheel(&self) -> &haw::Wheel<U64SumAggregator> {
        unimplemented!();
    }
}
