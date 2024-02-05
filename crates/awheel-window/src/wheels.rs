use crate::{
    soe::{TwoStacks, Window},
    state::State,
    util::pairs_space,
    WindowExt,
};

use awheel_core::{
    aggregator::Aggregator,
    rw_wheel::{
        read::{
            hierarchical::{HawConf, WheelRange},
            ReadWheel,
        },
        write::DEFAULT_WRITE_AHEAD_SLOTS,
    },
    time_internal::{Duration, NumericalDuration},
    Entry,
    OffsetDateTime,
    Options,
    RwWheel,
};

#[cfg(feature = "stats")]
use awheel_core::rw_wheel::WheelExt;

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "stats")]
use awheel_stats::profile_scope;

#[derive(Copy, Clone)]
pub struct Builder {
    range: Duration,
    slide: Duration,
    write_ahead: usize,
    time: u64,
    haw_conf: HawConf,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            range: Duration::seconds(0),
            slide: Duration::seconds(0),
            write_ahead: DEFAULT_WRITE_AHEAD_SLOTS,
            time: 0,
            haw_conf: Default::default(),
        }
    }
}

impl Builder {
    /// Configures the builder to create a wheel with the given watermark
    pub fn with_watermark(mut self, watermark: u64) -> Self {
        self.time = watermark;
        self
    }
    /// Configures the builder to create a wheel with the given write-ahead capacity
    pub fn with_write_ahead(mut self, write_ahead: usize) -> Self {
        self.write_ahead = write_ahead;
        self
    }

    pub fn with_haw_conf(mut self, conf: HawConf) -> Self {
        self.haw_conf = conf;
        self
    }
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
    /// Consumes the builder and returns a [WindowWheel]
    pub fn build<A: Aggregator>(self) -> WindowWheel<A> {
        assert!(
            self.range >= self.slide,
            "Range must be larger or equal to slide"
        );

        WindowWheel::new(
            self.time,
            self.write_ahead,
            self.range.whole_milliseconds() as usize,
            self.slide.whole_milliseconds() as usize,
            self.haw_conf,
        )
    }

    pub fn build_raw<A: Aggregator>(self) -> RawWindowWheel<A> {
        assert!(
            self.range >= self.slide,
            "Range must be larger or equal to slide"
        );

        RawWindowWheel::new(
            self.time,
            self.write_ahead,
            self.range.whole_milliseconds() as usize,
            self.slide.whole_milliseconds() as usize,
            self.haw_conf,
        )
    }
}

pub struct WindowWheel<A: Aggregator> {
    slide: usize,
    wheel: RwWheel<A>,
    window: Box<dyn Window<A>>,
    state: State,
    #[cfg(feature = "stats")]
    stats: super::stats::Stats,
}

impl<A: Aggregator> WindowWheel<A> {
    fn new(time: u64, write_ahead: usize, range: usize, slide: usize, haw_conf: HawConf) -> Self {
        let state = State::new(time, range, slide);
        let pairs = pairs_space(range, slide);
        // For now assume always non-invertible (benching reasons)
        let window = Box::new(TwoStacks::with_capacity(pairs));
        // let window: Box<dyn Window<A>> = if A::combine_inverse().is_some() {
        //     Box::new(SubtractOnEvict::with_capacity(pairs))
        // } else {
        //     Box::new(TwoStacks::with_capacity(pairs))
        // };

        Self {
            slide,
            wheel: RwWheel::with_options(
                time,
                Options::default()
                    .with_write_ahead(write_ahead)
                    .with_haw_conf(haw_conf),
            ),
            state,
            window,
            #[cfg(feature = "stats")]
            stats: Default::default(),
        }
    }
    #[inline]
    fn compute_window(&self) -> A::PartialAggregate {
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.window_computation_ns);
        self.window.query()
    }
}

impl<A: Aggregator> WindowExt<A> for WindowWheel<A> {
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)> {
        let ticks = duration.whole_seconds();
        let mut window_results = Vec::new();
        for _tick in 0..ticks {
            self.wheel.advance(1.seconds());
            let watermark = self.wheel.watermark();

            self.state.pair_ticks_remaining -= 1;

            if self.state.pair_ticks_remaining == 0 {
                // pair ended
                let from = watermark - self.state.current_pair_len as u64;
                let to = watermark;
                // dbg!(from, to);

                let start = OffsetDateTime::from_unix_timestamp(from as i64 / 1000).unwrap();
                let end = OffsetDateTime::from_unix_timestamp(to as i64 / 1000).unwrap();
                // dbg!(start, end);

                // query pair from Reader wheel
                let (pair, _cost) = self
                    .wheel
                    .read()
                    .as_ref()
                    .analyze_combine_range(WheelRange::new(start, end));
                // dbg!(pair);

                self.window.push(pair.unwrap_or(A::IDENTITY));

                // Update pair metadata
                self.state.update_pair_len();

                self.state.next_pair_end =
                    self.wheel.read().watermark() + self.state.current_pair_len as u64;
                self.state.pair_ticks_remaining =
                    self.state.current_pair_duration().whole_seconds() as usize;

                if self.wheel.read().watermark() == self.state.next_window_end {
                    let window = self.compute_window();

                    window_results.push((watermark, Some(A::lower(window))));

                    // clean up
                    self.window.pop();

                    // update new window end
                    self.state.next_window_end += self.slide as u64;
                }
            }
        }
        window_results
    }
    fn advance_to(&mut self, watermark: u64) -> Vec<(u64, Option<A::Aggregate>)> {
        let diff = watermark.saturating_sub(self.wheel.read().watermark());
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.advance_ns);
        self.advance(Duration::milliseconds(diff as i64))
    }
    #[inline]
    fn insert(&mut self, entry: Entry<A::Input>) {
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.insert_ns);

        self.wheel.insert(entry);
    }
    /// Returns a reference to the underlying HAW
    fn wheel(&self) -> &ReadWheel<A> {
        self.wheel.read()
    }
    #[cfg(feature = "stats")]
    fn stats(&self) -> &crate::stats::Stats {
        let agg_store_size = self.wheel.read().as_ref().size_bytes();
        let write_ahead_size = self.wheel.write().size_bytes().unwrap();

        let total = agg_store_size + write_ahead_size;

        self.stats.size_bytes.set(total);
        &self.stats
    }
}

pub struct RawWindowWheel<A: Aggregator> {
    range: usize,
    slide: usize,
    next_window_end: u64,
    wheel: RwWheel<A>,
    #[cfg(feature = "stats")]
    stats: super::stats::Stats,
}

impl<A: Aggregator> RawWindowWheel<A> {
    fn new(time: u64, write_ahead: usize, range: usize, slide: usize, haw_conf: HawConf) -> Self {
        Self {
            range,
            slide,
            next_window_end: time + range as u64,
            wheel: RwWheel::with_options(
                time,
                Options::default()
                    .with_write_ahead(write_ahead)
                    .with_haw_conf(haw_conf),
            ),
            #[cfg(feature = "stats")]
            stats: Default::default(),
        }
    }
}

impl<A: Aggregator> WindowExt<A> for RawWindowWheel<A> {
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)> {
        let ticks = duration.whole_seconds();
        let mut window_results = Vec::new();
        for _tick in 0..ticks {
            self.wheel.advance(1.seconds());
            let watermark = self.wheel.watermark();

            if watermark == self.next_window_end {
                let from = watermark - self.range as u64;
                let to = watermark;
                let start = OffsetDateTime::from_unix_timestamp(from as i64 / 1000).unwrap();
                let end = OffsetDateTime::from_unix_timestamp(to as i64 / 1000).unwrap();

                #[cfg(feature = "stats")]
                profile_scope!(&self.stats.window_computation_ns);
                let (window, _cost) = self
                    .wheel
                    .read()
                    .as_ref()
                    .analyze_combine_range(WheelRange::new(start, end));

                #[cfg(feature = "stats")]
                self.stats
                    .window_combines
                    .set(self.stats.window_combines.get() + _cost);

                window_results.push((watermark, window.map(A::lower)));

                self.next_window_end = watermark + self.slide as u64;
            }
        }
        window_results
    }
    fn advance_to(&mut self, watermark: u64) -> Vec<(u64, Option<A::Aggregate>)> {
        let diff = watermark.saturating_sub(self.wheel.read().watermark());
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.advance_ns);
        self.advance(Duration::milliseconds(diff as i64))
    }
    #[inline]
    fn insert(&mut self, entry: Entry<A::Input>) {
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.insert_ns);

        self.wheel.insert(entry);
    }
    /// Returns a reference to the underlying HAW
    fn wheel(&self) -> &ReadWheel<A> {
        self.wheel.read()
    }
    #[cfg(feature = "stats")]
    fn stats(&self) -> &crate::stats::Stats {
        let agg_store_size = self.wheel.read().as_ref().size_bytes();
        let write_ahead_size = self.wheel.write().size_bytes().unwrap();

        let total = agg_store_size + write_ahead_size;

        self.stats.size_bytes.set(total);
        &self.stats
    }
}
