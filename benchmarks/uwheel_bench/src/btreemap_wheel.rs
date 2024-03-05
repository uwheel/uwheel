use std::collections::BTreeMap;

use awheel::{
    rw_wheel::read::aggregation::combine_or_insert,
    stats::profile_scope,
    time_internal::Duration,
    window::{stats::Stats, WindowExt},
    Aggregator,
};

pub struct BTreeMapWheel<A: Aggregator> {
    range: Duration,
    slide: Duration,
    watermark: u64,
    next_window_end: u64,
    tree: BTreeMap<u64, A::PartialAggregate>,
    stats: Stats,
}

impl<A: Aggregator> BTreeMapWheel<A> {
    pub fn new(watermark: u64, range: Duration, slide: Duration) -> Self {
        Self {
            range,
            slide,
            watermark,
            next_window_end: watermark + range.whole_milliseconds() as u64,
            tree: BTreeMap::default(),
            stats: Default::default(),
        }
    }
}
impl<A: Aggregator> WindowExt<A> for BTreeMapWheel<A> {
    fn advance(&mut self, _duration: Duration) -> Vec<(u64, Option<A::Aggregate>)> {
        Vec::new()
    }
    fn advance_to(&mut self, watermark: u64) -> Vec<(u64, Option<A::Aggregate>)> {
        profile_scope!(&self.stats.advance_ns);

        let diff = watermark.saturating_sub(self.watermark);
        let seconds = Duration::milliseconds(diff as i64).whole_seconds() as u64;
        let mut res = Vec::new();

        for _tick in 0..seconds {
            self.watermark += 1000;
            if self.watermark == self.next_window_end {
                let from = self.watermark - self.range.whole_milliseconds() as u64;
                let to = self.watermark;
                {
                    profile_scope!(&self.stats.window_computation_ns);
                    let mut window: Option<A::PartialAggregate> = None;
                    for (_ts, agg) in self.tree.range(from..to) {
                        combine_or_insert::<A>(&mut window, *agg);
                    }
                    res.push((
                        self.watermark,
                        Some(A::lower(window.unwrap_or(Default::default()))),
                    ));
                }
                let evict_point = (self.watermark - self.range.whole_milliseconds() as u64)
                    + self.slide.whole_milliseconds() as u64;
                let mut split_tree = self.tree.split_off(&(evict_point - 1));
                std::mem::swap(&mut split_tree, &mut self.tree);
                self.next_window_end = self.watermark + self.slide.whole_milliseconds() as u64;
            }
        }
        res
    }
    #[inline]
    fn insert(&mut self, entry: awheel::Entry<A::Input>) {
        profile_scope!(&self.stats.insert_ns);
        if entry.timestamp >= self.watermark {
            let diff = entry.timestamp - self.watermark;
            let seconds = std::time::Duration::from_millis(diff).as_secs();
            let ts = self.watermark + (seconds * 1000);
            self.tree
                .entry(ts)
                .and_modify(|f| {
                    *f = A::combine(*f, A::freeze(A::lift(entry.data)));
                })
                .or_insert(A::freeze(A::lift(entry.data)));
        }
    }
    fn wheel(&self) -> &awheel::ReadWheel<A> {
        unimplemented!();
    }
    fn stats(&self) -> &Stats {
        &self.stats
    }
}
