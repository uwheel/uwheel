use std::collections::HashMap;

use awheel_core::{rw_wheel::read::Eager, time_internal::Duration, Aggregator, ReadWheel};

use crate::{
    lazy::PairsWheel,
    state::State,
    util::{pairs_capacity, pairs_space, PairType},
};
use awheel_core::delta::DeltaState;

#[cfg(feature = "stats")]
mod stats;

#[cfg(feature = "stats")]
use awheel_stats::profile_scope;

pub trait DistributedWindowExt<A: Aggregator> {
    fn merge_deltas(
        &mut self,
        id: u64,
        delta: DeltaState<A::PartialAggregate>,
    ) -> Vec<(u64, Option<A::Aggregate>)>;

    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)>;
    fn advance_to(&mut self, watermark: u64) -> Vec<(u64, Option<A::Aggregate>)>;
    #[cfg(feature = "stats")]
    fn stats(&self) -> &stats::Stats;
}

pub struct DistributedWindow<A: Aggregator> {
    range: usize,
    slide: usize,
    wheels: HashMap<u64, ReadWheel<A, Eager>>,
    watermark: u64,
    watermarks: HashMap<u64, u64>,
    pairs_wheel: PairsWheel<A>,
    state: State,
    workers: usize,
    #[cfg(feature = "stats")]
    stats: stats::Stats,
}

impl<A: Aggregator> DistributedWindow<A> {
    pub fn new(time: u64, range: usize, slide: usize, workers: usize) -> Self {
        let state = State::new(time, range, slide);
        Self {
            range,
            slide,
            pairs_wheel: PairsWheel::with_capacity(pairs_capacity(range, slide)),
            wheels: Default::default(),
            watermarks: HashMap::with_capacity(workers),
            watermark: time,
            state,
            workers,
            #[cfg(feature = "stats")]
            stats: stats::Stats::default(),
        }
    }
    fn handle_watermark(&mut self) -> Vec<(u64, Option<A::Aggregate>)> {
        if self.watermarks.len() == self.workers {
            // if we received watermarks from all workers
            let min: u64 = self.wheels.values().map(|r| r.watermark()).min().unwrap();
            self.watermarks.clear();

            self.advance_to(min)
        } else {
            Vec::new()
        }
    }
    // Combines aggregates from the Pairs Wheel in worst-case [2r/s] or best-case [r/s]
    #[inline]
    fn compute_window(&self) -> A::PartialAggregate {
        let pair_slots = pairs_space(self.range, self.slide);
        self.pairs_wheel.interval(pair_slots).0.unwrap_or_default()
    }
}

impl<A: Aggregator> DistributedWindowExt<A> for DistributedWindow<A> {
    fn merge_deltas(
        &mut self,
        id: u64,
        state: DeltaState<A::PartialAggregate>,
    ) -> Vec<(u64, Option<A::Aggregate>)> {
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.insert_ns);

        let watermark = if let Some(wheel) = self.wheels.get(&id) {
            wheel.delta_advance(state.deltas);
            wheel.watermark()
        } else {
            let wheel = ReadWheel::from_delta_state(state);
            let watermark = wheel.watermark();
            self.wheels.insert(id, wheel);
            watermark
        };

        self.watermarks.insert(id, watermark);
        self.handle_watermark()
    }
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)> {
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.advance_ns);

        let ticks = duration.whole_seconds();
        let mut window_results = Vec::new();
        for _tick in 0..ticks {
            self.watermark += 1000;
            self.state.pair_ticks_remaining -= 1;

            if self.state.pair_ticks_remaining == 0 {
                // pair ended:
                // calculate pair partial across all workers
                #[cfg(feature = "stats")]
                profile_scope!(&self.stats.merge_ns);
                {
                    let partial: Option<A::PartialAggregate> = self
                        .wheels
                        .values()
                        .map(|r| r.interval(self.state.current_pair_duration()))
                        .reduce(|acc, b| match (acc, b) {
                            (Some(curr), Some(agg)) => Some(A::combine(curr, agg)),
                            (None, Some(_)) => b,
                            _ => acc,
                        })
                        .flatten();

                    self.pairs_wheel.push(partial);
                }

                // Update pair metadata
                self.state.update_pair_len();

                self.state.next_pair_end = self.watermark + self.state.current_pair_len as u64;
                self.state.pair_ticks_remaining =
                    self.state.current_pair_duration().whole_seconds() as usize;

                if self.watermark == self.state.next_window_end {
                    // Window computation:
                    let window = self.compute_window();

                    window_results.push((self.watermark, Some(A::lower(window))));

                    // how many "pairs" we need to pop off from the Pairs wheel
                    let removals = match self.state.pair_type {
                        PairType::Even(_) => 1,
                        PairType::Uneven(_, _) => 2,
                    };
                    for _i in 0..removals {
                        let _ = self.pairs_wheel.tick();
                    }

                    // next window ends at next slide (p1+p2)
                    self.state.next_window_end += self.slide as u64;
                }
            }
        }
        window_results
    }
    fn advance_to(&mut self, watermark: u64) -> Vec<(u64, Option<A::Aggregate>)> {
        let diff = watermark.saturating_sub(self.watermark);
        self.advance(Duration::milliseconds(diff as i64))
    }
    #[cfg(feature = "stats")]
    fn stats(&self) -> &stats::Stats {
        &self.stats
    }
}
pub struct DistributedWindowOnly<A: Aggregator> {
    range: usize,
    slide: usize,
    worker_pairs: HashMap<u64, Option<A::PartialAggregate>>,
    watermark: u64,
    watermarks: HashMap<u64, u64>,
    pairs_wheel: PairsWheel<A>,
    state: State,
    workers: usize,
    #[cfg(feature = "stats")]
    stats: stats::Stats,
}

impl<A: Aggregator> DistributedWindowOnly<A> {
    pub fn new(time: u64, range: usize, slide: usize, workers: usize) -> Self {
        let state = State::new(time, range, slide);
        Self {
            range,
            slide,
            pairs_wheel: PairsWheel::with_capacity(pairs_capacity(range, slide)),
            worker_pairs: Default::default(),
            watermarks: HashMap::with_capacity(workers),
            watermark: time,
            state,
            workers,
            #[cfg(feature = "stats")]
            stats: stats::Stats::default(),
        }
    }
    fn handle_watermark(&mut self) -> Vec<(u64, Option<A::Aggregate>)> {
        if self.watermarks.len() == self.workers {
            // if we received watermarks from all workers
            let min: u64 = *self.watermarks.values().min().unwrap();
            self.watermarks.clear();

            self.advance_to(min)
        } else {
            Vec::new()
        }
    }
    // Combines aggregates from the Pairs Wheel in worst-case [2r/s] or best-case [r/s]
    #[inline]
    fn compute_window(&self) -> A::PartialAggregate {
        let pair_slots = pairs_space(self.range, self.slide);
        self.pairs_wheel.interval(pair_slots).0.unwrap_or_default()
    }
}

impl<A: Aggregator> DistributedWindowExt<A> for DistributedWindowOnly<A> {
    fn merge_deltas(
        &mut self,
        id: u64,
        mut state: DeltaState<A::PartialAggregate>,
    ) -> Vec<(u64, Option<A::Aggregate>)> {
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.insert_ns);
        // state -> pair
        let wm = state.oldest_ts;
        let pair = state.deltas[0].take();
        self.worker_pairs.insert(id, pair);
        self.watermarks.insert(id, wm);
        self.handle_watermark()
    }
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)> {
        #[cfg(feature = "stats")]
        profile_scope!(&self.stats.advance_ns);

        let ticks = duration.whole_seconds();
        let mut window_results = Vec::new();
        for _tick in 0..ticks {
            self.watermark += 1000;
            self.state.pair_ticks_remaining -= 1;

            if self.state.pair_ticks_remaining == 0 {
                // pair ended:
                #[cfg(feature = "stats")]
                profile_scope!(&self.stats.merge_ns);
                {
                    // calculate a combined pair across all workers
                    let partial: Option<A::PartialAggregate> = self
                        .worker_pairs
                        .drain()
                        .map(|(_, pa)| pa)
                        .reduce(|acc, b| match (acc, b) {
                            (Some(curr), Some(agg)) => Some(A::combine(curr, agg)),
                            (None, Some(_)) => b,
                            _ => acc,
                        })
                        .flatten();
                    self.pairs_wheel.push(partial);
                }

                // Update pair metadata
                self.state.update_pair_len();

                self.state.next_pair_end = self.watermark + self.state.current_pair_len as u64;
                self.state.pair_ticks_remaining =
                    self.state.current_pair_duration().whole_seconds() as usize;

                if self.watermark == self.state.next_window_end {
                    // Window computation:
                    let window = self.compute_window();

                    window_results.push((self.watermark, Some(A::lower(window))));

                    // how many "pairs" we need to pop off from the Pairs wheel
                    let removals = match self.state.pair_type {
                        PairType::Even(_) => 1,
                        PairType::Uneven(_, _) => 2,
                    };
                    for _i in 0..removals {
                        let _ = self.pairs_wheel.tick();
                    }

                    // next window ends at next slide (p1+p2)
                    self.state.next_window_end += self.slide as u64;
                }
            }
        }
        window_results
    }
    fn advance_to(&mut self, watermark: u64) -> Vec<(u64, Option<A::Aggregate>)> {
        let diff = watermark.saturating_sub(self.watermark);
        self.advance(Duration::milliseconds(diff as i64))
    }
    #[cfg(feature = "stats")]
    fn stats(&self) -> &stats::Stats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use awheel_core::{aggregator::sum::U64SumAggregator, Duration, Entry, RwWheel};

    use super::*;

    #[test]
    fn distributed_30_sec_range_10_sec_slide() {
        let range_ms = Duration::seconds(30).whole_milliseconds() as usize;
        let slide_ms = Duration::seconds(10).whole_milliseconds() as usize;
        let workers = 4;
        let start_time = 1533081600000;
        let mut dw: DistributedWindow<U64SumAggregator> =
            DistributedWindow::new(start_time, range_ms, slide_ms, workers);

        let mut workers: Vec<RwWheel<U64SumAggregator>> = vec![
            RwWheel::new(start_time),
            RwWheel::new(start_time),
            RwWheel::new(start_time),
            RwWheel::new(start_time),
        ];

        // insert on different workers

        workers[0].insert(Entry::new(681, 1533081607321));
        workers[1].insert(Entry::new(625, 1533081619748));
        workers[2].insert(Entry::new(1319, 1533081621175));
        workers[3].insert(Entry::new(220, 1533081626470));
        workers[0].insert(Entry::new(398, 1533081630291));
        workers[1].insert(Entry::new(2839, 1533081662717));
        workers[2].insert(Entry::new(172, 1533081663534));
        workers[3].insert(Entry::new(1133, 1533081664024));
        workers[0].insert(Entry::new(1417, 1533081678095));
        workers[1].insert(Entry::new(195, 1533081679609));

        // simulate distributed scenario
        for i in 0..3 {
            use awheel_core::NumericalDuration;
            for (id, w) in &mut workers.iter_mut().enumerate() {
                let delta = w.advance_and_emit_deltas(10.seconds());
                // "send" deltas to the distributed window and merge
                let results = dw.merge_deltas(id as u64, delta);
                if i == 2 && id == 3 {
                    assert_eq!(results, [(1533081630000, Some(2845))])
                }
            }
        }
    }
}
