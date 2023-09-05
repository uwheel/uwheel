use std::collections::HashMap;

use awheel_core::{time::Duration, Aggregator, ReadWheel};

use crate::{
    lazy::PairsWheel,
    state::State,
    util::{pairs_capacity, pairs_space, PairType},
};

pub trait DistributedWindowExt<A: Aggregator> {
    fn wheel_advance(&mut self, id: u64, wheel: ReadWheel<A>) -> Vec<(u64, Option<A::Aggregate>)>;
    fn delta_advance(&mut self, id: u64, deltas: &[Option<A::PartialAggregate>]);
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)>;
    fn advance_to(&mut self, watermark: u64) -> Vec<(u64, Option<A::Aggregate>)>;
}

pub struct DistributedWindow<A: Aggregator> {
    range: usize,
    slide: usize,
    wheels: HashMap<u64, ReadWheel<A>>,
    watermark: u64,
    watermarks: HashMap<u64, u64>,
    pairs_wheel: PairsWheel<A>,
    state: State,
    workers: usize,
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
        self.pairs_wheel.interval(pair_slots).unwrap_or_default()
    }
}

impl<A: Aggregator> DistributedWindowExt<A> for DistributedWindow<A> {
    fn wheel_advance(&mut self, id: u64, wheel: ReadWheel<A>) -> Vec<(u64, Option<A::Aggregate>)> {
        self.watermarks.insert(id, wheel.watermark());
        self.wheels.insert(id, wheel);
        self.handle_watermark()
    }
    fn delta_advance(&mut self, _id: u64, _deltas: &[Option<A::PartialAggregate>]) {
        unimplemented!();
    }
    fn advance(&mut self, duration: Duration) -> Vec<(u64, Option<A::Aggregate>)> {
        let ticks = duration.whole_seconds();
        let mut window_results = Vec::new();
        for _tick in 0..ticks {
            self.watermark += 1000;
            self.state.pair_ticks_remaining -= 1;

            if self.state.pair_ticks_remaining == 0 {
                // pair ended:
                // calculate pair partial across all workers
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

                dbg!(partial);

                self.pairs_wheel.push(partial);

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
}

#[cfg(test)]
mod tests {
    use awheel_core::{aggregator::sum::U64SumAggregator, time::Duration, Entry, RwWheel};

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

        for i in 0..3 {
            use awheel_core::time::NumericalDuration;
            for w in &mut workers {
                w.advance(10.seconds());
            }
            dw.wheel_advance(0, workers[0].read().clone());
            dw.wheel_advance(1, workers[1].read().clone());
            dw.wheel_advance(2, workers[2].read().clone());
            // this last worker wheel will trigger the distributed window
            let results = dw.wheel_advance(3, workers[3].read().clone());
            if i == 2 {
                assert_eq!(results, [(1533081630000, Some(2845))])
            }
        }
    }
}
