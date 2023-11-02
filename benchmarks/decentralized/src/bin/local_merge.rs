use awheel::{
    aggregator::sum::U64SumAggregator,
    rw_wheel::read::{
        aggregation::{
            array::{MutablePartialArray, PartialArray},
            conf::{RetentionPolicy, WheelConf},
            AggregationWheel,
            WheelSlot,
        },
        hierarchical::HOUR_TICK_MS,
    },
    Aggregator,
};
use clap::Parser;
use minstant::Instant;
use std::fs::File;

#[derive(Parser, Debug)]
#[clap(author, version, about = "Tool for checking wheel merge perf")]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 10)]
    iterations: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct PlottingOutput {
    id: String,
    exec: Execution,
}

#[derive(Debug, serde::Serialize)]
pub struct Execution {
    wheels: u64,
    stats: Vec<Vec<Stats>>,
}

impl Execution {
    pub fn from(wheels: u64, stats: Vec<Vec<Stats>>) -> Self {
        Self { wheels, stats }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Stats {
    id: String,
    runtime: f64,
    throughput: f64,
    slots: usize,
}

impl PlottingOutput {
    pub fn flush_to_file(&self) -> serde_json::Result<()> {
        #[cfg(not(feature = "simd"))]
        let path = format!("../results/{}.json", self.id);
        #[cfg(feature = "simd")]
        let path = format!("../results/{}_simd.json", self.id);

        let file = File::create(path).unwrap();
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
}

impl PlottingOutput {
    pub fn from(id: &str, exec: Execution) -> Self {
        Self {
            id: id.to_string(),
            exec,
        }
    }
}

fn wheel(slots: usize) -> AggregationWheel<U64SumAggregator> {
    let conf = WheelConf::new(HOUR_TICK_MS, slots).with_retention_policy(RetentionPolicy::Keep);
    let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);
    for _x in 0..slots {
        wheel.insert_slot(WheelSlot::new(Some(10), None));
        wheel.tick();
    }

    wheel
}

fn generate_raw_arrays(wheels: usize, slots: usize) -> Vec<Vec<u8>> {
    let mut result = Vec::with_capacity(wheels);
    let value = 10;

    for _i in 0..wheels {
        let conf = WheelConf::new(HOUR_TICK_MS, slots).with_retention_policy(RetentionPolicy::Keep);
        let mut wheel = AggregationWheel::<U64SumAggregator>::new(conf);
        for _x in 0..slots {
            wheel.insert_slot(WheelSlot::new(Some(value), None));
            wheel.tick();
        }
        result.push(wheel.slots().as_bytes().to_vec());
    }
    result
}

#[inline]
fn merge_wheel<A: Aggregator>(
    wheel: &mut AggregationWheel<A>,
    partial_slice: impl AsRef<[A::PartialAggregate]>,
) {
    wheel.merge_from_ref(partial_slice);
}

fn main() {
    let args = Args::parse();
    let iterations = args.iterations;

    let sizes = [1, 10, 100, 1000];
    let total_wheels = 100000;

    // input: Vec<u8> -> Either MutableArray (vec allocated), or PartialArray (zero-copy)
    let throughput = |elapsed: f64, total| (total as f64 / elapsed) / 1_000_000.0;

    let mut stats = Vec::new();
    for partials in sizes {
        let raw_arrays = generate_raw_arrays(total_wheels, partials);
        // let expected: u64 = ((10 * partials) * (total_wheels + 1)).try_into().unwrap();

        // alloc merge version

        let mut durations = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let mut target_wheel = wheel(partials);
            let now = Instant::now();
            for raw_arr in raw_arrays.iter() {
                let m: MutablePartialArray<U64SumAggregator> =
                    MutablePartialArray::from_bytes(raw_arr);
                merge_wheel(&mut target_wheel, m);
            }
            let elapsed = now.elapsed();
            durations.push(elapsed.as_secs_f64());
        }

        let elapsed: f64 = durations.iter().sum::<f64>() / iterations as f64;

        // assert_eq!(target_wheel.slots().range_query(..), Some(expected));
        println!(
            "Naivé wheel merge with {} partials took {:?} with {} million merges per second",
            partials,
            elapsed,
            throughput(elapsed, total_wheels),
        );

        #[cfg(not(feature = "simd"))]
        let id = "alloc";
        #[cfg(feature = "simd")]
        let id = "alloc_simd";

        let alloc_stats = Stats {
            id: id.to_string(),
            runtime: elapsed,
            throughput: throughput(elapsed, total_wheels),
            slots: partials,
        };

        // Zero-copy merge

        let mut durations = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let mut target_wheel = wheel(partials);
            let now = Instant::now();
            for raw_arr in raw_arrays.iter() {
                let m: PartialArray<'_, U64SumAggregator> = PartialArray::from_bytes(raw_arr);
                merge_wheel(&mut target_wheel, m);
            }
            let elapsed = now.elapsed();
            durations.push(elapsed.as_secs_f64());
        }

        let elapsed = durations.iter().sum::<f64>() / iterations as f64;
        // assert_eq!(target_wheel.slots().range_query(..), Some(expected));

        #[cfg(not(feature = "simd"))]
        let id = "zero_copy";
        #[cfg(feature = "simd")]
        let id = "zero_copy_simd";

        let zero_copy_stats = Stats {
            id: id.to_string(),
            runtime: elapsed,
            throughput: throughput(elapsed, total_wheels),
            slots: partials,
        };

        stats.push(vec![alloc_stats, zero_copy_stats]);

        println!(
            "Zero-copywheel merge with {} partials took {:?} with {} million merges per second",
            partials,
            elapsed,
            throughput(elapsed, total_wheels),
        );
    }

    let execution = Execution::from(total_wheels as u64, stats);
    let output = PlottingOutput::from("local_merge", execution);
    output.flush_to_file().unwrap();

    //     executions.push(execution);

    //     let now = Instant::now();
    //     let no_compressed_bytes = no_compress_wheel.as_bytes();
    //     let no_compress_runtime = now.elapsed();

    //     let now = Instant::now();
    //     let compressed_bytes = compress_wheel.as_bytes();
    //     let compress_runtime = now.elapsed();

    //     let no_compressed_size = no_compressed_bytes.len();
    //     let compressed_size = compressed_bytes.len();
    //     let compress_ratio: f64 = no_compressed_size as f64 / compressed_size as f64;

    //     let stats_1 = Stats {
    //         id: "wheel native".to_string(),
    //         runtime: no_compress_runtime.as_micros() as f64,
    //         total_bytes: no_compressed_size,
    //     };
    //     let stats_2 = Stats {
    //         id: "wheel compression (pco)".to_string(),
    //         runtime: compress_runtime.as_micros() as f64,
    //         total_bytes: compressed_size,
    //     };
    //     let execution = Execution::from(partials, vec![stats_1, stats_2]);

    //     executions.push(execution);

    //     println!("Wheel slots {}, Native Encoding Size {},
    //         Compressed Size (pco) {}, compress ratio {}, native runtime {:?}, compression runtime {:?}",
    //         partials,
    //         no_compressed_size,
    //         compressed_size,
    //         compress_ratio,
    //         no_compress_runtime,
    //         compress_runtime,
    //         );
    // }
    // let output = PlottingOutput::from("u64sumaggregator_encoding", executions);
    // output.flush_to_file().unwrap();
}
