use awheel::{
    aggregator::sum::U64SumAggregator,
    rw_wheel::read::{
        aggregation::{
            conf::{CompressionPolicy, RetentionPolicy, WheelConf},
            AggregationWheel,
            WheelSlot,
        },
        hierarchical::HOUR_TICK_MS,
    },
};
use clap::Parser;
use minstant::Instant;

#[derive(Parser, Debug)]
#[clap(
    author,
    version,
    about = "Tool for checking compression ratio/perf of AggregationWheel"
)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 100)]
    max_variance: usize,
}

// Hour wheel
fn wheel(policy: CompressionPolicy) -> AggregationWheel<U64SumAggregator> {
    let conf = WheelConf::new(HOUR_TICK_MS, 24)
        .with_retention_policy(RetentionPolicy::Keep)
        .with_compression_policy(policy);
    AggregationWheel::<U64SumAggregator>::new(conf)
}

fn main() {
    let args = Args::parse();
    let max_var = args.max_variance as u64;
    println!("Filling with values in range of 0..{}", max_var);

    let sizes = [10, 100, 1000, 10000, 100000];

    for partials in sizes {
        let mut no_compress_wheel = wheel(CompressionPolicy::Never);
        let mut compress_wheel = wheel(CompressionPolicy::Always);

        for _i in 0..partials {
            let value = fastrand::u64(0..max_var);

            no_compress_wheel.insert_slot(WheelSlot::new(Some(value), None));
            no_compress_wheel.tick();

            compress_wheel.insert_slot(WheelSlot::new(Some(value), None));
            compress_wheel.tick();
        }

        let now = Instant::now();
        let no_compressed_bytes = no_compress_wheel.as_bytes();
        let no_compress_runtime = now.elapsed();

        let now = Instant::now();
        let compressed_bytes = compress_wheel.as_bytes();
        let compress_runtime = now.elapsed();

        let no_compressed_size = no_compressed_bytes.len();
        let compressed_size = compressed_bytes.len();
        let compress_ratio: f64 = no_compressed_size as f64 / compressed_size as f64;

        println!("Wheel slots {}, Native Encoding Size {}, 
            Compressed Size (pco) {}, compress ratio {}, native runtime {:?}, compression runtime {:?}",
            partials,
            no_compressed_size,
            compressed_size,
            compress_ratio,
            no_compress_runtime,
            compress_runtime,
            );
    }
}
