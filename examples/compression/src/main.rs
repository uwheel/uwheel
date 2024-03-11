use clap::Parser;
use uwheel::{
    aggregator::{sum::U32SumAggregator, Compression},
    rw_wheel::read::{
        aggregation::conf::{DataLayout, RetentionPolicy},
        hierarchical::HawConf,
    },
    Aggregator,
    Options,
    RwWheel,
};

#[derive(Clone, Debug, Default)]
pub struct PcoSumAggregator;

impl Aggregator for PcoSumAggregator {
    const IDENTITY: Self::PartialAggregate = 0;
    type CombineSimd = fn(&[Self::PartialAggregate]) -> Self::PartialAggregate;

    type CombineInverse =
        fn(Self::PartialAggregate, Self::PartialAggregate) -> Self::PartialAggregate;

    type Input = u32;
    type PartialAggregate = u32;
    type MutablePartialAggregate = u32;
    type Aggregate = u32;

    fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
        input
    }
    fn combine_mutable(mutable: &mut Self::MutablePartialAggregate, input: Self::Input) {
        *mutable += input
    }
    fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
        a
    }

    fn combine(a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a + b
    }
    fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
        a
    }

    fn compression() -> Option<Compression<Self::PartialAggregate>> {
        let compressor = Box::new(|slice: &[u32]| {
            pco::standalone::auto_compress(slice, pco::DEFAULT_COMPRESSION_LEVEL)
        });
        let decompressor = Box::new(|slice: &[u8]| {
            pco::standalone::auto_decompress(slice).expect("failed to decompress")
        });
        Some(Compression::new(compressor, decompressor))
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 60)]
    chunk_size: usize,
    #[clap(short, long, value_parser, default_value_t = 3600)]
    seconds: usize,
}

fn main() {
    let args = Args::parse();
    let seconds = args.seconds;
    let mut pco_haw_conf = HawConf::default();
    pco_haw_conf
        .seconds
        .set_retention_policy(RetentionPolicy::Keep);
    let chunk_size = args.chunk_size;
    pco_haw_conf
        .seconds
        .set_data_layout(DataLayout::Compressed(chunk_size));

    let pco_wheel: RwWheel<PcoSumAggregator> =
        RwWheel::with_options(0, Options::default().with_haw_conf(pco_haw_conf));

    let mut haw_conf = HawConf::default();
    haw_conf.seconds.set_retention_policy(RetentionPolicy::Keep);

    let wheel: RwWheel<U32SumAggregator> =
        RwWheel::with_options(0, Options::default().with_haw_conf(haw_conf));

    let deltas: Vec<Option<u32>> = (0..seconds).map(|_| Some(fastrand::u32(1..1000))).collect();

    pco_wheel.read().delta_advance(deltas.clone());
    wheel.read().delta_advance(deltas);

    println!("U32SumAggregator Size {} bytes", wheel.size_bytes());
    println!("PcoSumAggregator Size {} bytes", pco_wheel.size_bytes());
}
