use bitpacking::{BitPacker, BitPacker4x};
use clap::Parser;
use uwheel::{
    Aggregator,
    Conf,
    RwWheel,
    aggregator::{Compression, sum::U32SumAggregator},
    wheels::read::{
        aggregation::conf::{DataLayout, RetentionPolicy},
        hierarchical::HawConf,
    },
};

pub const CHUNK_SIZE: usize = BitPacker4x::BLOCK_LEN;

#[derive(Clone, Debug, Default)]
pub struct PcoSumAggregator;

impl Aggregator for PcoSumAggregator {
    const IDENTITY: Self::PartialAggregate = 0;

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
        let compressor =
            |slice: &[u32]| pco::standalone::auto_compress(slice, pco::DEFAULT_COMPRESSION_LEVEL);
        let decompressor =
            |slice: &[u8]| pco::standalone::auto_decompress(slice).expect("failed to decompress");
        Some(Compression::new(compressor, decompressor))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BitPackingSumAggregator;

impl Aggregator for BitPackingSumAggregator {
    const IDENTITY: Self::PartialAggregate = 0;

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
        let compressor = |slice: &[u32]| {
            let bitpacker = BitPacker4x::new();
            let num_bits = bitpacker.num_bits(slice);
            let mut compressed = vec![0u8; BitPacker4x::BLOCK_LEN * 4];
            let compressed_len = bitpacker.compress(slice, &mut compressed[..], num_bits);

            // 1 bit for metadata + compressed data
            let mut result = Vec::with_capacity(1 + compressed_len);
            // Prepend metadata
            result.push(num_bits);
            // Append compressed data
            result.extend_from_slice(&compressed[..compressed_len]);

            result
        };
        let decompressor = |bytes: &[u8]| {
            let bit_packer = BitPacker4x::new();
            // Extract num bits metadata
            let bits = bytes[0];
            // Decompress data
            let mut decompressed = vec![0u32; BitPacker4x::BLOCK_LEN];
            bit_packer.decompress(&bytes[1..], &mut decompressed, bits);

            decompressed
        };
        Some(Compression::new(compressor, decompressor))
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value_t = 3600)]
    seconds: usize,
}

fn main() {
    let args = Args::parse();
    let seconds = args.seconds;

    // Pco Configuration
    let mut pco_haw_conf = HawConf::default();
    pco_haw_conf
        .seconds
        .set_retention_policy(RetentionPolicy::Keep);
    pco_haw_conf
        .seconds
        .set_data_layout(DataLayout::Compressed(CHUNK_SIZE));

    let pco_wheel: RwWheel<PcoSumAggregator> =
        RwWheel::with_conf(Conf::default().with_haw_conf(pco_haw_conf));

    // Bitpacking Configuration
    let mut bitpacking_haw_conf = HawConf::default();
    bitpacking_haw_conf
        .seconds
        .set_retention_policy(RetentionPolicy::Keep);
    bitpacking_haw_conf
        .seconds
        .set_data_layout(DataLayout::Compressed(CHUNK_SIZE));

    let bitpacking_wheel: RwWheel<BitPackingSumAggregator> =
        RwWheel::with_conf(Conf::default().with_haw_conf(bitpacking_haw_conf));

    // Regular Configuration
    let mut haw_conf = HawConf::default();
    haw_conf.seconds.set_retention_policy(RetentionPolicy::Keep);

    let wheel: RwWheel<U32SumAggregator> =
        RwWheel::with_conf(Conf::default().with_haw_conf(haw_conf));

    // Generate random deltas
    let deltas: Vec<Option<u32>> = (0..seconds).map(|_| Some(fastrand::u32(1..1000))).collect();

    pco_wheel.read().delta_advance(deltas.clone());
    bitpacking_wheel.read().delta_advance(deltas.clone());
    wheel.read().delta_advance(deltas);

    println!("U32SumAggregator Size {} bytes", wheel.size_bytes());
    println!(
        "BitpackingAggregator Size {} bytes",
        bitpacking_wheel.size_bytes()
    );
    println!("PcoSumAggregator Size {} bytes", pco_wheel.size_bytes());
}
