use std::time::SystemTime;

use bitpacking::{BitPacker, BitPacker4x};
use time::OffsetDateTime;
use uwheel::{aggregator, Aggregator};

use aggregator::{Compression, InverseFn};

#[derive(Clone, Debug, Default)]
pub struct SumAggregator;

impl Aggregator for SumAggregator {
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

    fn combine_inverse() -> Option<InverseFn<Self::PartialAggregate>> {
        Some(|a, b| a - b)
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

pub fn into_offset_date_time_start_end(start: u64, end: u64) -> (OffsetDateTime, OffsetDateTime) {
    (
        OffsetDateTime::from_unix_timestamp(start as i64).unwrap(),
        OffsetDateTime::from_unix_timestamp(end as i64).unwrap(),
    )
}

pub fn generate_seconds_range(start_watermark: u64, watermark: u64) -> (u64, u64) {
    // Specify the date range (2023-10-01 to watermark)
    let start_date =
        SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(start_watermark / 1000);
    let end_date = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(watermark / 1000);

    // Convert dates to Unix timestamps
    let start_timestamp = start_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let end_timestamp = end_date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Randomly generate a start time within the specified date range
    let random_start = fastrand::u64(start_timestamp..end_timestamp);

    // Generate a random duration between 1 and (watermark - random_start_seconds) seconds
    let max_duration = end_timestamp - random_start;
    let duration_seconds = fastrand::u64(1..=max_duration);

    (random_start, random_start + duration_seconds)
}
