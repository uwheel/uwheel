#[cfg(not(feature = "std"))]
use alloc::{vec, vec::Vec};

/// An enum with different retention policies
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Copy, Default, Clone, Debug)]
pub enum RetentionPolicy {
    #[default]
    /// The default retention policy which does not keep data around
    Drop,
    /// A no limit policy where data is always kept around
    ///
    /// Be careful using this in a low granularity wheel (e.g., milli, seconds)
    Keep,
    /// A policy that retains data but starts evicting at the given limit
    KeepWithLimit(usize),
}

impl RetentionPolicy {
    /// Indicates whether data should be dropped
    pub fn should_drop(&self) -> bool {
        matches!(self, RetentionPolicy::Drop)
    }
    /// Indicates whether data should be kept around
    pub fn should_keep(&self) -> bool {
        matches!(
            self,
            RetentionPolicy::Keep | RetentionPolicy::KeepWithLimit { .. }
        )
    }
    /// Serializes the policy to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            RetentionPolicy::Drop => vec![0],
            RetentionPolicy::Keep => vec![1],
            RetentionPolicy::KeepWithLimit(limit) => {
                let mut bytes = vec![2];
                bytes.extend_from_slice(&(*limit as u32).to_le_bytes());
                bytes
            }
        }
    }
}

/// Configurable Compression Policy for wheels
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Copy, Default, Clone, Debug)]
pub enum CompressionPolicy {
    #[default]
    /// Default policy is to not compress
    Never,
    /// Always compress
    Always,
    /// Set a customized limit to when compression should happen (i.e. when items > limit)
    After(usize),
}

impl CompressionPolicy {
    /// Serializes the compression policy to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            CompressionPolicy::Never => vec![0],
            CompressionPolicy::Always => vec![1],
            CompressionPolicy::After(limit) => {
                let mut bytes = vec![2];
                bytes.extend_from_slice(&(*limit as u32).to_le_bytes());
                bytes
            }
        }
    }
}

/// Configuration for an Aggregation Wheel
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Copy, Clone, Debug)]
pub struct WheelConf {
    pub(crate) capacity: usize,
    pub(crate) watermark: u64,
    pub(crate) compression: CompressionPolicy,
    pub(crate) tick_size_ms: u64,
    pub(crate) retention: RetentionPolicy,
    pub(crate) drill_down: bool,
}

impl WheelConf {
    /// Initiates a new Configuration
    pub fn new(tick_size_ms: u64, capacity: usize) -> Self {
        Self {
            capacity,
            watermark: Default::default(),
            compression: Default::default(),
            tick_size_ms,
            retention: Default::default(),
            drill_down: false,
        }
    }
    /// Sets the drill down flag
    pub fn set_drill_down(&mut self, drill_down: bool) {
        self.drill_down = drill_down;
    }
    /// Configures the wheel to maintain dril-down slots
    pub fn with_drill_down(mut self, drill_down: bool) -> Self {
        self.drill_down = drill_down;
        self
    }
    /// Sets the retention policy for this wheel
    pub fn set_retention_policy(&mut self, policy: RetentionPolicy) {
        self.retention = policy;
    }
    /// Sets the compression for this wheel
    pub fn set_compression_policy(&mut self, policy: CompressionPolicy) {
        self.compression = policy;
    }
    /// Sets the compression for this wheel
    pub fn with_compression_policy(mut self, policy: CompressionPolicy) -> Self {
        self.compression = policy;
        self
    }
    /// Configures the wheel to use the given retention policy
    pub fn with_retention_policy(mut self, policy: RetentionPolicy) -> Self {
        self.retention = policy;
        self
    }
}
