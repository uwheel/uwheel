use core::num::NonZeroUsize;

/// An enum with different retention policies
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Copy, Default, Clone, Debug)]
pub enum RetentionPolicy {
    /// The default retention policy which does not keep data around
    #[default]
    Drop,
    /// A no limit policy where data is always kept around
    ///
    /// Be careful using this in a low granularity wheel (e.g., seconds).
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

/// Enum containing different data layouts
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Copy, Clone, Default, Debug)]
pub enum DataLayout {
    /// The default layout which maintains partial aggregates in its raw format
    #[default]
    Normal,
    /// Configures the data to use compression and compresses at the given chunk size
    ///
    /// # Safety
    ///
    /// The aggregator must implement `compression` otherwise a panic will occur during wheel initialization.
    Compressed(usize),
    /// A prefix-sum data layout that requires double the space of the normal layout
    ///
    /// # Safety
    ///
    /// The aggregator must implement `combine_inverse` otherwise a panic will occur during wheel initialization.
    Prefix,
}

/// Enum containing different wheel modes
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Copy, Clone, Default, Debug, PartialEq)]
pub enum WheelMode {
    /// The default wheel mode which is a online streaming mode
    ///
    /// Indicating that the wheel will be updated incrementally
    #[default]
    Stream,
    /// An index wheel mode
    ///
    /// Indicating that the wheel will be used as read-only analytical index
    ///
    /// # Safety
    ///
    /// If Index mode is used together with explict SIMD support, then wheels must
    /// be converted to SIMD supported wheels.
    Index,
}

/// Configuration for an Aggregate Wheel
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Copy, Clone, Debug)]
pub struct WheelConf {
    /// Defines the base capacity of a wheel (e.g., seconds => 60).
    pub capacity: NonZeroUsize,
    /// Defines the low watermark of a wheel
    pub watermark: u64,
    /// Defines the chosen data layout of a wheel
    pub data_layout: DataLayout,
    /// Defines the ticking size in milliseconds
    ///
    /// A seconds wheel with 1-second slots must have a tick size of 1000 for example.
    pub tick_size_ms: u64,
    /// Defines the data retention policy of a wheel
    pub retention: RetentionPolicy,
    /// Defines the wheel mode
    pub mode: WheelMode,
}

impl WheelConf {
    /// Initiates a new Configuration
    pub fn new(tick_size_ms: u64, capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            watermark: Default::default(),
            data_layout: Default::default(),
            tick_size_ms,
            retention: Default::default(),
            mode: Default::default(),
        }
    }
    /// Sets the watermark
    pub fn with_watermark(mut self, watermark: u64) -> Self {
        self.watermark = watermark;
        self
    }

    /// Configures the wheel to use the given data layout
    pub fn with_data_layout(mut self, layout: DataLayout) -> Self {
        self.data_layout = layout;
        self
    }

    /// Configures the wheel to use the given wheel mode
    pub fn with_mode(mut self, mode: WheelMode) -> Self {
        self.mode = mode;
        self
    }

    /// Configures the wheel to use the given retention policy
    pub fn with_retention_policy(mut self, policy: RetentionPolicy) -> Self {
        self.retention = policy;
        self
    }

    /// Sets the watermark
    pub fn set_watermark(&mut self, watermark: u64) {
        self.watermark = watermark;
    }
    /// Sets the data layout for this wheel
    pub fn set_data_layout(&mut self, layout: DataLayout) {
        self.data_layout = layout;
    }

    /// Sets the mode for this wheel
    pub fn set_mode(&mut self, mode: WheelMode) {
        self.mode = mode;
    }

    /// Sets the retention policy for this wheel
    pub fn set_retention_policy(&mut self, policy: RetentionPolicy) {
        self.retention = policy;
    }
}
