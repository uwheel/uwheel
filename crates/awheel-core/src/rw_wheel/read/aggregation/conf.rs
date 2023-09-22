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
    /// Indicates whether data should be kept around
    pub fn should_keep(&self) -> bool {
        matches!(
            self,
            RetentionPolicy::Keep | RetentionPolicy::KeepWithLimit { .. }
        )
    }
}

/// Configuration for an Aggregation Wheel
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Copy, Clone, Debug)]
pub struct WheelConf {
    pub(crate) capacity: usize,
    pub(crate) retention: RetentionPolicy,
    pub(crate) drill_down: bool,
}

impl WheelConf {
    /// Initiates a new Configuration
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
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
    /// Configures the wheel to use the given retention policy
    pub fn with_retention_policy(mut self, policy: RetentionPolicy) -> Self {
        self.retention = policy;
        self
    }
}
