use core::{cell::Cell, fmt};

/// Stats for [AggregationWheel]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Default)]
pub struct Stats {
    pub combine_ops: u64,
    pub total_access: Cell<u64>,
    pub scans: Cell<u64>,
}
impl Stats {
    pub fn add_combine_ops(&mut self, ops: u64) {
        self.combine_ops += ops;
    }
    pub fn add_scans(&self, scans: u64) {
        self.scans.set(self.scans.get() + scans);
    }
    pub fn bump_total(&self) {
        self.total_access.set(self.total_access.get() + 1);
    }
}

impl core::fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("HAW Stats")
            .field("combine ops", &self.combine_ops)
            .field("scans", &self.scans.get())
            .field("total access", &self.total_access.get())
            .finish()
    }
}
