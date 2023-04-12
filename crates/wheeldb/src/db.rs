use smallvec::SmallVec;
use std::path::PathBuf;

pub type Key = SmallVec<[u8; 16]>;

pub struct WheelDB {}

impl WheelDB {
    pub fn open_default(_path: impl Into<PathBuf>) -> Self {
        Self {}
    }
}
