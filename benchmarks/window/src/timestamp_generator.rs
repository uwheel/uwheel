#[inline]
fn align_to_closest_thousand(timestamp: u64) -> u64 {
    let remainder = timestamp % 1000;
    if remainder < 500 {
        timestamp - remainder
    } else {
        timestamp + (1000 - remainder)
    }
}

#[allow(dead_code)]
pub struct TimestampGenerator {
    // current watermark
    watermark: u64,
    // How many seconds above the watermark we generate timestamps for.
    max_ooo_secs: u64,
    // Degree of out of order records between (watermark..watermark+max_ooo_secs)
    ooo_degree: f32,
}
impl TimestampGenerator {
    pub fn new(watermark: u64, max_ooo_secs: u64, ooo_degree: f32) -> Self {
        Self {
            watermark,
            max_ooo_secs,
            ooo_degree,
        }
    }
    #[inline]
    pub fn timestamp(&self) -> u64 {
        // generate a timestamp above the current watermark and below the max out of orderness.
        let ts = fastrand::u64(self.watermark..(self.watermark + self.max_ooo_secs));
        align_to_closest_thousand(ts)
    }
    // How often watermark is updated..
    pub fn update_watermark(&mut self, watermark: u64) {
        self.watermark = watermark;
    }
    pub fn watermark(&self) -> u64 {
        self.watermark
    }
}
