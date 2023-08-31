use awheel_core::{aggregator::sum::I32SumAggregator, time::NumericalDuration};
use awheel_db::WheelDB;

// a tiny streaming database
fn main() {
    let mut db: WheelDB<I32SumAggregator> = WheelDB::new("hello");

    // completely fill all wheel slots
    for _i in 0..db.read().remaining_ticks() {
        let timestamp = db.watermark();
        db.insert((1000, timestamp));
        db.advance(1.seconds());
    }

    db.checkpoint();
}
