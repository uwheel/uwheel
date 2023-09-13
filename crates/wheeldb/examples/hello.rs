use awheel::{aggregator::sum::I32SumAggregator, time::NumericalDuration};
use wheeldb::WheelDB;

// a tiny streaming database
fn main() {
    let mut db: WheelDB<I32SumAggregator> = WheelDB::open_default("hello");

    let _ = db.read().schedule_once(10000, |haw| {
        println!(
            "Executed a timer at {} with a landmark window result {:?}",
            haw.watermark(),
            haw.landmark()
        );
    });

    // completely fill all wheel slots
    for _i in 0..db.read().remaining_ticks() {
        let timestamp = db.watermark();
        db.insert((1000, timestamp));
        db.advance(1.seconds());
    }

    db.checkpoint();
}
