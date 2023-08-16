use awheel::{
    aggregator::sum::U64SumAggregator,
    time::NumericalDuration,
    window::{
        eager::{Builder, EagerWindowWheel},
        *,
    },
    Entry,
};
fn main() {
    let mut window: EagerWindowWheel<U64SumAggregator> = Builder::default()
        .with_range(10.seconds())
        .with_slide(3.seconds())
        .build();
    for i in 1..=22 {
        window.insert(Entry::new(i, i * 1000 - 1));
    }
    let results = window.advance(22.seconds());
    println!("{:#?}", results);
}
