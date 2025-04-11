use criterion::{Criterion, criterion_group, criterion_main};
use uwheel_stats::Sketch;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("profile_scope", |b| {
        let sketch = Sketch::default();
        b.iter(|| {
            uwheel_stats::profile_scope!(&sketch);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
