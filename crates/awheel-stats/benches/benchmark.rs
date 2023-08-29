use awheel_stats::Sketch;
use criterion::{criterion_group, criterion_main, Criterion};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("profile_scope", |b| {
        let sketch = Sketch::default();
        b.iter(|| {
            awheel_stats::profile_scope!(&sketch);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
