mod baseline_data;
mod random_edits;
mod automerge;

use criterion::{criterion_group, criterion_main, Criterion};


criterion_group!(benches,
    random_edits::baseline_random_benchmark,
    baseline_data::baseline_benches,
    automerge::am_benchmarks);
criterion_main!(benches);