// mod foo;

mod random_edits;
mod yjs;
mod ropey;

use criterion::{criterion_group, criterion_main};

criterion_group!(benches,
    // random_edits::baseline_random_benchmark,
    yjs::yjs_benchmarks,
    ropey::ropey_benchmarks,
);
criterion_main!(benches);