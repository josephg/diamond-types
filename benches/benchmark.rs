// mod foo;

mod baseline_data;
mod random_edits;
mod automerge;
mod yjs;
mod ropey;

use criterion::{criterion_group, criterion_main};

criterion_group!(benches,
    random_edits::baseline_random_benchmark,
    baseline_data::baseline_benches,
    automerge::am_benchmarks,
    yjs::yjs_benchmarks,
    ropey::ropey_benchmarks,
);
criterion_main!(benches);