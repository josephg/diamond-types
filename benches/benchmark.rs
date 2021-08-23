// mod foo;

mod random_edits;
mod local_edits;
mod remote_edits;
mod ropey;

use criterion::{criterion_group, criterion_main};

criterion_group!(benches,
    local_edits::local_benchmarks,
    remote_edits::remote_apply_benchmarks,
    ropey::ropey_benchmarks,
);
criterion_main!(benches);