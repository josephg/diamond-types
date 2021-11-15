// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
// mod testdata;

mod utils;

use criterion::{criterion_group, criterion_main, black_box, Criterion, BenchmarkId, Throughput};
use crdt_testdata::{load_testing_data, TestData};
use diamond_types_positional::list::*;
use crate::utils::*;

fn testing_data(name: &str) -> TestData {
    let filename = format!("benchmark_data/{}.json.gz", name);
    load_testing_data(&filename)
}

const DATASETS: &[&str] = &["automerge-paper", "rustcode", "sveltecomponent", "seph-blog1"];

fn local_benchmarks(c: &mut Criterion) {
    for name in DATASETS {
        let mut group = c.benchmark_group("local");
        let test_data = testing_data(name);
        assert_eq!(test_data.start_content.len(), 0);

        group.throughput(Throughput::Elements(test_data.len() as u64));

        group.bench_function(BenchmarkId::new("apply_local", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_edits_local(&mut doc, &test_data.txns);
                assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        group.bench_function(BenchmarkId::new("apply_push", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_edits_push_merge(&mut doc, &test_data.txns);
                // assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        group.finish();
    }
}

criterion_group!(benches,
    local_benchmarks,
    // remote_benchmarks,
    // ot_benchmarks,
    // encoding_benchmarks,
);
criterion_main!(benches);