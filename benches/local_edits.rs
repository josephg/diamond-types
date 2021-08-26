// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
// mod testdata;

mod utils;

use criterion::{criterion_group, criterion_main, black_box, Criterion, BenchmarkId};
use crdt_testdata::{load_testing_data, TestPatch, TestTxn};
use smartstring::alias::{String as SmartString};
use diamond_types::*;
use diamond_types::list::*;
use utils::apply_edits;

pub fn local_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("local edits");
    for name in &["automerge-paper", "rustcode", "sveltecomponent"] {
        group.bench_with_input(BenchmarkId::new("yjs", name), name, |b, name| {
            let filename = format!("benchmark_data/{}.json.gz", name);
            let test_data = load_testing_data(&filename);
            assert_eq!(test_data.start_content.len(), 0);

            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_edits(&mut doc, &test_data.txns);
                assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });
    }

    group.finish();

    c.bench_function("kevin", |b| {
        b.iter(|| {
            let mut doc = ListCRDT::new();

            let agent = doc.get_or_create_agent_id("seph");

            for _i in 0..5000000 {
                doc.local_insert(agent, 0, " ".into());
            }
            black_box(doc.len());
        })
    });
}

criterion_group!(benches, local_benchmarks);
criterion_main!(benches);