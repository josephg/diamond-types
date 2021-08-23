// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
// mod testdata;

use criterion::{black_box, Criterion, BenchmarkId};
use crdt_testdata::{load_testing_data, TestPatch, TestTxn};
use smartstring::alias::{String as SmartString};
use diamond_types::*;
use diamond_types::list::*;

fn apply_edits(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut local_ops: Vec<LocalOp> = Vec::new();

    for (_i, txn) in txns.iter().enumerate() {
        local_ops.clear();
        local_ops.extend(txn.patches.iter().map(|TestPatch(pos, del_span, ins_content)| {
            assert!(*pos <= doc.len());
            LocalOp {
                pos: *pos,
                del_span: *del_span,
                ins_content: SmartString::from(ins_content.as_str())
            }
        }));

        doc.apply_local_txn(id, local_ops.as_slice());
    }
}

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