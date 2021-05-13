// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
// mod testdata;

use criterion::{black_box, Criterion};
use crdt_testdata::{load_testing_data, TestPatch, TestTxn};
use smartstring::alias::{String as SmartString};
use text_crdt_rust::*;
use text_crdt_rust::yjs::*;

fn apply_edits(doc: &mut YjsDoc, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_client_id("jeremy");

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

        doc.local_txn(id, local_ops.as_slice());
    }
}

pub fn yjs_benchmarks(c: &mut Criterion) {
    c.bench_function("yjs automerge-perf set", |b| {
        let test_data = load_testing_data("benchmark_data/automerge-paper.json.gz");

        // let mut patches: Vec<TestPatch> = Vec::new();
        // for mut v in u.txns.iter() {
        //     patches.extend_from_slice(v.patches.as_slice());
        // }
        assert_eq!(test_data.start_content.len(), 0);

        b.iter(|| {
            #[cfg(feature = "memusage")]
            let start = get_thread_memory_usage();
            let mut doc = YjsDoc::new();
            apply_edits(&mut doc, &test_data.txns);
            // apply_edits_fast(&mut state, &patches);
            // println!("len {}", state.len());
            assert_eq!(doc.len(), test_data.end_content.len());
            #[cfg(feature = "memusage")]
            println!("alloc {} count {}", get_thread_memory_usage() - start, get_thread_num_allocations());
            // doc.print_stats();
            black_box(doc.len());
        })
    });
}