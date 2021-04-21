// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
use criterion::{black_box, Criterion};
use text_crdt_rust::*;
use text_crdt_rust::testdata::{load_testing_data, TestPatch, TestTxn};
use text_crdt_rust::automerge::{DocumentState, LocalOp};
use inlinable_string::InlinableString;

fn apply_edits(state: &mut DocumentState, txns: &Vec<TestTxn>) {
    let id = state.get_or_create_client_id("jeremy");

    let mut local_ops: Vec<LocalOp> = Vec::new();

    for (_i, txn) in txns.iter().enumerate() {
        local_ops.clear();
        local_ops.extend(txn.patches.iter().map(|TestPatch(pos, del_span, ins_content)| {
            assert!(*pos <= state.len());
            LocalOp {
                pos: *pos,
                del_span: *del_span,
                ins_content: InlinableString::from(ins_content.as_str())
            }
        }));

        state.internal_txn(id, local_ops.as_slice());
    }
}

fn apply_edits_fast(state: &mut CRDTState, patches: &[TestPatch]) {
    let id = state.get_or_create_client_id("jeremy");

    for TestPatch(pos, del_len, ins_content) in patches {
        debug_assert!(*pos <= state.len());
        if *del_len > 0 {
            state.delete(id, *pos as _, *del_len as _);
        }

        if !ins_content.is_empty() {
            state.insert(id, *pos as _, ins_content);
        }
    }
}

pub fn am_benchmarks(c: &mut Criterion) {
    c.bench_function("am automerge-perf set", |b| {
        let test_data = load_testing_data("benchmark_data/automerge-paper.json.gz");

        // let mut patches: Vec<TestPatch> = Vec::new();
        // for mut v in u.txns.iter() {
        //     patches.extend_from_slice(v.patches.as_slice());
        // }
        assert_eq!(test_data.start_content.len(), 0);

        b.iter(|| {
            // let start = get_thread_memory_usage();
            let mut state = DocumentState::new();
            apply_edits(&mut state, &test_data.txns);
            // apply_edits_fast(&mut state, &patches);
            // println!("len {}", state.len());
            assert_eq!(state.len(), test_data.end_content.len());
            // println!("alloc {} count {}", get_thread_memory_usage() - start, get_thread_num_allocations());
            // state.print_stats();
            black_box(state.len());
        })
    });
}