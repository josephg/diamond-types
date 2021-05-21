// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/

use criterion::{black_box, Criterion};
use text_crdt_rust::*;
use crdt_testdata::{load_testing_data, TestPatch, TestTxn};

fn apply_edits(state: &mut CRDTState, txns: &Vec<TestTxn>) {
    let id = state.get_or_create_client_id("jeremy");
    for txn in txns.iter() {
        for TestPatch(pos, del_len, ins_content) in txn.patches.iter() {
            debug_assert!(*pos <= state.len());
            if *del_len > 0 {
                state.delete(id, *pos as _, *del_len as _);
            }

            if !ins_content.is_empty() {
                state.insert(id, *pos as _, ins_content);
            }
        }
    }
}

pub fn baseline_benches(c: &mut Criterion) {
    c.bench_function("baseline automerge-perf dataset", |b| {
        let u = load_testing_data("benchmark_data/automerge-paper.json.gz");

        // let mut patches: Vec<TestPatch> = Vec::new();
        // for mut v in u.txns.iter() {
        //     patches.extend_from_slice(v.patches.as_slice());
        // }
        assert_eq!(u.start_content.len(), 0);

        b.iter(|| {
            // let start = get_thread_memory_usage();
            let mut state = CRDTState::new();
            apply_edits(&mut state, &u.txns);
            // apply_edits_fast(&mut state, &patches);
            // println!("len {}", state.len());
            assert_eq!(state.len(), u.end_content.len());
            // println!("alloc {} count {}", get_thread_memory_usage() - start, get_thread_num_allocations());
            // state.print_stats();
            black_box(state.len());
        })
    });

    if false {
        c.bench_function("baseline automerge-perf x100", |b| {
            let u = load_testing_data("benchmark_data/automerge-paper.json.gz");
            assert_eq!(u.start_content.len(), 0);

            b.iter(|| {
                // let start = ALLOCATED.load(Ordering::Acquire);

                let mut state = CRDTState::new();
                for _ in 0..100 {
                    apply_edits(&mut state, &u.txns);
                }
                // println!("len {}", state.len());
                assert_eq!(state.len(), u.end_content.len() * 100);
                // println!("alloc {}", ALLOCATED.load(Ordering::Acquire) - start);
                // state.print_stats();

                black_box(state.len());
            })
        });
    }
}