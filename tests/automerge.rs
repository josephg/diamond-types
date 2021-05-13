use smartstring::SmartString;
use text_crdt_rust::{get_thread_memory_usage, get_thread_num_allocations, LocalOp};
use text_crdt_rust::automerge::DocumentState;
use crdt_testdata::{load_testing_data, TestPatch};

#[test]
fn txn_real_world_data() {
    let test_data = load_testing_data("benchmark_data/sveltecomponent.json.gz");

    assert_eq!(test_data.start_content.len(), 0);
    println!("final length: {}, txns {} patches {}", test_data.end_content.len(), test_data.txns.len(),
             test_data.txns.iter().fold(0, |x, i| x + i.patches.len()));

    let start_alloc = get_thread_memory_usage();

    let mut state = DocumentState::new();
    let id = state.get_or_create_client_id("jeremy");
    let mut local_ops: Vec<LocalOp> = Vec::new();

    for (_i, txn) in test_data.txns.iter().enumerate() {
        local_ops.clear();
        local_ops.extend(txn.patches.iter().map(|TestPatch(pos, del_span, ins_content)| {
            assert!(*pos <= state.len());
            LocalOp {
                pos: *pos,
                del_span: *del_span,
                ins_content: SmartString::from(ins_content.as_str())
            }
        }));

        state.internal_txn(id, local_ops.as_slice());
    }

    assert_eq!(state.len(), test_data.end_content.len());

    state.check();
    state.check_content(test_data.end_content.as_str());

    // state.client_data[0].markers.print_stats();
    // state.range_tree.print_stats();
    println!("alloc {}", get_thread_memory_usage() - start_alloc);
    println!("alloc count {}", get_thread_num_allocations());

    state.print_stats();
}