// This isn't really an example. This runs the automerge-perf data set to check and print memory
// usage for this library.

// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/

// Run with:
// $ cargo run --release --features memusage --example stats

use text_crdt_rust::*;
use crdt_testdata::{load_testing_data, TestPatch, TestTxn};
use smartstring::alias::{String as SmartString};
use text_crdt_rust::universal::YjsDoc;
use criterion::black_box;

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

fn main() {
    #[cfg(not(feature = "memusage"))]
    eprintln!("Warning: Memory usage scanning not enabled. Run with --release --features memusage");

    #[cfg(debug_assertions)]
    eprintln!("Running in debugging mode. Memory usage not indicative. Run with --release");

    let test_data = load_testing_data("benchmark_data/automerge-paper.json.gz");
    assert_eq!(test_data.start_content.len(), 0);

    #[cfg(feature = "memusage")]
    let start_bytes = get_thread_memory_usage();
    #[cfg(feature = "memusage")]
    let start_count = get_thread_num_allocations();

    let mut doc = YjsDoc::new();
    apply_edits(&mut doc, &test_data.txns);
    assert_eq!(doc.len(), test_data.end_content.len());

    #[cfg(feature = "memusage")]
    println!("bytes allocated: {} alloc block count: {}",
             get_thread_memory_usage() - start_bytes,
             get_thread_num_allocations() - start_count);

    // doc.print_stats();
    black_box(doc);
}