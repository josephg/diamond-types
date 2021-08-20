// This isn't really an example. This runs the automerge-perf data set to check and print memory
// usage for this library.

// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/

// Run with:
// $ cargo run --release --features memusage --example stats

use diamond_types::*;
use crdt_testdata::{load_testing_data, TestPatch, TestTxn};
use smartstring::alias::{String as SmartString};
use diamond_types::list::ListCRDT;
use criterion::black_box;

#[cfg(feature = "memusage")]
use humansize::{FileSize, file_size_opts};

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

fn print_stats_for_file(filename: &str) {
    let test_data = load_testing_data(filename);
    assert_eq!(test_data.start_content.len(), 0);
    println!("\n\nLoaded testing data from {}\n ({} patches in {} txns)",
        filename,
        test_data.txns.iter()
            .fold(0, |acc, txn| { acc + txn.patches.len() }),
        test_data.txns.len()
    );

    #[cfg(feature = "memusage")]
    let start_bytes = get_thread_memory_usage();
    #[cfg(feature = "memusage")]
    let start_count = get_thread_num_allocations();

    let mut doc = ListCRDT::new();
    apply_edits(&mut doc, &test_data.txns);
    assert_eq!(doc.len(), test_data.end_content.len());

    #[cfg(feature = "memusage")]
    println!("allocated {} bytes in {} blocks",
        (get_thread_memory_usage() - start_bytes).file_size(file_size_opts::CONVENTIONAL).unwrap(),
         get_thread_num_allocations() - start_count);

    doc.print_stats(false);

    // doc.write_encoding_stats();
    black_box(doc);
}

fn main() {
    #[cfg(not(feature = "memusage"))]
    eprintln!("Warning: Memory usage scanning not enabled. Run with --release --features memusage");

    #[cfg(debug_assertions)]
    eprintln!("Running in debugging mode. Memory usage not indicative. Run with --release");

    print_stats_for_file("benchmark_data/automerge-paper.json.gz");
    print_stats_for_file("benchmark_data/rustcode.json.gz");
}