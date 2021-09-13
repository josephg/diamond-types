// This isn't really an example. This runs the automerge-perf data set to check and print memory
// usage for this library.

// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/

// Run with:
// $ cargo run --release --features memusage --example stats

use crdt_testdata::{load_testing_data, TestPatch, TestTxn};
use diamond_types::list::{ListCRDT, TraversalComponent};
use TraversalComponent::*;

#[cfg(feature = "memusage")]
use diamond_types::{get_thread_memory_usage, get_thread_num_allocations};
#[cfg(feature = "memusage")]
use humansize::{FileSize, file_size_opts};

pub fn apply_edits(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut traversal: Vec<TraversalComponent> = Vec::with_capacity(3);
    let mut content = String::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            traversal.clear();
            content.clear();
            traversal.push(Retain(*pos as u32));

            if *del_span > 0 {
                traversal.push(Del(*del_span as u32));
            }

            if !ins_content.is_empty() {
                traversal.push(Ins {
                    len: ins_content.chars().count() as u32,
                    content_known: true
                });
                content.push_str(ins_content.as_str());
            }

            doc.apply_local_txn(id, traversal.as_slice(), content.as_str());
        }
    }
}

fn print_stats_for_file(name: &str) {
    let filename = format!("benchmark_data/{}.json.gz", name);
    let test_data = load_testing_data(&filename);
    assert_eq!(test_data.start_content.len(), 0);
    println!("\n\nLoaded testing data from {}\n ({} patches in {} txns -> docsize {} chars)",
        filename,
        test_data.len(),
        test_data.txns.len(),
        test_data.end_content.chars().count()
    );

    #[cfg(feature = "memusage")]
    let start_bytes = get_thread_memory_usage();
    #[cfg(feature = "memusage")]
    let start_count = get_thread_num_allocations();

    let mut doc = ListCRDT::new();
    apply_edits(&mut doc, &test_data.txns);
    assert_eq!(doc.len(), test_data.end_content.chars().count());

    #[cfg(feature = "memusage")]
    println!("allocated {} bytes in {} blocks",
        (get_thread_memory_usage() - start_bytes).file_size(file_size_opts::CONVENTIONAL).unwrap(),
         get_thread_num_allocations() - start_count);

    // doc.print_stats(false);

    let encoded = doc.encode_small(false);
    // println!("Full encoded size {}", encoded.len().file_size(file_size_opts::CONVENTIONAL).unwrap());
    println!("Full encoded size {}", encoded.len());

    // ListCRDT::from_bytes(encoded.as_slice());
}

fn main() {
    #[cfg(not(feature = "memusage"))]
    eprintln!("Warning: Memory usage scanning not enabled. Run with --release --features memusage");

    #[cfg(debug_assertions)]
    eprintln!("Running in debugging mode. Memory usage not indicative. Run with --release");

    print_stats_for_file("automerge-paper");
    print_stats_for_file("rustcode");
    print_stats_for_file("sveltecomponent");
    print_stats_for_file("seph-blog1");
}