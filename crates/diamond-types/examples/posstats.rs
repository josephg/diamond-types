// This isn't really an example. This runs the automerge-perf data set to check and print memory
// usage for this library.

// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/

// Run with:
// $ cargo run --release --features memusage --example stats

use crdt_testdata::{load_testing_data, TestPatch, TestTxn};
use diamond_types::list::*;
use diamond_types::list::operation::*;

#[cfg(feature = "memusage")]
use diamond_types::alloc::*;
#[cfg(feature = "memusage")]
use humansize::{FileSize, file_size_opts};
use diamond_types::list::encoding::EncodeOptions;

pub fn apply_edits(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut positional: Vec<Operation> = Vec::with_capacity(3);
    // let mut content = String::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            positional.clear();
            // content.clear();

            if *del_span > 0 {
                positional.push(doc.branch.make_delete_op(*pos, *del_span));
            }

            if !ins_content.is_empty() {
                positional.push(Operation::new_insert(*pos, ins_content));
            }

            doc.apply_local_operation(id, positional.as_slice());
        }
    }
}

#[allow(unused)]
fn print_stats_for_testdata(name: &str) {
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

    doc.print_stats(false);

    // let _as_bytes = doc.ops.encode(true);
    let _as_bytes = doc.ops.encode(EncodeOptions {
        verbose: true,
        ..Default::default()
    });
    println!("Branch size {}", doc.len());
    // println!("---\nEncoded size {} (?? What do we include here?)", as_bytes.len());

    let out_file = format!("{}.dt", name);
    let data = doc.ops.encode(EncodeOptions {
        user_data: None,
        store_inserted_content: true,
        store_deleted_content: false,
        verbose: true
    });
    std::fs::write(out_file.clone(), data.as_slice()).unwrap();
    println!("Saved to {}", out_file);
}

#[allow(unused)]
fn print_stats_for_file(name: &str) {
    let contents = std::fs::read(name).unwrap();
    println!("\n\nLoaded testing data from {} ({} bytes)", name, contents.len());

    #[cfg(feature = "memusage")]
    let start_bytes = get_thread_memory_usage();
    #[cfg(feature = "memusage")]
    let start_count = get_thread_num_allocations();

    let oplog = OpLog::load_from(&contents).unwrap();
    #[cfg(feature = "memusage")]
    println!("allocated {} bytes in {} blocks",
             (get_thread_memory_usage() - start_bytes).file_size(file_size_opts::CONVENTIONAL).unwrap(),
             get_thread_num_allocations() - start_count);

    oplog.print_stats(false);
    // oplog.make_time_dag_graph("node_cc.svg");
}

fn main() {
    #[cfg(not(feature = "memusage"))]
    eprintln!("NOTE: Memory usage reporting disabled. Run with --release --features memusage");

    #[cfg(debug_assertions)]
    eprintln!("Running in debugging mode. Memory usage not indicative. Run with --release");

    // print_stats_for_file("node_nodecc.dt");
    print_stats_for_testdata("automerge-paper");
    print_stats_for_testdata("rustcode");
    print_stats_for_testdata("sveltecomponent");
    print_stats_for_testdata("seph-blog1");
}