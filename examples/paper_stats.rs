// This isn't really an example. This runs the automerge-perf data set to check and print memory
// usage for this library.

// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/

// Run with:
// $ cargo run --release --features memusage --example stats

// use std::fs::File;
use std::hint::black_box;
use crdt_testdata::{load_testing_data, TestPatch, TestTxn};
use diamond_types::list::*;

#[cfg(feature = "memusage")]
use trace_alloc::*;
#[cfg(feature = "memusage")]
use humansize::{DECIMAL, format_size};
use diamond_types::list::encoding::EncodeOptions;

pub fn apply_edits_direct(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            if *del_span > 0 {
                doc.delete_without_content(id, *pos .. *pos + *del_span);
            }

            if !ins_content.is_empty() {
                doc.insert(id, *pos, ins_content);
            }
        }
    }
}

// cargo run --example posstats --release --features gen_test_data
#[cfg(feature = "gen_test_data")]
fn write_stats(name: &str, oplog: &ListOpLog) {
    let stats = oplog.get_stats();
    let data = serde_json::to_string_pretty(&stats).unwrap();
    let stats_file = format!("stats_{}.json", name);
    std::fs::write(&stats_file, data).unwrap();
    println!("Wrote stats to {stats_file}");
}

#[allow(unused)]
fn print_stats_for_file(name: &str) {
    let contents = std::fs::read(&format!("benchmark_data/{name}.dt")).unwrap();
    println!("\n\nLoaded testing data from {} ({} bytes)", name, contents.len());

    #[cfg(feature = "memusage")]
        let start_bytes = get_thread_memory_usage();
    #[cfg(feature = "memusage")]
        let start_count = get_thread_num_allocations();

    let oplog = ListOpLog::load_from(&contents).unwrap();
    #[cfg(feature = "memusage")]
    println!("allocated {} bytes in {} blocks",
             format_size((get_thread_memory_usage() - start_bytes) as usize, DECIMAL),
             get_thread_num_allocations() - start_count);

    oplog.print_stats(false);
    print_stats_for_oplog(name, &oplog);
}

fn print_stats_for_oplog(_name: &str, oplog: &ListOpLog) {
    // oplog.make_time_dag_graph("node_cc.svg");

    println!("---- Saving normally ----");
    let data = oplog.encode(EncodeOptions {
        user_data: None,
        store_start_branch_content: false,
        experimentally_store_end_branch_content: false,
        store_inserted_content: true,
        store_deleted_content: false,
        compress_content: true,
        verbose: true
    });
    println!("Regular file size {} bytes", data.len());


    println!("---- Saving smol mode ----");
    let data_smol = oplog.encode(EncodeOptions {
        user_data: None,
        store_start_branch_content: false,
        experimentally_store_end_branch_content: true,
        store_inserted_content: false,
        store_deleted_content: false,
        compress_content: true,
        verbose: true
    });
    println!("Smol size {}", data_smol.len());

    println!("---- Saving uncompressed ----");
    let data_uncompressed = oplog.encode(EncodeOptions {
        user_data: None,
        store_start_branch_content: false,
        experimentally_store_end_branch_content: false,
        store_inserted_content: true,
        store_deleted_content: false,
        compress_content: false,
        verbose: true
    });
    println!("Uncompressed size {}", data_uncompressed.len());

    println!("---- Saving smol uncompressed ----");
    let data_uncompressed = oplog.encode(EncodeOptions {
        user_data: None,
        store_start_branch_content: false,
        experimentally_store_end_branch_content: true,
        store_inserted_content: false,
        store_deleted_content: false,
        compress_content: false,
        verbose: true
    });
    println!("Uncompressed size {}", data_uncompressed.len());

    oplog.bench_writing_xf_since(&[]);

    // oplog.make_time_dag_graph_with_merge_bubbles(&format!("{name}.svg"));

    // print_merge_stats();

    #[cfg(feature = "memusage")]
        let (start_bytes, start_count) = {
        reset_peak_memory_usage();
        (get_thread_memory_usage(), get_thread_num_allocations())
    };

    let state = oplog.checkout_tip().into_inner();

    #[cfg(feature = "memusage")]
    println!("allocated {} bytes in {} blocks, peak usage {}",
             format_size((get_thread_memory_usage() - start_bytes) as usize, DECIMAL),
             get_thread_num_allocations() - start_count,
             get_peak_memory_usage() - start_bytes
    );

    println!("Resulting document size {} characters", state.len_chars());

    #[cfg(feature = "gen_test_data")]
    write_stats(_name, &oplog);
}

#[allow(unused)]
fn profile_merge(name: &str) {
    let contents = std::fs::read(&format!("benchmark_data/{name}.dt")).unwrap();
    let oplog = ListOpLog::load_from(&contents).unwrap();

    for _i in 0..500 {
        black_box(oplog.checkout_tip());
    }
}

fn main() {
    #[cfg(not(feature = "memusage"))]
    eprintln!("NOTE: Memory usage reporting disabled. Run with --release --features memusage");

    #[cfg(debug_assertions)]
    eprintln!("Running in debugging mode. Memory usage not indicative. Run with --release");

    // const PAPER_DATASETS: &[&str] = &["C1"];
    for name in &["S1", "S2", "S3", "C1", "C2", "A1", "A2"] {
        let bytes = std::fs::read(format!("paper_benchmark_data/{name}.dt")).unwrap();

        #[cfg(feature = "memusage")]
            let (start_bytes, start_count) = {
            reset_peak_memory_usage();
            (get_thread_memory_usage(), get_thread_num_allocations())
        };

        {
            let oplog = ListOpLog::load_from(&bytes).unwrap();
            let _state = oplog.checkout_tip().into_inner();
        }

        #[cfg(feature = "memusage")]
        println!("{name}: allocated {} bytes in {} blocks, peak usage {}",
                 format_size((get_thread_memory_usage() - start_bytes) as usize, DECIMAL),
                 get_thread_num_allocations() - start_count,
                 get_peak_memory_usage() - start_bytes
        );
    }

    // print_stats_for_file("node_nodecc");
    // print_stats_for_file("git-makefile");
    //
    // print_stats_for_file("friendsforever");
    // print_stats_for_file("clownschool");

    // profile_merge("clownschool");
}