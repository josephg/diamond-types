// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
// mod testdata;

use criterion::{black_box, Criterion};
use crdt_testdata::{load_testing_data, TestPatch};
use ropey::Rope;
use text_crdt_rust::{get_thread_memory_usage, get_thread_num_allocations};

pub fn ropey_benchmarks(c: &mut Criterion) {
    c.bench_function("ropey baseline", |b| {
        let test_data = load_testing_data("benchmark_data/automerge-paper.json.gz");

        assert_eq!(test_data.start_content.len(), 0);

        b.iter(|| {
            #[cfg(feature = "memusage")]
            let start = get_thread_memory_usage();

            let mut string = Rope::new();
            for txn in test_data.txns.iter() {
                for TestPatch(pos, del_span, ins_content) in txn.patches.iter() {
                    if *del_span > 0 {
                        string.remove(*pos .. *pos + *del_span);
                    }
                    if !ins_content.is_empty() {
                        string.insert(*pos, ins_content.as_str());
                    }
                }
            }

            #[cfg(feature = "memusage")]
            println!("alloc {} count {}", get_thread_memory_usage() - start, get_thread_num_allocations());

            black_box(string);
        })
    });
}