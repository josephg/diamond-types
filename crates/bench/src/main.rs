#![allow(dead_code)]

// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
// mod testdata;
mod utils;

use criterion::{black_box, Criterion, BenchmarkId, Throughput};
use crdt_testdata::{load_testing_data, TestData};
use diamond_types::list::{ListCRDT, ListOpLog};
use diamond_types::list::encoding::*;
use crate::utils::*;

fn testing_data(name: &str) -> TestData {
    let filename = format!("benchmark_data/{}.json.gz", name);
    load_testing_data(&filename)
}

// const LINEAR_DATASETS: &[&str] = &["automerge-paper", "rustcode", "sveltecomponent", "seph-blog1", "friendsforever_flat"];
const LINEAR_DATASETS: &[&str] = &["automerge-paper", "seph-blog1", "clownschool_flat", "friendsforever_flat"];
const COMPLEX_DATASETS: &[&str] = &["automerge-paper", "seph-blog1", "node_nodecc", "git-makefile", "friendsforever", "clownschool"];

fn local_benchmarks(c: &mut Criterion) {
    for name in LINEAR_DATASETS {
        let mut group = c.benchmark_group("dt");
        let test_data = testing_data(name);
        assert_eq!(test_data.start_content.len(), 0);

        group.throughput(Throughput::Elements(test_data.len() as u64));
        // group.throughput(Throughput::Elements(test_data.len_keystrokes() as u64));

        group.bench_function(BenchmarkId::new("local", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_edits_direct(&mut doc, &test_data.txns);
                assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        group.bench_function(BenchmarkId::new("local_push", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_edits_push_merge(&mut doc, &test_data.txns);
                // assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        // group.bench_function(BenchmarkId::new("apply_grouped", name), |b| {
        //     b.iter(|| {
        //         let mut doc = ListCRDT::new();
        //         apply_grouped(&mut doc, &test_data.txns);
        //         // assert_eq!(doc.len(), test_data.end_content.len());
        //         black_box(doc.len());
        //     })
        // });

        // This is obnoxiously fast. Grouping operations using our RLE encoding before applying
        // drops the number of operations from ~260k -> 10k for automerge-paper, and has a
        // corresponding drop in the time taken to apply (12ms -> 0.8ms).
        let grouped_ops_rle = as_grouped_ops_rle(&test_data.txns);
        // dbg!(grouped_ops_rle.len());
        group.bench_function(BenchmarkId::new("local_rle", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_ops(&mut doc, &grouped_ops_rle);
                // assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        group.finish();
    }
}

// This benchmark is good but its existence drops performance of other benchmarks by 20%!!!
// fn encoding_benchmarks(c: &mut Criterion) {
//     for name in DATASETS {
//         let mut group = c.benchmark_group("encoding");
//         let test_data = testing_data(name);
//
//         let mut doc = ListCRDT::new();
//         apply_edits_direct(&mut doc, &test_data.txns);
//         assert_eq!(test_data.start_content.len(), 0);
//
//         // group.throughput(Throughput::Elements(test_data.len() as u64));
//
//         group.bench_function(BenchmarkId::new("encode", name), |b| {
//             b.iter(|| {
//                 let bytes = doc.oplog.encode(ENCODE_FULL);
//                 black_box(bytes);
//             })
//         });
//
//         let bytes = doc.oplog.encode(ENCODE_FULL);
//
//         group.bench_function(BenchmarkId::new("decode_oplog", name), |b| {
//             b.iter(|| {
//                 let doc = ListOpLog::load_from(&bytes).unwrap();
//                 black_box(doc.len());
//             })
//         });
//         group.bench_function(BenchmarkId::new("decode", name), |b| {
//             b.iter(|| {
//                 let doc = ListCRDT::load_from(&bytes).unwrap();
//                 black_box(doc.len());
//             })
//         });
//
//         group.finish();
//     }
// }

fn encoding_nodecc_benchmarks(c: &mut Criterion) {
    for name in COMPLEX_DATASETS {
        let mut group = c.benchmark_group("dt");
        // println!("benchmark_data/{name}.dt");
        let bytes = std::fs::read(format!("benchmark_data/{name}.dt")).unwrap();
        let oplog = ListOpLog::load_from(&bytes).unwrap();
        // group.throughput(Throughput::Bytes(bytes.len() as _));
        group.throughput(Throughput::Elements(oplog.len() as _));

        // Don't care.
        group.bench_function(BenchmarkId::new("decode", name), |b| {
            b.iter(|| {
                let oplog = ListOpLog::load_from(&bytes).unwrap();
                black_box(oplog);
            });
        });

        group.bench_function(BenchmarkId::new("encode", name), |b| {
            b.iter(|| {
                let bytes = oplog.encode(ENCODE_FULL);
                black_box(bytes);
            });
        });

        group.bench_function(BenchmarkId::new("merge", name), |b| {
            b.iter(|| {
                let branch = oplog.checkout_tip();
                black_box(branch);
            });
        });

        group.finish();
    }
}

// criterion_group!(benches,
//     local_benchmarks,
//     encoding_nodecc_benchmarks,
//     // encoding_benchmarks,
// );
// criterion_main!(benches);


fn main() {
    // benches();
    let mut c = Criterion::default()
        .configure_from_args();

    local_benchmarks(&mut c);
    encoding_nodecc_benchmarks(&mut c);
    c.final_summary();
}