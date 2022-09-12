// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
// mod testdata;

mod utils;

use criterion::{criterion_group, criterion_main, black_box, Criterion, BenchmarkId, Throughput};
use crdt_testdata::{load_testing_data, TestData};
use diamond_types::list::{ListCRDT, ListOpLog};
use diamond_types::list::encoding::*;
use crate::utils::*;

fn testing_data(name: &str) -> TestData {
    let filename = format!("benchmark_data/{}.json.gz", name);
    load_testing_data(&filename)
}

const DATASETS: &[&str] = &["automerge-paper", "rustcode", "sveltecomponent", "seph-blog1"];

fn local_benchmarks(c: &mut Criterion) {
    for name in DATASETS {
        let mut group = c.benchmark_group("local");
        let test_data = testing_data(name);
        assert_eq!(test_data.start_content.len(), 0);

        group.throughput(Throughput::Elements(test_data.len() as u64));

        group.bench_function(BenchmarkId::new("apply_local", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_edits_local(&mut doc, &test_data.txns);
                assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        group.bench_function(BenchmarkId::new("apply_push", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_edits_push_merge(&mut doc, &test_data.txns);
                // assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        group.bench_function(BenchmarkId::new("apply_grouped", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                apply_grouped(&mut doc, &test_data.txns);
                // assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        // This is obnoxiously fast. Grouping operations using our RLE encoding before applying
        // drops the number of operations from ~260k -> 10k for automerge-paper, and has a
        // corresponding drop in the time taken to apply (12ms -> 0.8ms).
        let grouped_ops_rle = as_grouped_ops_rle(&test_data.txns);
        // dbg!(grouped_ops_rle.len());
        group.bench_function(BenchmarkId::new("apply_grouped_rle", name), |b| {
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

fn encoding_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("encoding");
    let bytes = std::fs::read("node_nodecc.dt").unwrap();
    let oplog = ListOpLog::load_from(&bytes).unwrap();
    // group.throughput(Throughput::Bytes(bytes.len() as _));
    group.throughput(Throughput::Elements(oplog.len() as _));

    group.bench_function("decode_nodecc", |b| {
        b.iter(|| {
            let oplog = ListOpLog::load_from(&bytes).unwrap();
            black_box(oplog);
        });
    });

    group.bench_function("encode_nodecc", |b| {
        b.iter(|| {
            let bytes = oplog.encode(ENCODE_FULL);
            black_box(bytes);
        });
    });
    // group.bench_function("encode_nodecc_old", |b| {
    //     b.iter(|| {
    //         let bytes = oplog.encode_simple(EncodeOptions {
    //             user_data: None,
    //             store_inserted_content: true,
    //             store_deleted_content: false,
    //             verbose: false
    //         });
    //         black_box(bytes);
    //     });
    // });

    group.bench_function("merge", |b| {
        b.iter(|| {
            let branch = oplog.checkout_tip();
            black_box(branch);
        });
    });

    group.finish();
}

criterion_group!(benches,
    local_benchmarks,
    // remote_benchmarks,
    // ot_benchmarks,
    encoding_benchmarks,
);
criterion_main!(benches);