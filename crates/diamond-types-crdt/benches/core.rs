// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
// mod testdata;

mod utils;

use criterion::{criterion_group, criterion_main, black_box, Criterion, BenchmarkId, Throughput};
use crdt_testdata::{load_testing_data, TestData};
use diamond_types_crdt::list::*;
use utils::apply_edits;

fn testing_data(name: &str) -> TestData {
    // println!("{}", name);
    let filename = format!("../../benchmark_data/{}.json.gz", name);
    load_testing_data(&filename)
}

fn list_with_data(test_data: &TestData) -> ListCRDT {
    assert_eq!(test_data.start_content.len(), 0);

    let mut doc = ListCRDT::new();
    apply_edits(&mut doc, &test_data.txns);
    doc
}

// const DATASETS: &[&str] = &["automerge-paper", "seph-blog1"];
const DATASETS: &[&str] = &["automerge-paper", "rustcode", "sveltecomponent", "seph-blog1"];

fn local_benchmarks(c: &mut Criterion) {
    for name in DATASETS {
        let mut group = c.benchmark_group("old/local");
        let test_data = testing_data(name);
        group.throughput(Throughput::Elements(test_data.len() as u64));

        group.bench_function(BenchmarkId::new("yjs", name), |b| {
            b.iter(|| {
                let doc = list_with_data(&test_data);
                assert_eq!(doc.len(), test_data.end_content.len());
                black_box(doc.len());
            })
        });

        group.finish();
    }

    // c.bench_function("kevin", |b| {
    //     b.iter(|| {
    //         let mut doc = ListCRDT::new();
    //
    //         let agent = doc.get_or_create_agent_id("seph");
    //
    //         for _i in 0..5000000 {
    //             doc.local_insert(agent, 0, " ".into());
    //         }
    //         black_box(doc.len());
    //     })
    // });
}

fn remote_benchmarks(c: &mut Criterion) {
    for name in DATASETS {
        let mut group = c.benchmark_group("old/remote");
        let test_data = testing_data(name);
        let src_doc = list_with_data(&test_data);

        group.throughput(Throughput::Elements(test_data.len() as u64));

        group.bench_function(BenchmarkId::new( "generate", name), |b| {
            b.iter(|| {
                let remote_edits: Vec<_> = src_doc.get_all_txns();
                black_box(remote_edits);
            })
        });

        let remote_edits: Vec<_> = src_doc.get_all_txns();
        group.bench_function(BenchmarkId::new( "apply", name), |b| {
            b.iter(|| {
                let mut doc = ListCRDT::new();
                for txn in remote_edits.iter() {
                    doc.apply_remote_txn(&txn);
                }
                assert_eq!(doc.len(), src_doc.len());
                // black_box(doc.len());
            })
        });

        group.finish();
    }
}

fn ot_benchmarks(c: &mut Criterion) {
    for name in DATASETS {
        let mut group = c.benchmark_group("old/ot");
        let test_data = testing_data(name);
        let doc = list_with_data(&test_data);
        group.throughput(Throughput::Elements(test_data.len() as u64));

        group.bench_function(BenchmarkId::new("traversal_since", name), |b| {
            b.iter(|| {
                let changes = doc.traversal_changes_since(0);
                black_box(changes);
            })
        });
    }
}

fn encoding_benchmarks(c: &mut Criterion) {
    for name in DATASETS {
        let mut group = c.benchmark_group("old/encoding");
        let test_data = testing_data(name);
        let doc = list_with_data(&test_data);
        // let mut out = vec![];
        // doc.encode_small(&mut out, false).unwrap();
        // group.throughput(Throughput::Bytes(out.len() as _));
        group.throughput(Throughput::Bytes(doc.encode_small(false).len() as _));

        group.bench_function(BenchmarkId::new("encode_small", name), |b| {
            b.iter(|| {
                // let mut out = vec![];
                // doc.encode_small(&mut out, false).unwrap();
                let encoding = doc.encode_small(false);
                assert!(encoding.len() > 1000);
                black_box(encoding);
            })
        });
        group.bench_function(BenchmarkId::new("encode_patches", name), |b| {
            b.iter(|| {
                // let mut out = vec![];
                // doc.encode_small(&mut out, false).unwrap();
                let encoding = doc.encode_patches(false);
                assert!(encoding.len() > 1000);
                black_box(encoding);
            })
        });
    }
}

criterion_group!(benches,
    local_benchmarks,
    remote_benchmarks,
    ot_benchmarks,
    encoding_benchmarks,
);
criterion_main!(benches);