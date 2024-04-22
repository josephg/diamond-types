//! This file contains benchmarks for replaying index traces

use std::fs::File;
use std::io::{BufReader, Read};
use criterion::{BenchmarkId, Criterion};
use flate2::bufread::GzDecoder;
use diamond_types::IndexTreeTrace;

const DATASETS: &[&str] = &["node_nodecc", "git-makefile", "friendsforever", "clownschool"];

pub fn idxtrace_benchmarks(c: &mut Criterion) {
    for name in DATASETS {
        let mut group = c.benchmark_group("dt");

        let filename = format!("benchmark_data/idxtrace_{name}.json.gz");
        let reader = BufReader::new(File::open(filename).unwrap());
        let mut reader = GzDecoder::new(reader);
        let mut raw_json = vec!();
        reader.read_to_end(&mut raw_json).unwrap();

        let trace = IndexTreeTrace::from_json(&raw_json);

        group.bench_function(BenchmarkId::new("idxtrace", name), |b| {
            b.iter(|| {
                trace.replay();
            })
        });
    }
}
