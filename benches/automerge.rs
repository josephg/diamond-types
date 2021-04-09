// This benchmark interacts with the automerge-perf data set from here:
// https://github.com/automerge/automerge-perf/
use criterion::{black_box, Criterion};
use text_crdt_rust::*;
use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Deserialize)]
struct Edit(usize, usize, String);

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)] // field names match JSON.
struct TestData {
    edits: Vec<Edit>,
    finalText: String,
}

fn get_data() -> TestData {

    let file = File::open("automerge-trace.json").unwrap();
    let reader = BufReader::new(file);
    let u: TestData = serde_json::from_reader(reader).unwrap();
    // println!("final: {}, edits {}", u.finalText.len(), u.edits.len());

    return u;
}

fn apply_edits(state: &mut CRDTState, edits: &Vec<Edit>) {
    let id = state.get_or_create_client_id("jeremy");
    for Edit(pos, del_len, ins_content) in edits.iter() {
        // println!("pos {} del {} ins {}", pos, del_len, ins_content);
        if *del_len > 0 {
            state.delete(id, *pos as _, *del_len as _);
        } else {
            state.insert(id, *pos as _, ins_content);
        }
    }
}

pub fn automerge_perf_benchmarks(c: &mut Criterion) {
    c.bench_function("automerge-perf dataset", |b| {
        let u = get_data();

        b.iter(|| {
            // println!("alloc {}", ALLOCATED.load(Ordering::Acquire));
            let mut state = CRDTState::new();
            apply_edits(&mut state, &u.edits);
            // println!("len {}", state.len());
            assert_eq!(state.len(), u.finalText.len());
            // println!("alloc {}", ALLOCATED.load(Ordering::Acquire));
            black_box(state.len());
        })
    });

    if false {
        c.bench_function("automerge-perf x100", |b| {
            let u = get_data();

            b.iter(|| {
                // println!("alloc {}", ALLOCATED.load(Ordering::Acquire));

                let mut state = CRDTState::new();
                for _ in 0..100 {
                    apply_edits(&mut state, &u.edits);
                }
                // println!("len {}", state.len());
                assert_eq!(state.len(), u.finalText.len() * 100);
                // println!("alloc {}", ALLOCATED.load(Ordering::Acquire));

                black_box(state.len());
            })
        });
    }
}