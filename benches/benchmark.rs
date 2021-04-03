use text_crdt_rust::*;
// use inlinable_string::InlinableString;

use criterion::{black_box, criterion_group, criterion_main, Criterion};


fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("automerge-perf dataset", |b| {
        use serde::Deserialize;
        use serde_json::Result;
        use std::fs::File;
        use std::io::BufReader;
        use std::result;

        #[derive(Debug, Clone, Deserialize)]
        struct Edit(usize, usize, String);

        #[derive(Debug, Clone, Deserialize)]
        struct TestData {
            edits: Vec<Edit>,
            finalText: String,
        }

        let file = File::open("automerge-trace.json").unwrap();
        let reader = BufReader::new(file);
        let u: TestData = serde_json::from_reader(reader).unwrap();
        // println!("final: {}, edits {}", u.finalText.len(), u.edits.len());

        b.iter(|| {
            let mut state = CRDTState::new();
            let id = state.get_or_create_client_id("jeremy");
            for (i, Edit(pos, del_len, ins_content)) in u.edits.iter().enumerate() {
                // if i % 1000 == 0 {
                //     println!("i {}", i);
                // }
                // println!("pos {} del {} ins {}", pos, del_len, ins_content);
                if *del_len > 0 {
                    state.delete(id, *pos as _, *del_len as _);
                } else {
                    state.insert(id, *pos as _, ins_content.len());
                }
            }
            // println!("len {}", state.len());
            assert_eq!(state.len(), u.finalText.len());
            black_box(state.len());
        })
    });

    c.bench_function("insert start", |b| b.iter(|| {
        let mut state = CRDTState::new();
        let id = state.get_or_create_client_id("fred");

        for _ in 0..1000 {
            state.insert(id, 0, 4);
            // state.insert_name("fred", 0, InlinableString::from("fred"));
        }
    }));

    c.bench_function("single append end", |b| b.iter(|| {
        let mut state = CRDTState::new();
        let id = state.get_or_create_client_id("fred");

        let mut pos = 0;
        for _ in 0..1000 {
            state.insert(id, pos, 4);
            pos += 4;
        }
    }));

    c.bench_function("user pair append end", |b| b.iter(|| {
        let mut state = CRDTState::new();
        let fred = state.get_or_create_client_id("fred");
        let george = state.get_or_create_client_id("george");

        let mut pos = 0;
        for _ in 0..1000 {
            state.insert(fred, pos, 4);
            state.insert(george, pos + 4, 6);
            // state.insert_name("fred", pos, InlinableString::from("fred"));
            // state.insert_name("george", pos + 4, InlinableString::from("george"));
            pos += 10;
        }
    }));

}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);