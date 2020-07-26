use text_crdt_rust::*;
// use inlinable_string::InlinableString;

use criterion::{black_box, criterion_group, criterion_main, Criterion};


fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("insert start", |b| b.iter(|| {
        let mut state = CRDTState::new();
        let id = state.get_or_create_clientid("fred");

        for _ in 0..1000 {
            state.insert(id, 0, 4);
            // state.insert_name("fred", 0, InlinableString::from("fred"));
        }
    }));

    c.bench_function("single append end", |b| b.iter(|| {
        let mut state = CRDTState::new();
        let id = state.get_or_create_clientid("fred");

        let mut pos = 0;
        for _ in 0..1000 {
            state.insert(id, pos, 4);
            pos += 4;
        }
    }));

    c.bench_function("user pair append end", |b| b.iter(|| {
        let mut state = CRDTState::new();
        let fred = state.get_or_create_clientid("fred");
        let george = state.get_or_create_clientid("george");

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