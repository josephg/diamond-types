// use text_crdt_rust::*;
// use criterion::Criterion;
//
// pub fn baseline_random_benchmark(c: &mut Criterion) {
//
//     c.bench_function("insert start", |b| b.iter(|| {
//         let mut state = CRDTState::new();
//         let id = state.get_or_create_client_id("fred");
//
//         for _ in 0..1000 {
//             state.insert(id, 0, "abcd");
//             // state.insert_name("fred", 0, "fred");
//         }
//     }));
//
//     c.bench_function("single append end", |b| b.iter(|| {
//         let mut state = CRDTState::new();
//         let id = state.get_or_create_client_id("fred");
//
//         let mut pos = 0;
//         for _ in 0..1000 {
//             state.insert(id, pos, "abcd");
//             pos += 4;
//         }
//     }));
//
//     c.bench_function("user pair append end", |b| b.iter(|| {
//         let mut state = CRDTState::new();
//         let fred = state.get_or_create_client_id("fred");
//         let george = state.get_or_create_client_id("george");
//
//         let mut pos = 0;
//         for _ in 0..1000 {
//             state.insert(fred, pos, "abcd");
//             state.insert(george, pos + 4, "123456");
//             // state.insert_name("fred", pos, "fred");
//             // state.insert_name("george", pos + 4, "george");
//             pos += 10;
//         }
//     }));
//
// }
