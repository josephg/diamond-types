// use text_crdt_rust::{get_thread_memory_usage, get_thread_num_allocations};
// use crdt_testdata::{load_testing_data, TestPatch};
//
// #[test]
// fn real_world_data() {
//     // This test also shows up in the benchmarks. Its included here as well because run as part
//     // of the test suite it checks a lot of invariants throughout the run.
//     let data = load_testing_data("benchmark_data/sveltecomponent.json.gz");
//     println!("final length: {}, txns {} patches {}", data.end_content.len(), data.txns.len(),
//              data.txns.iter().fold(0, |x, i| x + i.patches.len()));
//
//     assert_eq!(data.start_content.len(), 0);
//     let start_alloc = get_thread_memory_usage();
//
//     let mut state = CRDTState::new();
//     let id = state.get_or_create_client_id("jeremy");
//     for (_i, txn) in data.txns.iter().enumerate() {
//         for TestPatch(pos, del_len, ins_content) in txn.patches.iter() {
//             // if i % 1000 == 0 {
//             //     println!("i {}", i);
//             // }
//             // println!("iter {} pos {} del {} ins '{}'", _i, pos, del_len, ins_content);
//             assert!(*pos <= state.len());
//             if *del_len > 0 {
//                 state.delete(id, *pos as _, *del_len as _);
//             }
//
//             if !ins_content.is_empty() {
//                 state.insert(id, *pos as _, ins_content);
//             }
//             // println!("after {} len {}", _i, state.len());
//         }
//     }
//     // println!("len {}", state.len());
//     assert_eq!(state.len(), data.end_content.len());
//     // assert!(state.text_content.eq(&u.finalText));
//
//     // state.client_data[0].markers.print_stats();
//     // state.range_tree.print_stats();
//     println!("alloc {}", get_thread_memory_usage() - start_alloc);
//     println!("alloc count {}", get_thread_num_allocations());
//
//     state.print_stats();
//     // println!("final node total {}", state.marker_tree.count_entries());
//     // println!("marker entries {}", state.client_data[0].markers.count_entries());
// }