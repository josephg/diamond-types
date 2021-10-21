use rand::prelude::*;
use crate::list::*;
use crate::list::external_txn::RemoteId;
use crate::test_helpers::*;

#[derive(Debug)]
struct ListWithHistory(ListCRDT, RleVec<KVPair<PositionalOp>>);

impl ListWithHistory {
    fn new() -> Self {
        ListWithHistory(ListCRDT::new(), RleVec::new())
    }

    fn replicate_into(&self, dest: &mut ListWithHistory) {
        let ListWithHistory(dest_doc, dest_ops) = dest;
        let clock = dest_doc.get_vector_clock();
        let time_ranges = self.0.get_time_spans_since::<Vec<_>>(&clock);

        // dbg!(&time_ranges);

        for span in time_ranges.iter() {
            let mut pos = 0;
            while pos < span.len {

                // println!();
                // println!();
                // println!();
                let (remote_span, parents) = self.0.next_remote_id_span(TimeSpan { start: span.start + pos, len: span.len - pos });

                let dest_agent = dest_doc.get_or_create_agent_id(remote_span.id.agent.as_str());
                let dest_parents = dest_doc.remote_ids_to_branch(&parents);
                // dest_doc.apply_patch_at_version(dest_agent, )

                let (ops, offset) = self.1.find_packed(span.start + pos);
                ops.1.check();
                assert_eq!(offset, 0);

                // println!("Agent {} / Parents {:?}", dest_agent, dest_parents);
                // println!("Merging {:?}", &ops.1);

                let time = dest_doc.get_next_time();
                // println!("Op merging at time {}", time);
                dest_ops.push(KVPair(time, ops.1.clone()));
                dest_doc.apply_patch_at_version(dest_agent, (&ops.1).into(), dest_parents.as_slice());

                // pos += remote_span.len;
                pos += ops.1.len() as u32;
            }
            assert!(pos <= span.len);
        }
    }
}

impl Default for ListWithHistory {
    fn default() -> Self { Self::new() }
}


type HistoryItem = (Vec<RemoteId>, PositionalOp);

// fn replicate_positional(dest: &mut ListCRDT, src: &ListCRDT) {
//     let clock = dest.get_vector_clock();
//     let time_ranges = src.get_time_spans_since::<Vec<_>>(&clock);
//     for mut span in time_ranges {
//         dbg!(&span);
//         while !span.is_empty() {
//
//         }
//     }
// }

fn run_fuzzer_iteration(seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut docs = [
        (ListWithHistory::new(), ListCRDT::new()),
        (ListWithHistory::new(), ListCRDT::new()),
        (ListWithHistory::new(), ListCRDT::new())
    ];

    // Each document will have a different local agent ID. I'm cheating here - just making agent
    // 0 for all of them.
    for (i, doc) in docs.iter_mut().enumerate() {
        doc.0.0.get_or_create_agent_id(format!("agent {}", i).as_str());
        doc.1.get_or_create_agent_id(format!("agent {}", i).as_str());
    }

    for _i in 0..300 {
        // println!("Iteration {}", _i);

        // Generate some operations
        for _j in 0..5 {
        // for _j in 0..1 {
            let doc_idx = rng.gen_range(0..docs.len());
            let (ListWithHistory(doc, ops), ref_doc) = &mut docs[doc_idx];

            // Agent ID 0 on each doc is what we want.
            // let seq = doc.client_data[0].get_next_seq();
            // let time = doc.get_frontier::<Vec<RemoteId>>();
            let time = doc.get_next_time();
            let op = make_random_change(doc, None, 0, &mut rng);
            op.check();

            ref_doc.apply_local_txn(0, (&op).into());
            // dbg!(doc_idx, &op, &doc.text_content);

            assert_eq!(doc, ref_doc);

            ops.push(KVPair(time, op));
        }

        // dbg!(&docs);

        // Then merge 2 documents at random
        let a_idx = rng.gen_range(0..docs.len());
        let b_idx = rng.gen_range(0..docs.len());

        if a_idx != b_idx {
            // println!("Merging {} and {}", a_idx, b_idx);
            // Oh god this is awful. I can't take mutable references to two array items.
            let (a_idx, b_idx) = if a_idx < b_idx { (a_idx, b_idx) } else { (b_idx, a_idx) };
            // a<b.
            let (start, end) = docs[..].split_at_mut(b_idx);
            let a = &mut start[a_idx];
            let b = &mut end[0];

            // dbg!(&a.content_tree, &b.content_tree);

            // Our frontier should contain everything in the document.
            // TODO: Turn this back on. Its free, just annoying.
            // let frontier = a.0.get_frontier_as_localtime().to_vec();
            // let mid_order = a.0.get_next_time();
            // if mid_order > 0 {
            //     for _k in 0..10 {
            //         let order = rng.gen_range(0..mid_order);
            //         assert!(a.0.branch_contains_order(&frontier, order));
            //     }
            // }


            // a.1.debug_print_segments();
            // println!("{} -> {}", b_idx, a_idx);
            b.1.replicate_into(&mut a.1);
            // println!("a.1");
            // a.1.debug_print_segments();
            // dbg!(&a.1.double_deletes);

            b.0.replicate_into(&mut a.0);
            a.0.0.check(false);

            // dbg!(a.0.0.get_all_txns::<Vec<_>>());
            // dbg!(a.1.get_all_txns::<Vec<_>>());
            // dbg!(b.0.0.get_all_txns::<Vec<_>>());

            // dbg!(a.0.0.range_tree.iter().collect::<Vec<_>>());
            // dbg!(a.1.range_tree.iter().collect::<Vec<_>>());
            assert_eq!(a.0.0, a.1);


            // println!("{} -> {}", a_idx, b_idx);
            a.0.replicate_into(&mut b.0);
            a.1.replicate_into(&mut b.1);
            b.0.0.check(false);

            // dbg!(&b.0.0.range_tree);
            // dbg!(&b.1.range_tree);
            assert_eq!(b.0.0, b.1);



            // println!("--- pos ---");
            // a.0.0.debug_print_segments();
            // println!("--- crdt ---");
            // a.1.debug_print_segments();


            // But our old frontier doesn't contain any of the new items.
            // if a.0.get_next_time() > mid_order {
            //     for _k in 0..10 {
            //         let order = rng.gen_range(mid_order..a.0.get_next_time());
            //         assert!(!a.0.branch_contains_order(&frontier, order));
            //     }
            // }

            if a.0.0 != b.0.0 {
                println!("Docs {} and {} after {} iterations:", a_idx, b_idx, _i);
                dbg!(&a.0.0.text_content);
                dbg!(&b.0.0.text_content);

                // dbg!(&a);
                // dbg!(&b);
                panic!("Documents do not match");
            }
        }

        for doc in &docs {
            doc.0.0.check(false);
        }
    }

    for doc in &docs {
        doc.0.0.check(true);
    }
}

#[test]
fn positional_fuzzer_once() {
    run_fuzzer_iteration(12);
}


#[test]
#[ignore]
fn positional_fuzzer_forever() {
    for i in 0.. {
        println!("Root iteration {}", i);
        run_fuzzer_iteration(i);
    }
}