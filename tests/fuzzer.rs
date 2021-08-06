/// This fuzzer is a simple test which simulates 3 peers. Each iteration:
///
/// - We generate a set of changes from one or more peers
/// - We pick two peers and:
///   - Sync all changes to the document state
///   - Verify the two peers have identical document states afterwards
///
/// Any viable CRDT should be able to run this test indefinitely.
///
/// Run with:
/// RUST_BACKTRACE=1 cargo test fuzz_concurrency_forever -- --nocapture --ignored

use rand::prelude::*;
use diamond_types::list::ListCRDT;
use ropey::Rope;
use diamond_types::AgentId;

fn random_str(len: usize, rng: &mut SmallRng) -> String {
    let mut str = String::new();
    let alphabet: Vec<char> = "abcdefghijklmnop_".chars().collect();
    for _ in 0..len {
        str.push(alphabet[rng.gen_range(0..alphabet.len())]);
    }
    str
}

fn make_random_change(doc: &mut ListCRDT, rope: Option<&mut Rope>, agent: AgentId, rng: &mut SmallRng) {
    let doc_len = doc.len();
    let insert_weight = if doc_len < 100 { 0.55 } else { 0.45 };
    if doc_len == 0 || rng.gen_bool(insert_weight) {
        // Insert something.
        let pos = rng.gen_range(0..=doc_len);
        let len: usize = rng.gen_range(1..2); // Ideally skew toward smaller inserts.
        // let len: usize = rng.gen_range(1..10); // Ideally skew toward smaller inserts.

        let content = random_str(len as usize, rng);
        // println!("Inserting '{}' at position {}", content, pos);
        if let Some(rope) = rope {
            rope.insert(pos, content.as_str());
        }
        doc.local_insert(agent, pos, content.into())
    } else {
        // Delete something
        let pos = rng.gen_range(0..doc_len);
        // println!("range {}", u32::min(10, doc_len - pos));
        let span = rng.gen_range(1..=usize::min(10, doc_len - pos));
        // dbg!(&state.marker_tree, pos, len);
        // println!("deleting {} at position {}", span, pos);
        if let Some(rope) = rope {
            rope.remove(pos..pos + span);
        }
        doc.local_delete(agent, pos, span)
    }
    // dbg!(&doc.markers);
    doc.check(false);
}

#[test]
fn random_single_document() {
    let mut rng = SmallRng::seed_from_u64(7);
    let mut doc = ListCRDT::new();

    let agent = doc.get_or_create_agent_id("seph");
    let mut expected_content = Rope::new();

    for _i in 0..1000 {
        make_random_change(&mut doc, Some(&mut expected_content), agent, &mut rng);
        doc.dbg_assert_content_eq(&expected_content);
    }

    doc.check_all_changes_rle_merged();
}

#[test]
fn random_single_replicate() {
    let mut rng = SmallRng::seed_from_u64(20);
    let mut doc = ListCRDT::new();

    let agent = doc.get_or_create_agent_id("seph");
    let mut expected_content = Rope::new();

    // This takes a long time to do 1000 operations. (Like 3 seconds).
    for _i in 0..10 {
        for _ in 0..100 {
            make_random_change(&mut doc, Some(&mut expected_content), agent, &mut rng);
        }
        let mut doc_2 = ListCRDT::new();

        // dbg!(&doc.range_tree);
        doc.replicate_into(&mut doc_2);
        assert_eq!(doc, doc_2);
    }
}

fn run_fuzzer_iteration(seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);

    let mut docs = [ListCRDT::new(), ListCRDT::new(), ListCRDT::new()];

    // Each document will have a different local agent ID. I'm cheating here - just making agent
    // 0 for all of them.
    for (i, doc) in docs.iter_mut().enumerate() {
        doc.get_or_create_agent_id(format!("agent {}", i).as_str());
    }
    // for (_i, doc) in docs.iter_mut().enumerate() {
    //     for a in 0..3 {
    //         doc.get_or_create_agent_id(format!("agent {}", a).as_str());
    //     }
    // }

    for _i in 0..300 {
        // if _i % 1000 == 0 { println!("{}", _i); }
        // println!("\n\n{}", _i);

        // Generate some operations
        for _j in 0..5 {
            let doc_idx = rng.gen_range(0..docs.len());
            let doc = &mut docs[doc_idx];

            // println!("editing doc {}:", doc_idx);
            make_random_change(doc, None, 0, &mut rng);
            // make_random_change(doc, None, doc_idx as AgentId, &mut rng);
            // println!("doc {} -> '{}'", doc_idx, doc.text_content);
        }

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

            // dbg!(&a.text_content, &b.text_content);
            // dbg!(&a.range_tree, &b.range_tree);

            // println!("{} -> {}", a_idx, b_idx);
            a.replicate_into(b);
            // println!("{} -> {}", b_idx, a_idx);
            b.replicate_into(a);

            a.check(false);
            b.check(false);

            if a != b {
                println!("Docs {} and {} after {} iterations:", a_idx, b_idx, _i);
                // dbg!(&a);
                // dbg!(&b);
                panic!("Documents do not match");
            }
        }

        for doc in &docs {
            doc.check(true);
        }
    }
}

#[test]
fn fuzz_quick() {
    run_fuzzer_iteration(0);
}

#[test]
#[ignore]
fn fuzz_concurrency_forever() {
    for k in 0u64.. {
        println!("{}", k);

        run_fuzzer_iteration(k as u64);
    }
}

#[test]
fn fuzz_unapply_reapply() {
    let mut rng = SmallRng::seed_from_u64(20);
    let mut doc = ListCRDT::new();
    doc.get_or_create_agent_id("seph".into());

    for _k in 0..100 {
        // Apply some changes to the doc
        for _i in 0..10 {
            make_random_change(&mut doc, None, 0, &mut rng);
        }

        // Ok now pick a random point in time. We'll go back there, then go forward again. The
        // result should be the same as if we never left. Note if both unapply and reapply are
        // no-ops this will also succeed. This test is only viable thanks to other unit tests of the
        // module.
        let point: u32 = rng.gen_range(0..doc.num_ops());
        doc.check_timetravel(point);
    }
}