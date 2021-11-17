use jumprope::JumpRope;
use rand::prelude::*;
use crate::AgentId;
use crate::list::{Branch, ListCRDT, OpSet, Time};

pub fn random_str(len: usize, rng: &mut SmallRng) -> String {
    let mut str = String::new();
    let alphabet: Vec<char> = "abcdefghijklmnop_".chars().collect();
    for _ in 0..len {
        str.push(alphabet[rng.gen_range(0..alphabet.len())]);
    }
    str
}

fn make_random_change_raw(opset: &mut OpSet, branch: &Branch, rope: Option<&mut JumpRope>, agent: AgentId, rng: &mut SmallRng) -> Time {
    let doc_len = branch.len();
    let insert_weight = if doc_len < 100 { 0.55 } else { 0.45 };
    let v = if doc_len == 0 || rng.gen_bool(insert_weight) {
        // Insert something.
        let pos = rng.gen_range(0..=doc_len);
        let len: usize = rng.gen_range(1..2); // Ideally skew toward smaller inserts.
        // let len: usize = rng.gen_range(1..10); // Ideally skew toward smaller inserts.

        let content = random_str(len as usize, rng);
        // eprintln!("Inserting '{}' at position {}", content, pos);
        if let Some(rope) = rope {
            rope.insert(pos, content.as_str());
        }
        opset.push_insert(agent, &branch.frontier, pos, &content)
    } else {
        // Delete something
        let pos = rng.gen_range(0..doc_len);
        // println!("range {}", u32::min(10, doc_len - pos));
        let span = rng.gen_range(1..=usize::min(10, doc_len - pos));
        // dbg!(&state.marker_tree, pos, len);
        // eprintln!("deleting {} at position {}", span, pos);
        if let Some(rope) = rope {
            rope.remove(pos..pos + span);
        }
        opset.push_delete(agent, &branch.frontier, pos, span)
        // doc.local_delete(agent, pos, span)
    };
    // dbg!(&doc.markers);
    opset.check(false);
    v
}

fn make_random_change(doc: &mut ListCRDT, rope: Option<&mut JumpRope>, agent: AgentId, rng: &mut SmallRng) {
    let v = make_random_change_raw(&mut doc.ops, &doc.branch, rope, agent, rng);
    doc.branch.merge(&doc.ops, &[v]);
    // doc.check(true);
    // doc.check(false);
}

#[test]
fn random_single_document() {
    let mut rng = SmallRng::seed_from_u64(3);
    let mut doc = ListCRDT::new();

    let agent = doc.get_or_create_agent_id("seph");
    let mut expected_content = JumpRope::new();

    for _i in 0..1000 {
        // eprintln!("i {}", _i);
        // doc.debug_print_stuff();
        make_random_change(&mut doc, Some(&mut expected_content), agent, &mut rng);
        assert_eq!(doc.branch.content, expected_content);
    }

    doc.check(true);
}

fn merge_fuzz(seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut opset = OpSet::new();
    let mut branches = [Branch::new(), Branch::new(), Branch::new()];

    // Each document will have a different local agent ID. I'm cheating here - just making agent
    // 0 for all of them.
    for i in 0..branches.len() {
        opset.get_or_create_agent_id(format!("agent {}", i).as_str());
    }

    for _i in 0..300 {
        println!("i {}", _i);
        // Generate some operations
        for _j in 0..5 {
            let doc_idx = rng.gen_range(0..branches.len());
            let branch = &mut branches[doc_idx];

            let v = make_random_change_raw(&mut opset, branch, None, doc_idx as AgentId, &mut rng);
            branch.merge(&opset, &[v]);
            // make_random_change(doc, None, 0, &mut rng);
        }

        // Then merge 2 branches at random
        let a_idx = rng.gen_range(0..branches.len());
        let b_idx = rng.gen_range(0..branches.len());

        if a_idx != b_idx {
            // println!("Merging {} and {}", a_idx, b_idx);
            // Oh god this is awful. I can't take mutable references to two array items.
            let (a_idx, b_idx) = if a_idx < b_idx { (a_idx, b_idx) } else { (b_idx, a_idx) };
            // a<b.
            let (start, end) = branches[..].split_at_mut(b_idx);
            let a = &mut start[a_idx];
            let b = &mut end[0];

            // dbg!(&a.text_content, &b.text_content);
            // dbg!(&a.content_tree, &b.content_tree);

            // println!("{} -> {}", a_idx, b_idx);
            a.merge(&opset, &b.frontier);
            // println!("{} -> {}", b_idx, a_idx);
            b.merge(&opset, &a.frontier);


            // Our frontier should contain everything in the document.

            // a.check(false);
            // b.check(false);

            if a != b {
                println!("Docs {} and {} after {} iterations:", a_idx, b_idx, _i);
                // dbg!(&a);
                // dbg!(&b);
                panic!("Documents do not match");
            }
        }

        // for doc in &branches {
        //     doc.check(false);
        // }
    }

    // for doc in &branches {
    //     doc.check(true);
    // }
}

#[test]
#[ignore]
fn fuzz_once() {
    merge_fuzz(0);
}