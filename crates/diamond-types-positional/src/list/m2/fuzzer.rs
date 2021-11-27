use jumprope::JumpRope;
use rand::prelude::*;
use crate::AgentId;
use crate::list::{Branch, ListCRDT, OpSet, Time};
use crate::list::frontier::frontier_eq;

pub fn random_str(len: usize, rng: &mut SmallRng) -> String {
    let mut str = String::new();
    let alphabet: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_".chars().collect();
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

        // I'm using this rather than push_delete to preserve the deleted content.
        let op = branch.make_delete_op(pos, span);
        opset.push(agent, &branch.frontier, &[op])
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

fn merge_fuzz(seed: u64, verbose: bool) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut opset = OpSet::new();
    let mut branches = [Branch::new(), Branch::new(), Branch::new()];

    // Each document will have a different local agent ID. I'm cheating here - just making agent
    // 0 for all of them.
    for i in 0..branches.len() {
        opset.get_or_create_agent_id(format!("agent {}", i).as_str());
    }

    for _i in 0..300 {
        if verbose { println!("\n\ni {}", _i); }
        // Generate some operations
        for _j in 0..2 {
        // for _j in 0..5 {
            let idx = rng.gen_range(0..branches.len());
            let branch = &mut branches[idx];

            let v = make_random_change_raw(&mut opset, branch, None, idx as AgentId, &mut rng);
            // dbg!(opset.iter_range((v..v+1).into()).next().unwrap());

            branch.merge(&opset, &[v]);
            // make_random_change(doc, None, 0, &mut rng);
            // println!("branch {} content '{}'", idx, &branch.content);
        }

        // Then merge 2 branches at random
        let a_idx = rng.gen_range(0..branches.len());
        let b_idx = rng.gen_range(0..branches.len());

        if a_idx != b_idx {
            // Oh god this is awful. I can't take mutable references to two array items.
            let (a_idx, b_idx) = if a_idx < b_idx { (a_idx, b_idx) } else { (b_idx, a_idx) };
            // a<b.
            let (start, end) = branches[..].split_at_mut(b_idx);
            let a = &mut start[a_idx];
            let b = &mut end[0];

            if verbose {
                println!("\n\n-----------");
                println!("a content '{}'", a.content);
                println!("b content '{}'", b.content);
                println!("Merging a({}) {:?} and b({}) {:?}", a_idx, &a.frontier, b_idx, &b.frontier);
                println!();
            }

            // if _i == 253 {
            //     dbg!(&opset.client_with_localtime);
            // }

            // dbg!(&opset);

            if verbose { println!("Merge b to a: {:?} -> {:?}", &b.frontier, &a.frontier); }
            a.merge2(&opset, &b.frontier, false);
            if verbose {
                println!("-> a content '{}'\n", a.content);
            }

            if verbose { println!("Merge a to b: {:?} -> {:?}", &a.frontier, &b.frontier); }
            b.merge2(&opset, &a.frontier, false);
            if verbose {
                println!("-> b content '{}'", b.content);
            }


            // Our frontier should contain everything in the document.

            // a.check(false);
            // b.check(false);

            if a != b {
                println!("Docs {} and {} after {} iterations:", a_idx, b_idx, _i);
                dbg!(&a);
                dbg!(&b);
                panic!("Documents do not match");
            } else {
                if verbose {
                    println!("Merge {:?} -> '{}'", &a.frontier, a.content);
                }
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
    merge_fuzz(2000 + 32106, true);
}

#[test]
#[ignore]
fn fuzz_many() {
    for k in 0.. {
        // println!("\n\n*** Iteration {} ***\n", k);
        if k % 1000 == 0 {
            println!("Iteration {}", k);
        }
        merge_fuzz(1000000 + k, false);
    }
}