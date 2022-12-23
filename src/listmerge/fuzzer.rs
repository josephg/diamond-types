use jumprope::JumpRope;
use rand::prelude::*;
use crate::{AgentId, DTRange};
use crate::list::operation::TextOperation;
use crate::list_fuzzer_tools::{choose_2, fuzz_multithreaded, make_random_change};
use crate::listmerge::simple_oplog::{SimpleBranch, SimpleOpLog};

#[test]
fn random_single_document() {
    let mut rng = SmallRng::seed_from_u64(10);
    let mut oplog = SimpleOpLog::new();
    let mut branch = SimpleBranch::new();

    let mut expected_content = JumpRope::new();

    for _i in 0..1000 {
        // eprintln!("i {}", _i);

        if rng.gen_bool(0.2) {
            oplog.goop(10);
            branch.version = oplog.cg.version.clone();
        }

        make_random_change(&mut oplog, &mut branch, Some(&mut expected_content), "seph", &mut rng);

        oplog.merge_all(&mut branch);
        assert_eq!(branch.content, expected_content);
    }

    assert_eq!(expected_content, oplog.to_string());
    oplog.dbg_check(true);
}

fn merge_fuzz(seed: u64, verbose: bool) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut oplog = SimpleOpLog::new();
    let mut branches = [SimpleBranch::new(), SimpleBranch::new(), SimpleBranch::new()];

    let agents = ["a", "b", "c"];

    for _i in 0..300 {
        if verbose { println!("\n\ni {}", _i); }
        // Generate some operations
        for _j in 0..2 {
        // for _j in 0..5 {
            let idx = rng.gen_range(0..branches.len());
            let branch = &mut branches[idx];

            if rng.gen_bool(0.1) {
                // Add some rubbish to simulate other documents being edited
                oplog.goop(10);
            }

            // This should + does also work if we set idx=0 and use the same agent for all changes.
            let v = make_random_change(&mut oplog, branch, None, agents[idx], &mut rng);
            // dbg!(opset.iter_range((v..v+1).into()).next().unwrap());

            oplog.merge_to_version(branch, &[v]);
            // println!("branch {} content '{}'", idx, &branch.content);
        }

        // Then merge 2 branches at random
        // TODO: Rewrite this to use choose_2.
        let (a_idx, a, b_idx, b) = choose_2(&mut branches, &mut rng);

        if verbose {
            println!("\n\n-----------");
            println!("a content '{}'", a.content);
            println!("b content '{}'", b.content);
            println!("Merging a({}) {:?} and b({}) {:?}", a_idx, &a.version, b_idx, &b.version);
            println!();
        }

        // if _i == 253 {
        //     dbg!(&opset.client_with_localtime);
        // }

        // dbg!(&opset);

        if verbose { println!("Merge b to a: {:?} -> {:?}", &b.version, &a.version); }
        oplog.merge_to_version(a, b.version.as_ref());
        if verbose {
            println!("-> a content '{}'\n", a.content);
        }

        if verbose { println!("Merge a to b: {:?} -> {:?}", &a.version, &b.version); }
        oplog.merge_to_version(b, a.version.as_ref());
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
                println!("Merge {:?} -> '{}'", &a.version, a.content);
            }
        }

        if _i % 50 == 0 {
            // Every little while, merge everything. This has 2 purposes:
            // 1. It stops the fuzzer being n^2. (Its really unfortunate we need this)
            // And 2. It makes sure n-way merging also works correctly.
            for branch in branches.iter_mut() {
                oplog.merge_all(branch);
            }
            for w in branches.windows(2) {
                assert_eq!(w[0].content, w[1].content);
            }
        }
    }

    // if rng.gen_bool(0.0001) {
    //     panic!("blerp!");
    // }

    // for doc in &branches {
    //     doc.check(true);
    // }
}

// // Included in standard smoke tests.
#[test]
fn fuzz_once_quietly_new() {
    merge_fuzz(0, false);
}

#[test]
#[ignore]
fn fuzz_dirty_benchmark() {
    for k in 0..100 {
        merge_fuzz(k, false);
    }
}

#[test]
#[ignore]
fn fuzz_once() {
    merge_fuzz(2000 + 32106, true);
}

#[test]
#[ignore]
fn fuzz_merge_st_forever() {
    for k in 0.. {
        // println!("\n\n*** Iteration {} ***\n", k);
        if k % 100 == 0 {
            println!("new Iteration {}", k);
        }
        merge_fuzz(1000000 + k, false);
    }
}

#[test]
#[ignore]
fn fuzz_merge_forever() {
    fuzz_multithreaded(u64::MAX, |seed| {
        if seed % 100 == 0 {
            println!("Iteration {}", seed);
        }
        merge_fuzz(seed, false);
    })
}