use rand::prelude::*;
use crate::AgentId;
use crate::list::{ListBranch, ListOpLog};
use crate::list::old_fuzzer_tools::old_make_random_change_raw;
use crate::list_fuzzer_tools::choose_2;

/// This is a simple generator for random test data used for conformance tests.
pub fn gen_oplog(seed: u64, steps: usize, use_unicode: bool) -> ListOpLog {
    let verbose = false;

    let mut rng = SmallRng::seed_from_u64(seed);
    let mut oplog = ListOpLog::new();
    let mut branches = [ListBranch::new(), ListBranch::new(), ListBranch::new()];

    let agents = ["a", "b", "c"];
    for a in agents {
        oplog.get_or_create_agent_id(a);
    }

    for _i in 0..steps {
        if verbose { println!("\n\ni {}", _i); }
        // Generate some operations
        for _j in 0..2 {
            // for _j in 0..5 {
            let idx = rng.gen_range(0..branches.len());
            let branch = &mut branches[idx];

            // This should + does also work if we set idx=0 and use the same agent for all changes.
            let v = old_make_random_change_raw(&mut oplog, branch, None, idx as AgentId, &mut rng, use_unicode);
            branch.merge(&oplog, &[v]);
            // println!("branch {} content '{}'", idx, &branch.content);
        }

        // Then merge 2 branches at random
        // TODO: Rewrite this to use choose_2.
        let (_a_idx, a, _b_idx, b) = choose_2(&mut branches, &mut rng);

        a.merge(&oplog, b.version.as_ref());
        b.merge(&oplog, a.version.as_ref());

        // Our frontier should contain everything in the document.
        debug_assert_eq!(a, b);

        if _i % 50 == 0 {
            // Every little while, merge everything. This has 2 purposes:
            // 1. It stops the fuzzer being n^2. (Its really unfortunate we need this)
            // And 2. It makes sure n-way merging also works correctly.
            for branch in branches.iter_mut() {
                branch.merge(&oplog, oplog.local_frontier_ref());
                // oplog.merge_all(branch);
            }
            for w in branches.windows(2) {
                assert_eq!(w[0].content, w[1].content);
            }
        }
    }

    // let result = oplog.checkout_tip().content.to_string();
    // (oplog, result)
    oplog
}

#[test]
fn generates_simple_oplog() {
    let _oplog = gen_oplog(123, 10);
    // dbg!(oplog);
}