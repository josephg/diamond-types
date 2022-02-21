use rand::prelude::*;
use crate::list::fuzzer_tools::make_random_change;
use crate::list::{fuzzer_tools, ListCRDT};

fn merge_fuzz(seed: u64, verbose: bool) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut docs = [ListCRDT::new(), ListCRDT::new(), ListCRDT::new()];

    for i in 0..docs.len() {
        docs[i].get_or_create_agent_id(format!("agent {}", i).as_str());
    }

    for _i in 0..300 {
        if verbose { println!("\n\ni {}", _i); }

        // Generate some operations
        for _j in 0..2 {
            let idx = rng.gen_range(0..docs.len());

            // This should + does also work if we set idx=0 and use the same agent for all changes.
            make_random_change(&mut docs[idx], None, 0, &mut rng);
        }

        let (_a_idx, a, _b_idx, b) = fuzzer_tools::choose_2(&mut docs, &mut rng);

        a.ops.merge_entries_from(&b.ops);
        b.ops.merge_entries_from(&a.ops);

        dbg!(&a.ops, &b.ops);
        assert_eq!(a.ops, b.ops);

        a.branch.merge(&a.ops, &a.ops.frontier);
        b.branch.merge(&b.ops, &b.ops.frontier);
        assert_eq!(a.branch, b.branch);
    }
}

#[test]
#[ignore]
fn merge_fuzz_once() {
    merge_fuzz(123, true);
}