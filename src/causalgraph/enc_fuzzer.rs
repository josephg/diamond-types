use rand::prelude::*;
use crate::{CausalGraph, Frontier};
use crate::encoding::bufparser::BufParser;
use crate::encoding::cg_entry::{read_cg_entry_into_cg, write_cg_entry_iter};
use crate::encoding::map::{ReadMap, WriteMap};
use crate::list_fuzzer_tools::{choose_2, fuzz_multithreaded};

fn merge_changes(from_cg: &CausalGraph, into_cg: &mut CausalGraph, from_root: bool) {
    let from_frontier = if from_root {
        Frontier::root()
    } else {
        let into_summary = into_cg.agent_assignment.summarize_versions_flat();
        // dbg!(&a_summary);
        let (frontier, _remainder) = from_cg.intersect_with_flat_summary(&into_summary, &[]);
        frontier
    };

    // Serialize the changes from from_frontier.
    let msg = from_cg.serialize_changes_since2(from_frontier.as_ref());

    // And merge them in!
    into_cg.merge_serialized_changes2(&msg).unwrap();
}

/// This fuzzer variant creates linear timelines from 3 different user agents. We still end up with
/// a complex entwined graph, but `(agent, x)` always directly precedes `(agent, x+1)`.
fn fuzz_cg_flat(seed: u64, verbose: bool) {
    let mut rng = SmallRng::seed_from_u64(seed);

    let mut cgs = [CausalGraph::new(), CausalGraph::new(), CausalGraph::new()];
    let agents = ["a", "b", "c"];

    for c in &mut cgs {
        for a in &agents {
            c.get_or_create_agent_id_from_str(*a);
        }
    }

    for _i in 0..50 {
        if verbose { println!("\n\ni {}", _i); }

        // Generate some operations
        for _j in 0..3 {
            // for _j in 0..5 {
            let idx = rng.gen_range(0..cgs.len());
            let cg = &mut cgs[idx];

            let agent_id = cg.get_or_create_agent_id_from_str(agents[idx]);
            let num = rng.gen_range(1..10);
            cg.assign_local_op(agent_id, num);
        }

        // And merge 2 random causal graphs
        let (_a_idx, a, _b_idx, b) = choose_2(&mut cgs, &mut rng);

        merge_changes(a, b, rng.gen_bool(0.04));
        // println!("--\n\n---");
        merge_changes(b, a, rng.gen_bool(0.04));

        assert_eq!(a, b);
    }

    for cg in cgs {
        cg.dbg_check(true);
    }
}

#[test]
fn fuzz_cg_once() {
    fuzz_cg_flat(123, true);
}

#[test]
fn fuzz_cg() {
    for k in 0..70 {
        // println!("{k}...");
        fuzz_cg_flat(k, false);
    }
}

#[test]
#[ignore]
fn fuzz_cg_forever() {
    fuzz_multithreaded(u64::MAX, |seed| {
        if seed % 1000 == 0 {
            println!("Iteration {}", seed);
        }
        fuzz_cg_flat(seed, false);
    })
}