//! This file contains a fuzzer-style generator of random causal graphs used to test various
//! CG functions.

use std::path::Path;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use crate::causalgraph::graph::Graph;
use crate::{AgentId, CausalGraph, DTRange, Frontier};
use crate::list_fuzzer_tools::choose_2;

pub(crate) fn with_random_cgs<F: FnMut((usize, usize), &CausalGraph, &[Frontier])>(seed: u64, iterations: (usize, usize), mut f: F) {
    for outer in 0..iterations.0 {
        let seed_here = seed + outer as u64;
        let mut rng = SmallRng::seed_from_u64(seed_here);
        // println!("seed {seed_here}");
        let mut frontiers = [Frontier::root(), Frontier::root(), Frontier::root()];
        let mut cg = CausalGraph::new();

        let agents = ["a", "b", "c"];
        // Agent IDs 0, 1 and 2.
        for a in agents { cg.get_or_create_agent_id_from_str(a); }

        // for _i in 0..300 {
        for i in 0..iterations.1 {
            // Generate some "operations" from the peers.
            for _j in 0..2 {
                let idx = rng.gen_range(0..frontiers.len());
                let frontier = &mut frontiers[idx];

                let first_change = cg.len();
                // let span: DTRange = (first_change..first_change + rng.gen_range(1..5)).into();
                let span: DTRange = (first_change..first_change + 1).into();
                cg.assign_span(idx as AgentId, frontier.as_ref(), span);

                frontier.replace_with_1(span.last());
            }

            // Now randomly merge some frontiers into other frontiers.
            for _j in 0..5 {
                let (_a_idx, a, _b_idx, b) = choose_2(&mut frontiers, &mut rng);

                *a = cg.graph.find_dominators_2(a.as_ref(), b.as_ref());
            }

            f((outer, i), &cg, &frontiers);
        }
    }
}

// This generates some graphs to the graphs/ folder.
#[test]
#[ignore]
#[cfg(feature = "dot_export")]
fn generate_some_graphs() {
    with_random_cgs(123, (1, 10), |(_, i), cg, _frontiers| {
        // dbg!(&cg.graph);
        cg.generate_dot_svg(Path::new(&format!("graphs/{i}.svg")));
    });
}