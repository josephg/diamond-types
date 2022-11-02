use rand::prelude::*;
use crate::list::fuzzer_tools::make_random_change;
use crate::list::{fuzzer_tools, ListCRDT};

fn oplog_merge_fuzz(seed: u64, verbose: bool) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut docs = [ListCRDT::new(), ListCRDT::new(), ListCRDT::new()];

    for i in 0..docs.len() {
        // docs[i].get_or_create_agent_id(format!("agent {}", i).as_str());
        for a in 0..docs.len() {
            docs[i].get_or_create_agent_id(format!("agent {}", a).as_str());
        }
    }

    for _i in 0..200 {
        if verbose { println!("\n\ni {}", _i); }

        // for (idx, d) in docs.iter().enumerate() {
        //     println!("doc {idx} length {}", d.ops.len());
        // }

        // Generate some operations
        for _j in 0..2 {
            let idx = rng.gen_range(0..docs.len());

            // This should + does also work if we set idx=0 and use the same agent for all changes.
            // make_random_change(&mut docs[idx], None, 0, &mut rng);
            make_random_change(&mut docs[idx], None, idx as _, &mut rng);
        }

        // for (idx, d) in docs.iter().enumerate() {
        //     println!("with changes {idx} length {}", d.ops.len());
        // }

        let (_a_idx, a, _b_idx, b) = fuzzer_tools::choose_2(&mut docs, &mut rng);

        // a.ops.dbg_print_assignments_and_ops();
        // println!("\n");
        // b.ops.dbg_print_assignments_and_ops();

        // dbg!((&a.ops, &b.ops));
        a.oplog.add_missing_operations_from(&b.oplog);
        // a.check(true);
        // println!("->c {_a_idx} length {}", a.ops.len());

        b.oplog.add_missing_operations_from(&a.oplog);
        // b.check(true);
        // println!("->c {_b_idx} length {}", b.ops.len());


        // dbg!((&a.ops, &b.ops));

        assert_eq!(a.oplog, b.oplog);

        a.branch.merge(&a.oplog, a.oplog.version.as_ref());
        b.branch.merge(&b.oplog, b.oplog.version.as_ref());
        assert_eq!(a.branch.content, b.branch.content);
    }

    for doc in &docs {
        doc.dbg_check(true);
    }
}

#[test]
fn oplog_merge_fuzz_once() {
    oplog_merge_fuzz(321, true);
}

#[test]
#[ignore]
fn oplog_merge_fuzz_forever() {
    for seed in 0.. {
        if seed % 10 == 0 { println!("seed {seed}"); }
        oplog_merge_fuzz(seed, false);
    }
}