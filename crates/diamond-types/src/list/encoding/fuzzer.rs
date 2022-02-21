use rand::prelude::*;
use crate::list::{ListCRDT, OpLog};
use crate::list::encoding::EncodeOptions;
use crate::list::fuzzer_tools::make_random_change;

// This fuzzer will make an oplog, spam it with random changes from a single peer. Then save & load
// it back to make sure the result doesn't change.
fn fuzz_encode_decode_once(seed: u64) {
    let mut doc = ListCRDT::new();
    doc.get_or_create_agent_id("a"); // 0
    doc.get_or_create_agent_id("b"); // 1
    doc.get_or_create_agent_id("c"); // 2

    let mut rng = SmallRng::seed_from_u64(seed);

    for _i in 0..300 {
        // println!("\n\nIteration {i}");
        let agent = rng.gen_range(0..3);
        for _k in 0..rng.gen_range(1..=3) {
            make_random_change(&mut doc, None, agent, &mut rng);
        }

        let bytes = doc.ops.encode(EncodeOptions {
            user_data: None,
            store_inserted_content: true,
            store_deleted_content: true,
            verbose: false
        });

        let decoded = OpLog::load_from(&bytes).unwrap();
        if doc.ops != decoded {
            // eprintln!("Original doc {:#?}", &doc.ops);
            // eprintln!("Loaded doc {:#?}", &decoded);
            panic!("Docs do not match!");
        }
        // assert_eq!(decoded, doc.ops);
    }
}

#[test]
fn encode_decode_fuzz_once() {
    fuzz_encode_decode_once(2);
}

#[test]
#[ignore]
fn encode_decode_fuzz_forever() {
    for seed in 0.. {
        if seed % 10 == 0 { println!("seed {seed}"); }
        fuzz_encode_decode_once(seed);
    }
}