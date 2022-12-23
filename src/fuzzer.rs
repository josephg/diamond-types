// use rand::prelude::*;
// use crate::OpLog;
//
// fn merge_fuzz(seed: u64, verbose: bool) {
//     // A parachute so if the fuzzer crashes we can recover the seed.
//     let mut rng = SmallRng::seed_from_u64(seed);
//
//     let mut oplogs = [OpLog::new(), OpLog::new(), OpLog::new()];
//     let agents = ["a", "b", "c"];
//
//     for _i in 0..300 {
//         if verbose { println!("\n\ni {}", _i); }
//
//         // Generate some operations
//         for _j in 0..2 {
//             // for _j in 0..5 {
//             let idx = rng.gen_range(0..oplogs.len());
//             let oplog = &mut oplogs[idx];
//
//
//         }
//     }
// }