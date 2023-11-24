use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;
use rand::prelude::SmallRng;
use jumprope::JumpRope;
use rand::Rng;
use smallvec::smallvec;
use rle::MergeableIterator;
use rle::zip::{rle_zip, rle_zip3};
use crate::{AgentId, LV};
use crate::listmerge::simple_oplog::*;

const USE_UNICODE: bool = true;

const UCHARS: [char; 23] = [
    'a', 'b', 'c', '1', '2', '3', ' ', '\n', // ASCII
    // 'd', 'e', 'f',
    // 'g', 'h', 'i', 'j',
    // 'k', 'l', 'm', 'n',
    // 'r', 'q', 'p', 'o',
    '©', '¥', '½', // The Latin-1 suppliment (U+80 - U+ff)
    'Ύ', 'Δ', 'δ', 'Ϡ', // Greek (U+0370 - U+03FF)
    '←', '↯', '↻', '⇈', // Arrows (U+2190 – U+21FF)
    '𐆐', '𐆔', '𐆘', '𐆚', // Ancient roman symbols (U+10190 – U+101CF)
];

pub(crate) fn random_str(len: usize, rng: &mut SmallRng, use_unicode: bool) -> String {
    let mut str = String::new();
    let alphabet: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_".chars().collect();

    for _ in 0..len {
        let charset = if use_unicode { &UCHARS[..] } else { &alphabet };
        str.push(charset[rng.gen_range(0..charset.len())]);
            // str.push(UCHARS[rng.gen_range(0..UCHARS.len())]);
            // str.push(alphabet[rng.gen_range(0..alphabet.len())]);
    }
    str
}

pub(crate) fn make_random_change(oplog: &mut SimpleOpLog, branch: &SimpleBranch, mut rope: Option<&mut JumpRope>, agent: &str, rng: &mut SmallRng) -> LV {
    let doc_len = branch.len();
    let insert_weight = if doc_len < 100 { 0.55 } else { 0.45 };

    let v = if doc_len == 0 || rng.gen_bool(insert_weight) {
        // Insert something.
        let pos = rng.gen_range(0..=doc_len);
        let len: usize = rng.gen_range(1..3); // Ideally skew toward smaller inserts.
        let content = random_str(len as usize, rng, true);
        let fwd = len == 1 || rng.gen_bool(0.5);
        // eprintln!("Inserting '{}' at position {} (fwd: {})", content, pos, fwd);

        if let Some(rope) = rope {
            rope.insert(pos, content.as_str());
        }

        if fwd {
            oplog.add_insert_at(agent, branch.version.as_ref(), pos, &content)
        } else {
            let mut frontier = branch.version.clone();
            for c in content.chars().rev() {
                let mut buf = [0u8; 8]; // Not sure what the biggest utf8 char is but eh.
                let str = c.encode_utf8(&mut buf);
                let v = oplog.add_insert_at(agent, frontier.as_ref(), pos, str);
                frontier.replace_with_1(v);
            }
            // dbg!(&oplog);
            frontier[0]
        }
    } else {
        // Delete something
        let pos = rng.gen_range(0..doc_len);
        // println!("range {}", u32::min(10, doc_len - pos));
        let span = rng.gen_range(1..=usize::min(10, doc_len - pos));
        // dbg!(&state.marker_tree, pos, len);
        // Sometimes deletes happen backwards - ie, via hitting backspace a bunch of times.
        let fwd = span == 1 || rng.gen_bool(0.5);

        let del_loc = pos..pos+span;

        // eprintln!("deleting {} at position {}", span, pos);
        if let Some(ref mut rope) = rope {
            rope.remove(del_loc.clone());
        }

        // I'm using this rather than push_delete to preserve the deleted content.
        if fwd {
            let op = branch.make_delete_op(del_loc);
            oplog.add_operation_at(agent, branch.version.as_ref(), op)
        } else {
            // Backspace each character individually.
            let mut frontier = branch.version.clone(); // Not the most elegant but eh.
            for i in del_loc.rev() {
                // println!("Delete {}", pos + i);
                let op = branch.make_delete_op(i .. i + 1);
                let v = oplog.add_operation_at(agent, frontier.as_ref(), op);
                frontier.replace_with_1(v);
            }
            frontier[0]
        }
        // doc.local_delete(agent, pos, span)
    };
    // dbg!(&doc.markers);
    oplog.dbg_check(false);
    // oplog.dbg_check(true);
    v
}

// pub(crate) fn make_random_change(doc: &mut ListCRDT, rope: Option<&mut JumpRope>, agent: AgentId, rng: &mut SmallRng) {
//     let v = make_random_change_raw(&mut doc.oplog, &doc.branch, rope, agent, rng);
//     doc.branch.merge(&doc.oplog, &[v]);
//     // doc.check(true);
//     // doc.check(false);
// }

pub(crate) fn choose_2<'a, T>(arr: &'a mut [T], rng: &mut SmallRng) -> (usize, &'a mut T, usize, &'a mut T) {
    loop {
        // Then merge 2 branches at random
        let a_idx = rng.gen_range(0..arr.len());
        let b_idx = rng.gen_range(0..arr.len());

        if a_idx != b_idx {
            // Oh god this is awful. I can't take mutable references to two array items.
            let (a_idx, b_idx) = if a_idx < b_idx { (a_idx, b_idx) } else { (b_idx, a_idx) };
            // a<b.
            let (start, end) = arr[..].split_at_mut(b_idx);
            let a = &mut start[a_idx];
            let b = &mut end[0];

            return (a_idx, a, b_idx, b);
        }
    }
}

/// A seed wrapper which prints out the seed on panic. This is handy because drop() is called during
/// unwinding so we can see the seed which crashed things.
pub(crate) struct Seed(pub u64);
impl Drop for Seed {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("*** CRASHED ON SEED {} ***", self.0);
            drop(std::io::stderr().flush());
        }
    }
}

pub(crate) fn fuzz_multithreaded<F: Fn(u64) + Send + Sync + Copy + Clone + 'static>(num_iter: u64, f: F) {
    let num_threads: usize = std::thread::available_parallelism().unwrap().into();
    let mut threads = vec![];
    let is_error = Arc::new(AtomicBool::new(false));

    for t in 0..num_threads {
        let is_error = is_error.clone();
        let is_error2 = is_error.clone();
        threads.push(std::thread::spawn(move || {
            let orig_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                // Signal to the other threads to stop iterating.
                is_error2.store(true, Ordering::Relaxed);
                orig_hook(info);
            }));

            let chunk_size = u64::MAX / (num_threads as u64);
            let seed_start = (chunk_size * t as u64) / 1000 * 1000;
            for seed_n in seed_start..seed_start.saturating_add(num_iter) {
                let seed = Seed(seed_n);
                f(seed.0);
                if is_error.load(Ordering::Relaxed) { break; }
            }
        }));
    }

    for t in threads {
        t.join().unwrap();
    }
}

// These methods are currently wrong by virtue of the operations not lining up with the causal
// into.version = self.cg.version.clone();
// impl SimpleOpLog {
//     /// TODO: Consider removing this
//     #[allow(unused)]
//     pub fn dbg_print_all(&self) {
//         // self.iter_history()
//         // self.operations.iter()
//         for x in rle_zip(
//             self.cg.parents.iter(),
//             // self.operations.iter().map(|p| p.1.clone()) // Only the ops.
//             self.info.iter()
//         ) {
//             println!("{:?}", x);
//         }
//     }
//
//     #[allow(unused)]
//     pub(crate) fn dbg_print_ops(&self) {
//         for (time, op) in rle_zip(
//             self.cg.parents.iter().map(|h| h.span).merge_spans(),
//             self.info.iter()
//         ) {
//             println!("{:?} Op: {:?}", time, op);
//         }
//     }
//
//     #[allow(unused)]
//     pub(crate) fn dbg_print_assignments_and_ops(&self) {
//         for (time, map, op) in rle_zip3(
//             self.cg.parents.iter().map(|h| h.span).merge_spans(),
//             self.cg.iter_remote_mappings(),
//             self.info.iter()
//         ) {
//             println!("{:?} M: {:?} Op: {:?}", time, map, op);
//         }
//     }
// }
