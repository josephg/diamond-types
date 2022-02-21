use rand::prelude::SmallRng;
use jumprope::JumpRope;
use rand::Rng;
use rle::MergeableIterator;
use rle::zip::{rle_zip, rle_zip3};
use crate::AgentId;
use crate::list::{Branch, ListCRDT, OpLog, Time};

pub(crate) fn random_str(len: usize, rng: &mut SmallRng) -> String {
    let mut str = String::new();
    let alphabet: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_".chars().collect();
    for _ in 0..len {
        str.push(alphabet[rng.gen_range(0..alphabet.len())]);
    }
    str
}

pub(crate) fn make_random_change_raw(oplog: &mut OpLog, branch: &Branch, rope: Option<&mut JumpRope>, agent: AgentId, rng: &mut SmallRng) -> Time {
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
        oplog.push_insert_at(agent, &branch.frontier, pos, &content)
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
        oplog.push_at(agent, &branch.frontier, &[op])
        // doc.local_delete(agent, pos, span)
    };
    // dbg!(&doc.markers);
    oplog.check(false);
    v
}

pub(crate) fn make_random_change(doc: &mut ListCRDT, rope: Option<&mut JumpRope>, agent: AgentId, rng: &mut SmallRng) {
    let v = make_random_change_raw(&mut doc.ops, &doc.branch, rope, agent, rng);
    doc.branch.merge(&doc.ops, &[v]);
    // doc.check(true);
    // doc.check(false);
}

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

impl OpLog {
    #[allow(unused)]
    fn dbg_print_ops(&self) {
        for (time, op) in rle_zip(
            self.iter_history().map(|h| h.span).merge_spans(),
            self.iter()
        ) {
            println!("{:?} Op: {:?}", time, op);
        }
    }

    #[allow(unused)]
    fn dbg_print_assignments_and_ops(&self) {
        for (time, map, op) in rle_zip3(
            self.iter_history().map(|h| h.span).merge_spans(),
            self.iter_mappings(),
            self.iter()
        ) {
            println!("{:?} M: {:?} Op: {:?}", time, map, op);
        }
    }
}
