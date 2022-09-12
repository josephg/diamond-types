use rand::prelude::SmallRng;
use jumprope::JumpRope;
use rand::Rng;
use smallvec::smallvec;
use rle::MergeableIterator;
use rle::zip::{rle_zip, rle_zip3};
use crate::{AgentId, Time};
use crate::list::{ListBranch, ListCRDT, ListOpLog};

const USE_UNICODE: bool = true;

const UCHARS: [char; 23] = [
    'a', 'b', 'c', '1', '2', '3', ' ', '\n', // ASCII
    '©', '¥', '½', // The Latin-1 suppliment (U+80 - U+ff)
    'Ύ', 'Δ', 'δ', 'Ϡ', // Greek (U+0370 - U+03FF)
    '←', '↯', '↻', '⇈', // Arrows (U+2190 – U+21FF)
    '𐆐', '𐆔', '𐆘', '𐆚', // Ancient roman symbols (U+10190 – U+101CF)
];

pub(crate) fn random_str(len: usize, rng: &mut SmallRng) -> String {
    let mut str = String::new();
    let alphabet: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_".chars().collect();

    for _ in 0..len {
        let charset = if USE_UNICODE { &UCHARS[..] } else { &alphabet };
        str.push(charset[rng.gen_range(0..charset.len())]);
            // str.push(UCHARS[rng.gen_range(0..UCHARS.len())]);
            // str.push(alphabet[rng.gen_range(0..alphabet.len())]);
    }
    str
}

pub(crate) fn make_random_change_raw(oplog: &mut ListOpLog, branch: &ListBranch, mut rope: Option<&mut JumpRope>, agent: AgentId, rng: &mut SmallRng) -> Time {
    let doc_len = branch.len();
    let insert_weight = if doc_len < 100 { 0.55 } else { 0.45 };
    let v = if doc_len == 0 || rng.gen_bool(insert_weight) {
        // Insert something.
        let pos = rng.gen_range(0..=doc_len);
        let len: usize = rng.gen_range(1..3); // Ideally skew toward smaller inserts.
        let content = random_str(len as usize, rng);
        let fwd = len == 1 || rng.gen_bool(0.5);
        // eprintln!("Inserting '{}' at position {} (fwd: {})", content, pos, fwd);

        if let Some(rope) = rope {
            rope.insert(pos, content.as_str());
        }

        if fwd {
            oplog.add_insert_at(agent, &branch.version, pos, &content)
        } else {
            let mut frontier = branch.version.clone();
            for c in content.chars().rev() {
                let mut buf = [0u8; 8]; // Not sure what the biggest utf8 char is but eh.
                let str = c.encode_utf8(&mut buf);
                let v = oplog.add_insert_at(agent, &frontier, pos, str);
                frontier = smallvec![v];
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
            oplog.add_operations_at(agent, &branch.version, &[op])
        } else {
            // Backspace each character individually.
            let mut frontier = branch.version.clone(); // Not the most elegant but eh.
            for i in del_loc.rev() {
                // println!("Delete {}", pos + i);
                let op = branch.make_delete_op(i .. i + 1);
                let v = oplog.add_operations_at(agent, &frontier, &[op]);
                frontier = smallvec![v];
            }
            frontier[0]
        }
        // doc.local_delete(agent, pos, span)
    };
    // dbg!(&doc.markers);
    oplog.dbg_check(false);
    v
}

pub(crate) fn make_random_change(doc: &mut ListCRDT, rope: Option<&mut JumpRope>, agent: AgentId, rng: &mut SmallRng) {
    let v = make_random_change_raw(&mut doc.oplog, &doc.branch, rope, agent, rng);
    doc.branch.merge(&doc.oplog, &[v]);
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

impl ListOpLog {
    /// TODO: Consider removing this
    #[allow(unused)]
    pub fn dbg_print_all(&self) {
        // self.iter_history()
        // self.operations.iter()
        for x in rle_zip(
            self.iter_history(),
            // self.operations.iter().map(|p| p.1.clone()) // Only the ops.
            self.iter()
        ) {
            println!("{:?}", x);
        }
    }

    #[allow(unused)]
    pub(crate) fn dbg_print_ops(&self) {
        for (time, op) in rle_zip(
            self.iter_history().map(|h| h.span).merge_spans(),
            self.iter()
        ) {
            println!("{:?} Op: {:?}", time, op);
        }
    }

    #[allow(unused)]
    pub(crate) fn dbg_print_assignments_and_ops(&self) {
        for (time, map, op) in rle_zip3(
            self.iter_history().map(|h| h.span).merge_spans(),
            self.iter_mappings(),
            self.iter()
        ) {
            println!("{:?} M: {:?} Op: {:?}", time, map, op);
        }
    }
}
