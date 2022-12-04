use rand::prelude::SmallRng;
use jumprope::JumpRope;
use rand::Rng;
use smallvec::smallvec;
use rle::MergeableIterator;
use rle::zip::{rle_zip, rle_zip3};
use crate::{AgentId, LV};
use crate::list::{ListBranch, ListCRDT, ListOpLog};
use crate::list_fuzzer_tools::random_str;

fn old_make_random_change_raw(oplog: &mut ListOpLog, branch: &ListBranch, mut rope: Option<&mut JumpRope>, agent: AgentId, rng: &mut SmallRng) -> LV {
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
            oplog.add_operations_at(agent, branch.version.as_ref(), &[op])
        } else {
            // Backspace each character individually.
            let mut frontier = branch.version.clone(); // Not the most elegant but eh.
            for i in del_loc.rev() {
                // println!("Delete {}", pos + i);
                let op = branch.make_delete_op(i .. i + 1);
                let v = oplog.add_operations_at(agent, frontier.as_ref(), &[op]);
                frontier.replace_with_1(v);
            }
            frontier[0]
        }
        // doc.local_delete(agent, pos, span)
    };
    // dbg!(&doc.markers);
    oplog.dbg_check(false);
    v
}

pub(crate) fn old_make_random_change(doc: &mut ListCRDT, rope: Option<&mut JumpRope>, agent: AgentId, rng: &mut SmallRng) {
    let v = old_make_random_change_raw(&mut doc.oplog, &doc.branch, rope, agent, rng);
    doc.branch.merge(&doc.oplog, &[v]);
    // doc.check(true);
    // doc.check(false);
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
            self.iter_remote_mappings(),
            self.iter()
        ) {
            println!("{:?} M: {:?} Op: {:?}", time, map, op);
        }
    }
}
