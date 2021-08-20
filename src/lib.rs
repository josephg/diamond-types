#![allow(dead_code)] // TODO: turn this off and clean up before releasing.

pub use alloc::*;
pub use common::{LocalOp, AgentId};

pub mod list;

mod common;
mod range_tree;
mod split_list;
mod splitable_span;
mod alloc;
mod order;
mod rle;
mod unicount;
mod merge_iter;

#[cfg(test)]
mod tests {
    // As per smartstring's documentation.
    #[test]
    fn validate_smartstring() {
        smartstring::validate();
    }
}

#[cfg(test)]
pub mod fuzz_helpers {
    use rand::prelude::SmallRng;
    use rand::Rng;
    use crate::list::ListCRDT;
    use ropey::Rope;
    use crate::{AgentId, LocalOp};

    pub fn random_str(len: usize, rng: &mut SmallRng) -> String {
        let mut str = String::new();
        let alphabet: Vec<char> = "abcdefghijklmnop_".chars().collect();
        for _ in 0..len {
            str.push(alphabet[rng.gen_range(0..alphabet.len())]);
        }
        str
    }

    pub fn make_random_change(doc: &mut ListCRDT, rope: Option<&mut Rope>, agent: AgentId, rng: &mut SmallRng) -> LocalOp {
        let doc_len = doc.len();
        let insert_weight = if doc_len < 100 { 0.6 } else { 0.4 };
        let op = if doc_len == 0 || rng.gen_bool(insert_weight) {
            // Insert something.
            let pos = rng.gen_range(0..=doc_len);
            let len: usize = rng.gen_range(1..2); // Ideally skew toward smaller inserts.
            // let len: usize = rng.gen_range(1..10); // Ideally skew toward smaller inserts.

            let content = random_str(len as usize, rng);
            // println!("Inserting '{}' at position {}", content, pos);
            if let Some(rope) = rope {
                rope.insert(pos, content.as_str());
            }
            LocalOp {
                pos,
                ins_content: content.into(),
                del_span: 0
            }
        } else {
            // Delete something
            let pos = rng.gen_range(0..doc_len);
            // println!("range {}", u32::min(10, doc_len - pos));
            let span = rng.gen_range(1..=usize::min(10, doc_len - pos));
            // dbg!(&state.marker_tree, pos, len);
            // println!("deleting {} at position {}", span, pos);
            if let Some(rope) = rope {
                rope.remove(pos..pos + span);
            }
            LocalOp {
                pos,
                ins_content: "".into(),
                del_span: span
            }
        };
        doc.apply_local_txn(agent, std::slice::from_ref(&op));
        // dbg!(&doc.markers);
        doc.check(false);
        op
    }
}