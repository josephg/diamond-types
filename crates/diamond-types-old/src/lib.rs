#![allow(dead_code)] // TODO: turn this off and clean up before releasing.
#![allow(unused)]

use crate::list::external_txn::RemoteId;

pub mod list;

mod common;
mod order;
mod rle;
mod unicount;
mod crdtspan;
mod rangeextra;

// TODO: Move this somewhere else.
pub fn root_id() -> RemoteId {
    RemoteId {
        agent: "ROOT".into(),
        seq: u32::MAX
    }
}

#[cfg(test)]
mod tests {

    // #[test]
    // fn print_sizes() {
    //     use smartstring::alias::{String as SmartString};
    //
    //     dbg!(std::mem::size_of::<SmartString>());
    //     dbg!(std::mem::size_of::<String>());
    // }
}

#[cfg(test)]
pub mod test_helpers {
    use rand::prelude::*;
    use ropey::Rope;

    use diamond_core_old::AgentId;

    use crate::list::{ListCRDT, PositionalOp};
    use rand::seq::index::sample;

    pub fn random_str(len: usize, rng: &mut SmallRng) -> String {
        let mut str = String::new();
        let alphabet: Vec<char> = "abcdefghijklmnop_".chars().collect();
        for _ in 0..len {
            str.push(alphabet[rng.gen_range(0..alphabet.len())]);
        }
        str
    }

    pub fn make_random_change(doc: &mut ListCRDT, rope: Option<&mut Rope>, agent: AgentId, rng: &mut SmallRng) -> PositionalOp {
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

            PositionalOp::new_insert(pos as u32, content)
            // doc.local_insert(agent, pos, content.as_str());
            // len as usize
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
            // doc.local_delete(agent, pos, span);
            // span
            PositionalOp::new_delete(pos as u32, span as u32)
        };

        doc.apply_local_txn(agent, (&op).into());

        // dbg!(&doc.markers);
        doc.check(false);
        op
    }

    /// A simple iterator over an infinite list of documents with random single-user edit histories
    #[derive(Debug)]
    pub struct RandomSingleDocIter(SmallRng, usize);

    impl RandomSingleDocIter {
        pub fn new(seed: u64, num_edits: usize) -> Self {
            Self(SmallRng::seed_from_u64(seed), num_edits)
        }
    }

    impl Iterator for RandomSingleDocIter {
        type Item = ListCRDT;

        fn next(&mut self) -> Option<Self::Item> {
            let mut doc = ListCRDT::new();
            let agent_0 = doc.get_or_create_agent_id("0");
            for _i in 0..self.1 {
                make_random_change(&mut doc, None, agent_0, &mut self.0);
            }

            Some(doc)
        }
    }

    /// This is a fuzz helper which constructs and permutes an endless stream of documents for
    /// testing. The documents are edited by complex multi user histories.
    ///
    /// I tried to write this as an iterator but failed to make something which borrow checks.
    ///
    /// This code is based on run_fuzzer_iteration.
    pub fn each_complex_random_doc_pair<F: FnMut(usize, &ListCRDT, &ListCRDT)>(seed: u64, iter: usize, mut f: F) -> [ListCRDT; 3] {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut docs = [ListCRDT::new(), ListCRDT::new(), ListCRDT::new()];

        // Each document will have a different local agent ID. I'm cheating here - just making agent
        // 0 for all of them.
        for (i, doc) in docs.iter_mut().enumerate() {
            doc.get_or_create_agent_id(format!("agent {}", i).as_str());
        }

        for _i in 0..iter {
            // Generate some operations
            for _j in 0..5 {
                let doc = docs.choose_mut(&mut rng).unwrap();
                make_random_change(doc, None, 0, &mut rng);
            }

            // Then merge 2 documents at random. I'd use sample_multiple but it doesn't return
            // mutable references to the sampled items.
            let idxs = sample(&mut rng, docs.len(), 2);
            let a_idx = idxs.index(0);
            let b_idx = idxs.index(1);
            assert_ne!(a_idx, b_idx);

            // Oh god this is awful. I can't take mutable references to two array items.
            let (a_idx, b_idx) = if a_idx < b_idx { (a_idx, b_idx) } else { (b_idx, a_idx) };
            // a<b.
            let (start, end) = docs[..].split_at_mut(b_idx);
            let a = &mut start[a_idx];
            let b = &mut end[0];

            a.replicate_into(b);
            b.replicate_into(a);

            if a != b {
                println!("Docs {} and {} after {} iterations:", a_idx, b_idx, _i);
                panic!("Documents do not match");
            }

            f(_i, &a, &b);

            for doc in &docs {
                doc.check(false);
            }
        }

        for doc in &docs {
            doc.check(false);
        }

        docs
    }

    pub fn gen_complex_docs(seed: u64, iter: usize) -> [ListCRDT; 3] {
        each_complex_random_doc_pair(seed, iter, |_, _, _| {})
    }
}

#[cfg(test)]
mod size_info {
    use std::mem::size_of;
    use content_tree::*;
    use crate::crdtspan::CRDTSpan;

    #[test]
    #[ignore]
    fn print_memory_stats() {
        let x = ContentTreeRaw::<CRDTSpan, ContentMetrics, DEFAULT_IE, DEFAULT_LE>::new();
        x.print_stats("", false);
        let x = ContentTreeRaw::<CRDTSpan, FullMetricsU32, DEFAULT_IE, DEFAULT_LE>::new();
        x.print_stats("", false);

        println!("sizeof ContentIndex offset {}", size_of::<<ContentMetrics as TreeMetrics<CRDTSpan>>::Value>());
        println!("sizeof FullIndex offset {}", size_of::<<FullMetricsU32 as TreeMetrics<CRDTSpan>>::Value>());
    }
}