use jumprope::JumpRope;
use crate::list::{Branch, Frontier, ListCRDT, OpLog};
use smallvec::{SmallVec, smallvec};
use crate::list::frontier::advance_frontier_by_known_run;
use crate::list::history::History;
use crate::ROOT_TIME;

/// This file contains debugging assertions to validate the document's internal state.
///
/// This is used during fuzzing to make sure everything is working properly, and if not, find bugs
/// as early as possible.

impl Branch {
    #[allow(unused)]
    pub fn dbg_assert_content_eq_rope(&self, expected_content: &JumpRope) {
        assert_eq!(&self.content, expected_content);
    }


}

impl OpLog {
    fn get_frontier_inefficiently(&self) -> Frontier {
        // Could improve this by just looking at the last txn, and following shadows down.

        let mut b = smallvec![ROOT_TIME];
        for txn in self.history.entries.iter() {
            advance_frontier_by_known_run(&mut b, txn.parents.as_slice(), txn.span);
        }
        b
    }

    #[allow(unused)]
    pub fn check(&self, deep: bool) {
        let actual_frontier = self.get_frontier_inefficiently();
        assert_eq!(self.frontier, actual_frontier);

        if deep {
            self.history.check();
        }
    }

    #[allow(unused)]
    pub fn check_all_changes_rle_merged(&self) {
        assert_eq!(self.client_data[0].item_times.len(), 1);
        // .. And operation log.
        assert_eq!(self.history.entries.len(), 1);
    }
}

impl ListCRDT {
    // Used for testing.
    #[allow(unused)]
    pub fn check(&self, deep: bool) {
        self.ops.check(deep);
    }
}

impl History {
    fn check(&self) {
        let expect_root_children = self.entries
        .iter()
        .enumerate()
        .filter_map(|(i, entry)| {
            if entry.parents.len() == 1 && entry.parents[0] == ROOT_TIME {
                Some(i)
            } else { None }
        });
        assert!(expect_root_children.eq(self.root_child_indexes.iter().copied()));

        // The shadow entries in txns name the smallest order for which all txns from
        // [shadow..txn.order] are transitive parents of the current txn.

        // I'm testing here sort of by induction. Iterating the txns in order allows us to assume
        // all previous txns have valid shadows while we advance.

        for (idx, hist) in self.entries.iter().enumerate() {
            assert!(hist.span.end > hist.span.start);

            // We contain prev_txn_order *and more*! See if we can extend the shadow by
            // looking at the other entries of parents.
            let mut parents = hist.parents.clone();
            let mut expect_shadow = hist.span.start;

            // The first txn *must* have ROOT as a parent, so 0 should never show up in shadow.
            assert_ne!(hist.shadow, 0);

            // Check our child_indexes all contain this item in their parents list.
            for child_idx in &hist.child_indexes {
                let child = &self.entries.0[*child_idx];
                assert!(child.parents.iter().any(|p| hist.contains(*p)));
            }

            if parents[0] == ROOT_TIME {
                // The root order will be sorted out of order, but it doesn't matter because
                // if it shows up at all it should be the only item in parents.
                debug_assert_eq!(parents.len(), 1);
                if hist.span.start == 0 { expect_shadow = ROOT_TIME; }
                assert!(hist.parent_indexes.is_empty());
            } else {
                parents.sort_by(|a, b| b.cmp(a)); // descending order
                let mut expect_parent_idx: SmallVec<[usize; 2]> = smallvec![];

                // By induction, we can assume the previous shadows are correct.
                for parent_order in parents {
                    // Note parent_order could point in the middle of a txn run.
                    let parent_idx = self.entries.find_index(parent_order).unwrap();
                    if !expect_parent_idx.contains(&parent_idx) {
                        expect_parent_idx.push(parent_idx);
                    }

                    let parent_txn = &self.entries.0[parent_idx];
                    let offs = parent_order - parent_txn.span.start;

                    // Check the parent txn names this txn in its child_indexes
                    assert!(parent_txn.child_indexes.contains(&idx));

                    // dbg!(parent_txn.order + offs, expect_shadow);
                    // Shift it if the expected shadow points to the last item in the txn run.
                    if parent_txn.span.start + offs + 1 == expect_shadow {
                        expect_shadow = parent_txn.shadow;
                    }
                }

                expect_parent_idx.sort_unstable();
                let mut actual_parent_idx = hist.parent_indexes.clone();
                actual_parent_idx.sort_unstable();

                // if expect_parent_idx != actual_parent_idx {
                //     dbg!(&self.txns.0[..=idx]);
                //     dbg!(&expect_parent_idx);
                //     dbg!(&txn);
                // }
                assert_eq!(expect_parent_idx, actual_parent_idx);
            }

            assert_eq!(hist.shadow, expect_shadow);
        }
    }
}