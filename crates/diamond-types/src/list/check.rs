use jumprope::JumpRope;
use crate::list::{ListCRDT, ROOT_ORDER};
use rle::HasLength;
use smallvec::{SmallVec, smallvec};

/// This file contains debugging assertions to validate the document's internal state.
///
/// This is used during fuzzing to make sure everything is working properly, and if not, find bugs
/// as early as possible.

impl ListCRDT {
    #[allow(unused)]
    pub fn dbg_assert_content_eq(&self, expected_content: &JumpRope) {
        if let Some(ref text) = self.text_content {
            assert_eq!(text, expected_content);
        }
    }

    // Used for testing.
    #[allow(unused)]
    pub fn check(&self, deep: bool) {
        self.index.check();

        if let Some(text) = self.text_content.as_ref() {
            assert_eq!(self.range_tree.content_len() as usize, text.len_chars());

            let num_deleted_items = self.deletes.iter().fold(0, |x, y| x + y.len());
            if let Some(del_content) = self.deleted_content.as_ref() {
                assert_eq!(del_content.chars().count(), num_deleted_items);
            }
        }

        let mut cursor = self.range_tree.cursor_at_start();
        loop {
            // The call to cursor.next() places the cursor at the next item before returning.
            let this_cursor = cursor.clone();

            if let Some(e) = cursor.next() { // Iterating manually for the borrow checker.
                // Each item's ID should come after its origin left and right
                assert!(e.origin_left == ROOT_ORDER || e.order > e.origin_left);
                assert!(e.origin_right == ROOT_ORDER || e.order > e.origin_right);
                assert_ne!(e.len, 0);

                if deep {
                    // Also check that the origin left appears before this entry, and origin right
                    // appears after it.
                    let left = self.get_cursor_after(e.origin_left, true);
                    assert!(left <= this_cursor);

                    let right = self.get_cursor_before(e.origin_right);
                    assert!(this_cursor < right);
                }
            } else { break; }
        }

        if deep {
            self.check_txns();
            self.check_index();
        }
    }

    fn check_index(&self) {
        // Go through each entry in the range tree and make sure we can find it using the index.
        for entry in self.range_tree.raw_iter() {
            let marker = self.marker_at(entry.order);
            unsafe { marker.as_ref() }.find(entry.order).unwrap();
        }
    }

    fn check_txns(&self) {
        // The shadow entries in txns name the smallest order for which all txns from
        // [shadow..txn.order] are transitive parents of the current txn.

        // I'm testing here sort of by induction. Iterating the txns in order allows us to assume
        // all previous txns have valid shadows while we advance.

        for (idx, txn) in self.txns.iter().enumerate() {
            assert!(txn.len > 0);

            // We contain prev_txn_order *and more*! See if we can extend the shadow by
            // looking at the other entries of parents.
            let mut parents = txn.parents.clone();
            let mut expect_shadow = txn.order;

            // The first txn *must* have ROOT as a parent, so 0 should never show up in shadow.
            assert_ne!(txn.shadow, 0);

            // Check our child_indexes all contain this item in their parents list.
            for child_idx in &txn.child_indexes {
                let child = &self.txns.0[*child_idx];
                assert!(child.parents.iter().any(|p| txn.contains(*p)));
            }

            if parents[0] == ROOT_ORDER {
                // The root order will be sorted out of order, but it doesn't matter because
                // if it shows up at all it should be the only item in parents.
                debug_assert_eq!(parents.len(), 1);
                if txn.order == 0 { expect_shadow = ROOT_ORDER; }
                assert!(txn.parent_indexes.is_empty());
            } else {
                parents.sort_by(|a, b| b.cmp(a)); // descending order
                let mut expect_parent_idx: SmallVec<[usize; 2]> = smallvec![];

                // By induction, we can assume the previous shadows are correct.
                for parent_order in parents {
                    // Note parent_order could point in the middle of a txn run.
                    let parent_idx = self.txns.find_index(parent_order).unwrap();
                    if !expect_parent_idx.contains(&parent_idx) {
                        expect_parent_idx.push(parent_idx);
                    }

                    let parent_txn = &self.txns.0[parent_idx];
                    let offs = parent_order - parent_txn.order;

                    // Check the parent txn names this txn in its child_indexes
                    assert!(parent_txn.child_indexes.contains(&idx));

                    // dbg!(parent_txn.order + offs, expect_shadow);
                    // Shift it if the expected shadow points to the last item in the txn run.
                    if parent_txn.order + offs + 1 == expect_shadow {
                        expect_shadow = parent_txn.shadow;
                    }
                }

                expect_parent_idx.sort_unstable();
                let mut actual_parent_idx = txn.parent_indexes.clone();
                actual_parent_idx.sort_unstable();

                // if expect_parent_idx != actual_parent_idx {
                //     dbg!(&self.txns.0[..=idx]);
                //     dbg!(&expect_parent_idx);
                //     dbg!(&txn);
                // }
                assert_eq!(expect_parent_idx, actual_parent_idx);
            }

            assert_eq!(txn.shadow, expect_shadow);
        }
    }

    #[allow(unused)]
    pub fn check_all_changes_rle_merged(&self) {
        assert_eq!(self.client_data[0].item_orders.len(), 1);
        assert_eq!(self.client_with_order.len(), 1);
        assert_eq!(self.txns.len(), 1);
    }
}