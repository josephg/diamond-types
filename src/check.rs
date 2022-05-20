use smallvec::smallvec;
use crate::frontier::{advance_frontier_by_known_run, clone_smallvec, debug_assert_frontier_sorted};
use crate::history::History;
use crate::{CRDTKind, LocalVersion};
use crate::NewOpLog;

impl NewOpLog {
    fn get_frontier_inefficiently(&self) -> LocalVersion {
        // Could improve this by just looking at the last txn, and following shadows down.

        let mut b = smallvec![];
        for txn in self.history.entries.iter() {
            advance_frontier_by_known_run(&mut b, txn.parents.as_slice(), txn.span);
        }
        b
    }

    /// Check the internal state of the oplog. This is only exported for integration testing. You
    /// shouldn't have any reason to call this method.
    ///
    /// This method is public, but do not depend on it as part of the DT API. It could be removed at
    /// any time.
    #[allow(unused)]
    pub fn dbg_check(&self, deep: bool) {
        let actual_frontier = self.get_frontier_inefficiently();
        assert_eq!(self.version, actual_frontier);

        if deep {
            self.history.check();
        }

        // The client_with_localtime should match with the corresponding items in client_data
        self.client_with_localtime.check_packed();
        for pair in self.client_with_localtime.iter() {
            let expected_range = pair.range();

            let span = pair.1;
            let client = &self.client_data[span.agent as usize];
            let actual_range = client.item_times.find_packed_and_split(span.seq_range);

            assert_eq!(actual_range.1, expected_range);
        }

        if deep {
            // Also check the other way around.
            for (agent, client) in self.client_data.iter().enumerate() {
                for range in client.item_times.iter() {
                    let actual = self.client_with_localtime.find_packed_and_split(range.1);
                    assert_eq!(actual.1.agent as usize, agent);
                }
            }
        }

        for scope in self.scopes.iter() {
            if scope.kind == CRDTKind::Map {
                assert!(scope.history.owned_times.is_empty());
                assert!(scope.map_children.is_some());
            } else {
                assert!(scope.map_children.is_none());
            }
        }

        // TODO: Also check the scopes are non-overlapping.
    }
}

impl History {
    pub(crate) fn check(&self) {
        self.entries.check_packed();

        let expect_root_children = self.entries
        .iter()
        .enumerate()
        .filter_map(|(i, entry)| {
            if entry.parents.is_empty() {
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

            debug_assert_frontier_sorted(&hist.parents);

            // We contain prev_txn_order *and more*! See if we can extend the shadow by
            // looking at the other entries of parents.
            let mut parents = clone_smallvec(&hist.parents);
            let mut expect_shadow = hist.span.start;

            // The first txn *must* have ROOT as a parent, so 0 should never show up in shadow.
            assert_ne!(hist.shadow, 0);

            // Check our child_indexes all contain this item in their parents list.
            for child_idx in &hist.child_indexes {
                let child = &self.entries.0[*child_idx];
                assert!(child.parents.iter().any(|p| hist.contains(*p)));
            }

            if parents.is_empty() {
                if hist.span.start == 0 { expect_shadow = usize::MAX; }
                // assert!(hist.parent_indexes.is_empty());
            } else {
                // We'll resort parents into descending order.
                parents.sort_unstable_by(|a, b| b.cmp(a)); // descending order

                // By induction, we can assume the previous shadows are correct.
                for parent_order in parents {
                    // Note parent_order could point in the middle of a txn run.
                    let parent_idx = self.entries.find_index(parent_order).unwrap();
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
            }

            assert_eq!(hist.shadow, expect_shadow);
        }
    }
}
