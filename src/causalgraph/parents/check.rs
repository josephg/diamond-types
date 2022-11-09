use crate::causalgraph::parents::Parents;
use crate::Frontier;

impl Parents {
    pub(crate) fn dbg_get_frontier_inefficiently(&self) -> Frontier {
        // Could improve this by just looking at the last txn, and following shadows down.
        // TODO: Actually thats a useful function!

        let mut b = Frontier::root();
        for txn in self.entries.iter() {
            b.advance_by_known_run(txn.parents.as_ref(), txn.span);
        }
        b
    }

    pub(crate) fn dbg_check(&self) {
        self.entries.check_packed();

        let expect_root_children = self.entries
            .iter()
            .enumerate()
            .filter_map(|(i, entry)| {
                if entry.parents.is_root() {
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

            hist.parents.debug_check_sorted();

            // We contain prev_txn_order *and more*! See if we can extend the shadow by
            // looking at the other entries of parents.
            let mut parents = hist.parents.clone().0;
            let mut expect_shadow = hist.span.start;

            // Check our child_indexes all contain this item in their parents list.
            for child_idx in &hist.child_indexes {
                let child = &self.entries.0[*child_idx];
                assert!(child.parents.iter().any(|p| hist.contains(*p)));
            }

            if !parents.is_empty() {
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
