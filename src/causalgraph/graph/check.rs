use crate::causalgraph::graph::Graph;
use crate::Frontier;
use crate::frontier::is_sorted_slice;

impl Graph {
    pub(crate) fn dbg_get_frontier_inefficiently(&self) -> Frontier {
        // Could improve this by just looking at the last txn, and following shadows down.
        // TODO: Actually thats a useful function!

        let mut b = Frontier::root();
        for txn in self.entries.iter() {
            b.advance_by_known_run(txn.parents.as_ref(), txn.span);
        }
        b
    }

    fn dbg_check_internal(&self, deep: bool, sparse: bool, strict_shadow_checks: bool) {
        // The shadow entries in txns name the smallest order for which all txns from
        // [shadow..txn.order] are transitive parents of the current txn.

        // I'm testing here sort of by induction. Iterating the txns in order allows us to assume
        // all previous txns have valid shadows while we advance.

        if !sparse {
            self.entries.check_packed_from_0();
        }

        // And check the list is properly RLE compacted
        self.entries.check_fully_merged();

        let expect_root_children = self.entries
            .iter()
            .enumerate()
            .filter_map(|(i, entry)| {
                if entry.parents.is_root() {
                    Some(i)
                } else { None }
            });

        assert!(expect_root_children.eq(self.root_child_indexes.iter().copied()));

        let mut next_change = 0;
        for (idx, hist) in self.entries.iter().enumerate() {
            assert!(hist.span.end > hist.span.start);

            hist.parents.debug_check_sorted();

            // We contain prev_txn_order *and more*! See if we can extend the shadow by
            // looking at the other entries of parents.
            let parents = &hist.parents;
            let mut expect_shadow = next_change;//hist.span.start;
            next_change = hist.span.end;

            // Check our child_indexes all contain this item in their parents list.
            for child_idx in &hist.child_indexes {
                let child = &self.entries.0[*child_idx];
                assert!(child.parents.iter().any(|p| hist.contains(*p)));
            }
            assert!(is_sorted_slice::<true, _>(&hist.child_indexes));

            if !parents.is_empty() {
                // By induction, we can assume the previous shadows are correct.
                for p in parents.iter().copied().rev() { // highest to lowest.
                    assert!(p < hist.span.start);

                    if sparse {
                        assert!(self.entries.contains_needle(p));
                    }

                    // Note parent_order could point in the middle of a txn run.
                    let parent_idx = self.entries.find_index(p).unwrap();
                    let parent_txn = &self.entries.0[parent_idx];

                    // Check the parent txn names this txn in its child_indexes
                    assert!(parent_txn.child_indexes.contains(&idx));

                    // Shift it if the expected shadow points to the last item in the txn run.
                    // if p + 1 == parent_txn.span.end && expect_shadow == self.0.0[parent_idx + 1].span.start {
                    //     expect_shadow = parent_txn.shadow;
                    // }
                    if p + 1 == expect_shadow || (sparse && expect_shadow == self.entries.0[parent_idx+1].span.start) {
                        expect_shadow = parent_txn.shadow;
                    }
                }

                // And check that none of the entries in parents are redundant.
                if deep {
                    self.find_dominators_full(parents.iter().copied(), |_v, dominates| {
                        assert!(dominates);
                    });
                }
            }

            if strict_shadow_checks {
                assert_eq!(hist.shadow, expect_shadow);
            } else {
                // dbg!(hist.shadow, expect_shadow);
                assert!(hist.shadow >= expect_shadow);
            }
        }
    }

    pub(crate) fn dbg_check(&self, deep: bool) {
        self.dbg_check_internal(deep, false, true);
    }

    pub(crate) fn dbg_check_subgraph(&self, deep: bool) {
        self.dbg_check_internal(deep, true, false);
    }
}
