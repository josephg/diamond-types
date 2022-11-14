use crate::causalgraph::parents::Parents;
use crate::Frontier;

impl Parents {
    pub(crate) fn dbg_get_frontier_inefficiently(&self) -> Frontier {
        // Could improve this by just looking at the last txn, and following shadows down.
        // TODO: Actually thats a useful function!

        let mut b = Frontier::root();
        for txn in self.0.iter() {
            b.advance_by_known_run(txn.parents.as_ref(), txn.span);
        }
        b
    }

    pub(crate) fn dbg_check_subgraph(&self, _deep: bool, sparse: bool) {
        // The shadow entries in txns name the smallest order for which all txns from
        // [shadow..txn.order] are transitive parents of the current txn.

        // I'm testing here sort of by induction. Iterating the txns in order allows us to assume
        // all previous txns have valid shadows while we advance.

        for hist in self.0.iter() {
            assert!(hist.span.end > hist.span.start);

            hist.parents.debug_check_sorted();

            // We contain prev_txn_order *and more*! See if we can extend the shadow by
            // looking at the other entries of parents.
            let parents = &hist.parents;
            let mut expect_shadow = hist.span.start;

            if !parents.is_empty() {
                // By induction, we can assume the previous shadows are correct.
                for p in parents.iter().copied().rev() { // highest to lowest.
                    assert!(p < hist.span.start);

                    if sparse {
                        assert!(self.0.contains_needle(p));
                    }

                    // Note parent_order could point in the middle of a txn run.
                    let parent_idx = self.0.find_index(p).unwrap();
                    let parent_txn = &self.0.0[parent_idx];

                    // Shift it if the expected shadow points to the last item in the txn run.
                    // if p + 1 == parent_txn.span.end && expect_shadow == self.0.0[parent_idx + 1].span.start {
                    //     expect_shadow = parent_txn.shadow;
                    // }
                    if p + 1 == expect_shadow {
                        expect_shadow = parent_txn.shadow;
                    }
                }
            }

            assert_eq!(hist.shadow, expect_shadow);
        }
    }

    pub(crate) fn dbg_check(&self, deep: bool) {
        self.0.check_packed_from_0();
        self.dbg_check_subgraph(deep, false);
    }
}
