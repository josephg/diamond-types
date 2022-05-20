use jumprope::JumpRope;
use crate::list::{Branch, ListCRDT, OpLog};
use smallvec::smallvec;
use crate::frontier::advance_frontier_by_known_run;
use crate::LocalVersion;

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
    fn get_frontier_inefficiently(&self) -> LocalVersion {
        // Could improve this by just looking at the last txn, and following shadows down.

        let mut b = smallvec![];
        for txn in self.history.entries.iter() {
            advance_frontier_by_known_run(&mut b, txn.parents.as_slice(), txn.span);
        }
        b
    }

    /// Check the internal state of the diamond types list. This is only exported for integration
    /// testing.
    ///
    /// You shouldn't have any reason to call this method.
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
    }

    #[allow(unused)]
    pub(crate) fn check_all_changes_rle_merged(&self) {
        assert_eq!(self.client_data[0].item_times.num_entries(), 1);
        // .. And operation log.
        assert_eq!(self.history.entries.num_entries(), 1);
    }
}

impl ListCRDT {
    /// Check the internal state of the diamond types document. This is only exported for
    /// integration testing.
    ///
    /// You shouldn't have any reason to call this method.
    ///
    /// This method is public, but do not depend on it as part of the DT API. It could be removed at
    /// any time.
    #[allow(unused)]
    pub fn dbg_check(&self, deep: bool) {
        self.oplog.dbg_check(deep);
    }
}