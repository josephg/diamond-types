use jumprope::JumpRope;
use crate::list::{ListBranch, ListCRDT, ListOpLog};

/// This file contains debugging assertions to validate the document's internal state.
///
/// This is used during fuzzing to make sure everything is working properly, and if not, find bugs
/// as early as possible.

impl ListBranch {
    #[allow(unused)]
    pub fn dbg_assert_content_eq_rope(&self, expected_content: &JumpRope) {
        assert_eq!(&self.content, expected_content);
    }


}

impl ListOpLog {
    /// Check the internal state of the diamond types list. This is only exported for integration
    /// testing.
    ///
    /// You shouldn't have any reason to call this method.
    ///
    /// This method is public, but do not depend on it as part of the DT API. It could be removed at
    /// any time.
    #[allow(unused)]
    pub fn dbg_check(&self, deep: bool) {
        self.cg.dbg_check(deep);
    }

    #[allow(unused)]
    pub(crate) fn check_all_changes_rle_merged(&self) {
        assert_eq!(self.cg.agent_assignment.client_data[0].lv_for_seq.num_entries(), 1);
        // .. And operation log.
        assert_eq!(self.cg.graph.entries.num_entries(), 1);
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