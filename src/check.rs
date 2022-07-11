use smallvec::smallvec;
use crate::frontier::advance_frontier_by_known_run;
use crate::LocalVersion;
use crate::NewOpLog;

impl NewOpLog {
    fn get_frontier_inefficiently(&self) -> LocalVersion {
        // Could improve this by just looking at the last txn, and following shadows down.

        let mut b = smallvec![];
        for txn in self.cg.history.entries.iter() {
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

        self.cg.dbg_check(deep);

        // for map in self.maps.iter() {
        //     for (key, item) in map.children.iter() {
        //         // Each child of a map must be a LWWRegister.
        //         let child_item = &self.known_crdts[*item];
        //         assert_eq!(child_item.kind, CRDTKind::LWWRegister);
        //         assert_eq!(child_item.history.created_at, map.created_at);
        //     }
        // }

        // TODO: Check all owned CRDT objects exists in overlay.
    }
}
