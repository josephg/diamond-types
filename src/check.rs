use smallvec::smallvec;
use crate::{Branch, Frontier, OverlayValue};
use crate::OpLog;

impl OpLog {
    /// Check the internal state of the oplog. This is only exported for integration testing. You
    /// shouldn't have any reason to call this method.
    ///
    /// This method is public, but do not depend on it as part of the DT API. It could be removed at
    /// any time.
    #[allow(unused)]
    pub fn dbg_check(&self, deep: bool) {
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

impl Branch {
    #[allow(unused)]
    pub fn dbg_check(&self, deep: bool) {
        if deep {
            let mut num_invalid_entries = 0;

            todo!();
            // for (time, value) in &self.overlay {
            //     match value {
            //         OverlayValue::LWW(lwwval) => {
            //
            //         }
            //         OverlayValue::Map(_) => {}
            //         OverlayValue::Set(_) => {}
            //     }
            // }

            assert_eq!(num_invalid_entries, self.num_invalid);
        }
    }
}