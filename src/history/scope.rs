use crate::{History, ROOT_TIME, ScopedHistory, Time};

/// A scope is a part of history attached to a specific CRDT

impl ScopedHistory {

    pub(crate) fn exists_at(&self, history: &History, version: &[Time]) -> bool {
        // If the item has not been created yet, return None.
        if self.created_at != ROOT_TIME && !history.version_contains_time(version, self.created_at) {
            // Not created yet.
            return false;
        }

        // If the item has been deleted, return false.
        for v in &self.deleted_at {
            if history.version_contains_time(version, *v) {
                // Deleted.
                return false;
            }
        }

        true
    }
}