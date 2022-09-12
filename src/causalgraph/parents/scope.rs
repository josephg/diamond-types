use crate::*;

/// A scope is a part of history attached to a specific CRDT

// TODO: Move me!
#[derive(Debug, Clone)]
pub(crate) struct ScopedParents {
    pub(crate) created_at: Time,

    /// This isn't a real Version. Its a list of times at which this CRDT was deleted.
    ///
    /// (What do we need this for??)
    pub(crate) deleted_at: LocalVersion,

    pub(crate) owned_times: RleVec<DTRange>,
}

impl ScopedParents {

    pub(crate) fn exists_at(&self, history: &Parents, version: &[Time]) -> bool {
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