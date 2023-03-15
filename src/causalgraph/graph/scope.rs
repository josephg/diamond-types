//! TODO: This code is not currently used. Use it or remove it!

use std::collections::BinaryHeap;
use smallvec::smallvec;
use crate::*;
use crate::causalgraph::graph::tools::DiffFlag;
use crate::causalgraph::graph::tools::DiffFlag::{OnlyA, OnlyB, Shared};
use crate::frontier::debug_assert_frontier_sorted;

/// A scope is a part of history attached to a specific CRDT

// TODO: Move me!
#[derive(Debug, Clone)]
pub(crate) struct ScopedParents {
    pub(crate) created_at: LV,

    /// This isn't a real Version. Its a list of times at which this CRDT was deleted.
    ///
    /// (What do we need this for??)
    pub(crate) deleted_at: Frontier,

    pub(crate) owned_times: RleVec<DTRange>,
}

impl ScopedParents {
    pub(crate) fn exists_at(&self, graph: &Graph, version: &[LV]) -> bool {
        // If the item has not been created yet, return None.
        if !graph.frontier_contains_version(version, self.created_at) {
            // Not created yet.
            return false;
        }

        // If the item has been deleted, return false.
        for v in self.deleted_at.iter() {
            if graph.frontier_contains_version(version, *v) {
                // Deleted.
                return false;
            }
        }

        true
    }
}

// TODO: Remove this.
const OLD_INVALID_ROOT_TIME: usize = usize::MAX;

impl Graph {
    pub(crate) fn version_in_scope(&self, version: &[LV], info: &ScopedParents) -> Option<Frontier> {
        debug_assert_ne!(info.created_at, OLD_INVALID_ROOT_TIME);

        // If v == creation time, its a bit hacky but I still consider that a valid version, because
        // the CRDT has a value then (the default value for the CRDT).
        debug_assert_frontier_sorted(version);

        let Some(&highest_time) = version.last() else {
            // The root item has a creation time at the root time. But nothing else exists then.
            return if info.created_at == OLD_INVALID_ROOT_TIME {
                Some(Frontier::root())
            } else {
                None
            }
        };

        // let info = &oplog.items[item];
        if info.created_at != OLD_INVALID_ROOT_TIME && highest_time < info.created_at {
            // If the version exists entirely before this root was created, there is no common
            // ancestor.
            return None;
        }

        if version.len() == 1 {
            if let Some(last) = info.owned_times.last_entry() {
                let last_time = last.last();

                // Fast path. If the last operation in the root is a parent of v, we're done.
                if self.is_direct_descendant_coarse(highest_time, last_time) {
                    return Some(Frontier::new_1(last_time));
                }
            }

            if info.owned_times.find_index(highest_time).is_ok() {
                // Another fast path. The requested version is already in the operation.
                return Some(Frontier::new_1(highest_time));
            }

            // TODO: Should we have more fast paths here?
        }

        // Slow path. We'll trace back through time until we land entirely in the root.
        let mut result = smallvec![];

        // I'm using DiffFlag here, but only the OnlyA and Shared values out of it.
        let mut queue: BinaryHeap<(LV, DiffFlag)> = BinaryHeap::new();

        for &t in version {
            // Append children so long as they aren't earlier than the item's ctime.
            if info.created_at == OLD_INVALID_ROOT_TIME || t >= info.created_at {
                queue.push((t, OnlyA));
            }
        }

        let mut num_shared_entries = 0;

        while let Some((time, mut flag)) = queue.pop() {
            if flag == Shared { num_shared_entries -= 1; }
            debug_assert_ne!(flag, OnlyB);

            // dbg!((ord, flag));
            while let Some((peek_time, peek_flag)) = queue.peek() {
                debug_assert_ne!(*peek_flag, OnlyB);

                if *peek_time != time { break; } // Normal case.
                else {
                    // 3 cases if peek_flag != flag. We set flag = Shared in all cases.
                    // if *peek_flag != flag { flag = Shared; }
                    if flag == OnlyA && *peek_flag == Shared { flag = Shared; }
                    if *peek_flag == Shared { num_shared_entries -= 1; }
                    queue.pop();
                }
            }

            if flag == OnlyA && info.owned_times.find_index(time).is_ok() {
                // The time we've picked is in the CRDT we're looking for. Woohoo!
                result.push(time);
                flag = Shared;
            }

            if flag == Shared && queue.len() == num_shared_entries { break; } // No expand necessary.

            // Ok, we need to expand the item based on its parents. The tricky thing here is what
            // we can skip safely.
            let containing_txn = self.entries.find_packed(time);

            let min_safe_base = if flag == Shared {
                0
            } else {
                // TODO: Reuse binary search from above.
                let r = info.owned_times.find_sparse(time).0;
                r.unwrap_err().start
            };
            let base = min_safe_base.max(containing_txn.span.start);

            // Eat everything >= base in queue.
            while let Some((peek_time, peek_flag)) = queue.peek() {
                // dbg!((peek_ord, peek_flag));
                if *peek_time < base { break; } else {
                    // if *peek_flag != flag {
                    if flag == OnlyA && *peek_flag == Shared {
                        flag = Shared;
                    }
                    if *peek_flag == Shared { num_shared_entries -= 1; }
                    queue.pop();
                }
            }

            containing_txn.with_parents(base, |parents| {
                for &p in parents {
                    queue.push((p, flag));
                    if flag == Shared { num_shared_entries += 1; }
                }
            });

            // If there's only shared entries left, stop.
            if queue.len() == num_shared_entries { break; }
        }

        result.reverse();
        debug_assert_frontier_sorted(&result);
        Some(Frontier(result))
    }

}