use crate::list::{Frontier, Time};
use crate::list::history::{History, HistoryEntry};
use crate::localtime::TimeSpan;
use crate::rle::RleVec;
use crate::ROOT_TIME;

pub(crate) fn advance_frontier_by(frontier: &mut Frontier, history: &History, range: TimeSpan) {
    let txn = history.entries.find(range.start).unwrap();
    if let Some(parent) = txn.parent_at_time(range.start) {
        advance_frontier_by_known(frontier, &[parent], range);
    } else {
        advance_frontier_by_known(frontier, &txn.parents, range);
    }
}

pub(crate) fn retreat_frontier_by(frontier: &mut Frontier, history: &History, range: TimeSpan) {
    let txn = history.entries.find(range.start).unwrap();
    retreat_frontier_known_txn(frontier, history, txn, range);
}

pub(crate) fn frontier_is_sorted(branch: &[Time]) -> bool {
    // For debugging.
    if branch.len() >= 2 {
        let mut last = branch[0];
        for t in &branch[1..] {
            debug_assert!(*t != last);
            if *t < last { return false; }
            last = *t;
        }
    }
    true
}

pub(crate) fn debug_assert_frontier_sorted(frontier: &[Time]) {
    debug_assert!(frontier_is_sorted(frontier));
}

fn add_to_frontier(frontier: &mut Frontier, new_item: Time) {
    // In order to maintain the order of items in the branch, we want to insert the new item in the
    // appropriate place.
    let new_idx = frontier.binary_search(&new_item).unwrap_err();
    frontier.insert(new_idx, new_item);
    debug_assert_frontier_sorted(frontier.as_slice());
}

/// Advance branch frontier by a transaction. This is written creating a new branch, which is
/// somewhat inefficient (especially if the frontier is spilled).
pub(crate) fn advance_frontier_by_known(frontier: &mut Frontier, txn_parents: &[Time], range: TimeSpan) {
    // TODO: Check the branch contains everything in txn_parents, but not txn_id:
    // Check the operation fits. The operation should not be in the branch, but
    // all the operation's parents should be.
    // From braid-kernel:
    // assert(!branchContainsVersion(db, order, branch), 'db already contains version')
    // for (const parent of op.parents) {
    //    assert(branchContainsVersion(db, parent, branch), 'operation in the future')
    // }

    if frontier.len() == 1 && txn_parents.len() == 1 && frontier[0] == txn_parents[0] {
        // Fast path.
        debug_assert!(frontier[0] == ROOT_TIME || range.start > frontier[0]);
        frontier[0] = range.last();
        return;
    }

    assert!(!frontier.contains(&range.start)); // Remove this when branch_contains_version works.
    debug_assert_frontier_sorted(frontier.as_slice());

    frontier.retain(|o| !txn_parents.contains(o)); // Usually removes all elements.

    // In order to maintain the order of items in the branch, we want to insert the new item in the
    // appropriate place.
    add_to_frontier(frontier, range.last());
}

pub(crate) fn retreat_frontier_known_txn(frontier: &mut Frontier, history: &History, txn: &HistoryEntry, range: TimeSpan) {
    let last_order = range.last();
    let idx = frontier.iter().position(|&e| e == last_order).unwrap();

    debug_assert!(txn.contains(last_order));
    debug_assert_frontier_sorted(frontier.as_slice());

    if range.start > txn.span.start {
        frontier[idx] = range.start - 1;
        debug_assert_frontier_sorted(frontier.as_slice());
    } else if range.start == txn.span.start {
        frontier.retain(|t| *t != last_order);
        // branch.swap_remove(idx);

        for &parent in &txn.parents {
            // TODO: This is pretty inefficient. We're calling branch_contains_order in a loop and
            // each call to branch_contains_version does a call to history.find() in turn for each
            // item in branch.
            if frontier.is_empty() || !history.branch_contains_order(frontier, parent) {
                add_to_frontier(frontier, parent);
                // branch.push(parent);
            }
        }

        // branch.sort();
    } else {
        // Is this something worth implementing?
        unimplemented!("retreat_branch cannot retreat by more than one transaction");
    }
}

pub fn frontier_eq(a: &[Time], b: &[Time]) -> bool {
    // Almost all branches only have one element in them.
    debug_assert_frontier_sorted(a);
    debug_assert_frontier_sorted(b);
    a == b
    // a.len() == b.len() && ((a.len() == 1 && a[0] == b[0]) || {
    //     a.iter().all(|o| b.contains(o))
    // })
}

pub fn frontier_is_root(branch: &[Time]) -> bool {
    branch.len() == 1 && branch[0] == ROOT_TIME
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;

    use crate::list::Frontier;
    use crate::ROOT_TIME;

    use super::*;

    #[test]
    fn frontier_movement_smoke_tests() {
        let mut branch: Frontier = smallvec![ROOT_TIME];
        advance_frontier_by_known(&mut branch, &[ROOT_TIME], (0..10).into());
        assert_eq!(branch.as_slice(), &[9]);

        let history = History::from_entries(&[
            HistoryEntry {
                span: (0..10).into(), shadow: ROOT_TIME,
                parents: smallvec![ROOT_TIME],
                parent_indexes: smallvec![], child_indexes: smallvec![]
            }
        ]);

        retreat_frontier_by(&mut branch, &history, (5..10).into());
        assert_eq!(branch.as_slice(), &[4]);

        retreat_frontier_by(&mut branch, &history, (0..5).into());
        assert_eq!(branch.as_slice(), &[ROOT_TIME]);
    }

    #[test]
    fn frontier_stays_sorted() {
        let history = History::from_entries(&[
            HistoryEntry {
                span: (0..2).into(), shadow: ROOT_TIME,
                parents: smallvec![ROOT_TIME],
                parent_indexes: smallvec![], child_indexes: smallvec![]
            },
            HistoryEntry {
                span: (2..6).into(), shadow: ROOT_TIME,
                parents: smallvec![0],
                parent_indexes: smallvec![], child_indexes: smallvec![]
            },
            HistoryEntry {
                span: (6..50).into(), shadow: 6,
                parents: smallvec![0],
                parent_indexes: smallvec![], child_indexes: smallvec![]
            },
        ]);

        let mut branch: Frontier = smallvec![1, 10];
        advance_frontier_by(&mut branch, &history, (2..4).into());
        assert_eq!(branch.as_slice(), &[1, 3, 10]);

        advance_frontier_by(&mut branch, &history, (11..12).into());
        assert_eq!(branch.as_slice(), &[1, 3, 11]);

        retreat_frontier_by(&mut branch, &history, (2..4).into());
        assert_eq!(branch.as_slice(), &[1, 11]);

        retreat_frontier_by(&mut branch, &history, (11..12).into());
        assert_eq!(branch.as_slice(), &[1, 10]);
    }
}
