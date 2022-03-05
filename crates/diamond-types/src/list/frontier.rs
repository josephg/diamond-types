use crate::list::{Frontier, Time};
use crate::list::history::History;
use crate::localtime::TimeSpan;
use crate::ROOT_TIME;

/// Advance a frontier by the set of time spans in range
pub(crate) fn advance_frontier_by(frontier: &mut Frontier, history: &History, mut range: TimeSpan) {
    let mut txn_idx = history.entries.find_index(range.start).unwrap();
    while !range.is_empty() {
        let txn = &history.entries[txn_idx];
        debug_assert!(txn.contains(range.start));

        let end = txn.span.end.min(range.end);
        txn.with_parents(range.start, |parents| {
            advance_frontier_by_known_run(frontier, parents, (range.start..end).into());
        });

        range.start = end;
        // The txns are in order, so we're guaranteed that subsequent ranges will be in subsequent
        // txns in the list.
        txn_idx += 1;
    }
}

pub(crate) fn retreat_frontier_by(frontier: &mut Frontier, history: &History, mut range: TimeSpan) {
    if range.is_empty() { return; }

    debug_assert_frontier_sorted(frontier.as_slice());

    let mut txn_idx = history.entries.find_index(range.last()).unwrap();
    loop {
        let last_order = range.last();
        let txn = &history.entries[txn_idx];
        // debug_assert_eq!(txn_idx, history.entries.find_index(range.last()).unwrap());
        debug_assert_eq!(txn, history.entries.find(last_order).unwrap());
        // let mut idx = frontier.iter().position(|&e| e == last_order).unwrap();

        if frontier.len() == 1 {
            // Fast case. Just replace frontier's contents with parents.
            if range.start > txn.span.start {
                frontier[0] = range.start - 1;
                break;
            } else {
                *frontier = txn.parents.as_slice().into();
            }
        } else {
            // Remove the old item from frontier and only reinsert parents when they aren't included
            // in the transitive history from this point.
            frontier.retain(|t| *t != last_order);

            txn.with_parents(range.start, |parents| {
                for parent in parents {
                    // TODO: This is pretty inefficient. We're calling frontier_contains_time in a
                    // loop and each call to frontier_contains_time does a call to history.find() in
                    // turn for each item in branch.
                    debug_assert!(!frontier.is_empty());
                    // TODO: At least check shadow directly.
                    if !history.frontier_contains_time(frontier, *parent) {
                        add_to_frontier(frontier, *parent);
                    }
                }
            });
        }

        if range.start >= txn.span.start {
            break;
        }

        // Otherwise keep scanning down through the txns.
        range.end = txn.span.start;
        txn_idx -= 1;
    }
    if cfg!(debug_assertions) { check_frontier(frontier, history); }
    debug_assert_frontier_sorted(frontier.as_slice());
}

/// Frontiers should always be sorted smallest to largest.
pub(crate) fn frontier_is_sorted(branch: &[Time]) -> bool {
    // For debugging.
    if branch.len() >= 2 {
        let mut last = branch[0];
        for t in &branch[1..] {
            debug_assert!(*t != last);
            if last > *t { return false; }
            last = *t;
        }
    }
    true
}

pub(crate) fn debug_assert_frontier_sorted(frontier: &[Time]) {
    debug_assert!(frontier_is_sorted(frontier));
}

pub(crate) fn check_frontier(frontier: &[Time], history: &History) {
    assert!(frontier_is_sorted(frontier));
    if frontier.len() >= 2 {
        // let mut frontier = frontier.iter().copied().collect::<Vec<_>>();
        let mut frontier = frontier.to_vec();
        for i in 0..frontier.len() {
            let removed = frontier.remove(i);
            assert!(!history.frontier_contains_time(&frontier, removed));
            frontier.insert(i, removed);
        }
    }
}

fn add_to_frontier(frontier: &mut Frontier, new_item: Time) {
    // In order to maintain the order of items in the branch, we want to insert the new item in the
    // appropriate place.

    // Binary search might actually be slower here than a linear scan.
    let new_idx = frontier.binary_search(&new_item).unwrap_err();
    frontier.insert(new_idx, new_item);
    debug_assert_frontier_sorted(frontier.as_slice());
}

/// Advance branch frontier by a transaction.
///
/// This is ONLY VALID if the range is entirely within a txn.
pub(crate) fn advance_frontier_by_known_run(frontier: &mut Frontier, parents: &[Time], span: TimeSpan) {
    // TODO: Check the branch contains everything in txn_parents, but not txn_id:
    // Check the operation fits. The operation should not be in the branch, but
    // all the operation's parents should be.
    // From braid-kernel:
    // assert(!branchContainsVersion(db, order, branch), 'db already contains version')
    // for (const parent of op.parents) {
    //    assert(branchContainsVersion(db, parent, branch), 'operation in the future')
    // }

    if parents.len() == 1 && frontier.len() == 1 && parents[0] == frontier[0] {
        // Short circuit the common case where time is just advancing linearly.
        frontier[0] = span.last();
        return;
    } else if frontier.as_slice() == parents {
        // TODO: This is another short circuit. Can probably remove this?
        frontier.truncate(1);
        frontier[0] = span.last();
        return;
    }

    assert!(!frontier.contains(&span.start)); // Remove this when branch_contains_version works.
    debug_assert_frontier_sorted(frontier.as_slice());

    frontier.retain(|o| !parents.contains(o)); // Usually removes all elements.

    // In order to maintain the order of items in the branch, we want to insert the new item in the
    // appropriate place.
    // TODO: Check if its faster to try and append it to the end first.
    add_to_frontier(frontier, span.last());
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

#[allow(unused)]
pub fn frontier_is_root(branch: &[Time]) -> bool {
    branch.len() == 1 && branch[0] == ROOT_TIME
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;

    use crate::list::Frontier;
    use crate::list::history::HistoryEntry;
    use crate::ROOT_TIME;

    use super::*;

    #[test]
    fn frontier_movement_smoke_tests() {
        let mut branch: Frontier = smallvec![ROOT_TIME];
        advance_frontier_by_known_run(&mut branch, &[ROOT_TIME], (0..10).into());
        assert_eq!(branch.as_slice(), &[9]);

        let history = History::from_entries(&[
            HistoryEntry {
                span: (0..10).into(), shadow: ROOT_TIME,
                parents: smallvec![ROOT_TIME],
                child_indexes: smallvec![]
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
                child_indexes: smallvec![]
            },
            HistoryEntry {
                span: (2..6).into(), shadow: ROOT_TIME,
                parents: smallvec![0],
                child_indexes: smallvec![]
            },
            HistoryEntry {
                span: (6..50).into(), shadow: 6,
                parents: smallvec![0],
                child_indexes: smallvec![]
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
