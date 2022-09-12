use smallvec::{Array, SmallVec};
use crate::causalgraph::parents::Parents;
use crate::dtrange::DTRange;
use crate::{LocalVersion, Time};

/// Advance a frontier by the set of time spans in range
pub(crate) fn advance_frontier_by(frontier: &mut LocalVersion, history: &Parents, mut range: DTRange) {
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

pub(crate) fn retreat_frontier_by(frontier: &mut LocalVersion, history: &Parents, mut range: DTRange) {
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
                    if !history.version_contains_time(frontier, *parent) {
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

pub(crate) fn clean_version<T: Array<Item=usize>>(v: &mut SmallVec<T>) {
    if !frontier_is_sorted(v.as_slice()) {
        v.sort_unstable();
    }
}

pub(crate) fn debug_assert_frontier_sorted(frontier: &[Time]) {
    debug_assert!(frontier_is_sorted(frontier));
}

pub(crate) fn check_frontier(frontier: &[Time], history: &Parents) {
    assert!(frontier_is_sorted(frontier));
    if frontier.len() >= 2 {
        // let mut frontier = frontier.iter().copied().collect::<Vec<_>>();
        let mut frontier = frontier.to_vec();
        for i in 0..frontier.len() {
            let removed = frontier.remove(i);
            assert!(!history.version_contains_time(&frontier, removed));
            frontier.insert(i, removed);
        }
    }
}

pub(crate) fn add_to_frontier(frontier: &mut LocalVersion, new_item: Time) {
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
pub(crate) fn advance_frontier_by_known_run(frontier: &mut LocalVersion, parents: &[Time], span: DTRange) {
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
        if frontier.is_empty() {
            frontier.push(span.last())
        } else {
            // TODO: This is another short circuit. Can probably remove this?
            frontier.truncate(1);
            frontier[0] = span.last();
        }
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

pub fn local_version_eq(a: &[Time], b: &[Time]) -> bool {
    // Almost all branches only have one element in them.
    debug_assert_frontier_sorted(a);
    debug_assert_frontier_sorted(b);
    a == b
    // a.len() == b.len() && ((a.len() == 1 && a[0] == b[0]) || {
    //     a.iter().all(|o| b.contains(o))
    // })
}

#[allow(unused)]
pub fn local_version_is_root(branch: &[Time]) -> bool {
    branch.is_empty()
    // branch.len() == 1 && branch[0] == ROOT_TIME
}

/// This method clones a version or parents vector. Its slightly faster and smaller than just
/// calling v.clone() directly.
#[inline]
pub fn clone_smallvec<T, const LEN: usize>(v: &SmallVec<[T; LEN]>) -> SmallVec<[T; LEN]> where T: Clone + Copy {
    // This is now smaller again as of rust 1.60. Looks like the problem was fixed.
    v.clone()

    // if v.spilled() { // Unlikely. If only there was a stable rust intrinsic for this..
    //     v.clone()
    // } else {
    //     unsafe {
    //         // We only need to copy v.len() items, because LEN is small (2, usually) its actually
    //         // faster & less code to just copy the bytes in all cases rather than branch.
    //         // let mut arr: MaybeUninit<[T; LEN]> = MaybeUninit::uninit();
    //         // std::ptr::copy_nonoverlapping(v.as_ptr(), arr.as_mut_ptr().cast(), LEN);
    //         // SmallVec::from_buf_and_len_unchecked(arr, v.len())
    //
    //         let mut result: MaybeUninit<SmallVec<[T; LEN]>> = MaybeUninit::uninit();
    //         std::ptr::copy_nonoverlapping(v, result.as_mut_ptr(), 1);
    //         result.assume_init()
    //     }
    // }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;

    use crate::LocalVersion;
    use crate::causalgraph::parents::ParentsEntryInternal;

    use super::*;

    #[test]
    fn frontier_movement_smoke_tests() {
        let mut branch: LocalVersion = smallvec![];
        advance_frontier_by_known_run(&mut branch, &[], (0..10).into());
        assert_eq!(branch.as_slice(), &[9]);

        let history = Parents::from_entries(&[
            ParentsEntryInternal {
                span: (0..10).into(), shadow: usize::MAX,
                parents: smallvec![],
                child_indexes: smallvec![]
            }
        ]);

        retreat_frontier_by(&mut branch, &history, (5..10).into());
        assert_eq!(branch.as_slice(), &[4]);

        retreat_frontier_by(&mut branch, &history, (0..5).into());
        assert!(branch.is_empty());
    }

    #[test]
    fn frontier_stays_sorted() {
        let history = Parents::from_entries(&[
            ParentsEntryInternal {
                span: (0..2).into(), shadow: usize::MAX,
                parents: smallvec![],
                child_indexes: smallvec![]
            },
            ParentsEntryInternal {
                span: (2..6).into(), shadow: usize::MAX,
                parents: smallvec![0],
                child_indexes: smallvec![]
            },
            ParentsEntryInternal {
                span: (6..50).into(), shadow: 6,
                parents: smallvec![0],
                child_indexes: smallvec![]
            },
        ]);

        let mut branch: LocalVersion = smallvec![1, 10];
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
