use std::ops::Range;

use crate::list::{Branch, Order, ROOT_ORDER};
use crate::list::txn::TxnSpan;
use crate::rangeextra::OrderRange;
use crate::rle::RleVec;

/// Advance branch frontier by a transaction. This is written creating a new branch, which is
/// somewhat inefficient (especially if the frontier is spilled).
pub(crate) fn advance_branch_by_known(branch: &mut Branch, txn_parents: &[Order], range: Range<Order>) {
    // TODO: Check the branch contains everything in txn_parents, but not txn_id:
    // Check the operation fits. The operation should not be in the branch, but
    // all the operation's parents should be.
    // From braid-kernel:
    // assert(!branchContainsVersion(db, order, branch), 'db already contains version')
    // for (const parent of op.parents) {
    //    assert(branchContainsVersion(db, parent, branch), 'operation in the future')
    // }
    assert!(!branch.contains(&range.start)); // Remove this when branch_contains_version works.

    // TODO: Consider sorting the branch after we do this.
    branch.retain(|o| !txn_parents.contains(o)); // Usually removes all elements.
    branch.push(range.last_order());
}

pub(crate) fn advance_branch(branch: &mut Branch, history: &RleVec<TxnSpan>, range: Range<Order>) {
    let txn = history.find(range.start).unwrap();
    if let Some(parent) = txn.parent_at_order(range.start) {
        advance_branch_by_known(branch, &[parent], range);
    } else {
        advance_branch_by_known(branch, &txn.parents, range);
    }
}

// TODO: Change this to take a range instead of first_order / len pair.
pub(crate) fn retreat_branch_by(branch: &mut Branch, history: &RleVec<TxnSpan>, range: Range<Order>) {
    let txn = history.find(range.start).unwrap();
    retreat_branch_known_txn(branch, history, txn, range);
}

pub(crate) fn retreat_branch_known_txn(branch: &mut Branch, history: &RleVec<TxnSpan>, txn: &TxnSpan, range: Range<Order>) {
    let last_order = range.last_order();
    let idx = branch.iter().position(|&e| e == last_order).unwrap();

    // Now add back any parents.
    debug_assert!(txn.contains(last_order));

    if range.start > txn.order {
        branch[idx] = range.start - 1;
    } else if range.start == txn.order {
        branch.swap_remove(idx);
        for &parent in &txn.parents {
            // TODO: This is pretty inefficient. We're calling branch_contains_order in a loop and
            // each call to branch_contains_version does a call to history.find() in turn for each
            // item in branch.
            if branch.is_empty() || !history.branch_contains_order(branch, parent) {
                branch.push(parent);
            }
        }
    } else {
        // Is this something worth implementing?
        unimplemented!("retreat_branch cannot retreat by more than one transaction");
    }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;

    use crate::list::{Branch, ROOT_ORDER};

    use super::*;

    #[test]
    fn branch_movement_smoke_tests() {
        let mut branch: Branch = smallvec![ROOT_ORDER];
        advance_branch_by_known(&mut branch, &[ROOT_ORDER], 0..10);
        assert_eq!(branch.as_slice(), &[9]);

        let txns = RleVec(vec![
            TxnSpan {
                order: 0, len: 10, shadow: ROOT_ORDER,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![]
            }
        ]);

        retreat_branch_by(&mut branch, &txns, 5..10);
        assert_eq!(branch.as_slice(), &[4]);

        retreat_branch_by(&mut branch, &txns, 0..5);
        assert_eq!(branch.as_slice(), &[ROOT_ORDER]);
    }
}

pub fn branch_eq(a: &[Order], b: &[Order]) -> bool {
    // Almost all branches only have one element in them. But it would be cleaner to keep branches
    // sorted.
    a.len() == b.len() && ((a.len() == 1 && a[0] == b[0]) || {
        a.iter().all(|o| b.contains(o))
    })
}

pub fn branch_is_root(branch: &[Order]) -> bool {
    branch.len() == 1 && branch[0] == ROOT_ORDER
}