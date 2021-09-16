use crate::list::{Branch, Order};
use crate::rle::RleVec;
use crate::list::txn::TxnSpan;
use crate::order::OrderSpan;

/// Advance branch frontier by a transaction. This is written creating a new branch, which is
/// somewhat inefficient (especially if the frontier is spilled).
pub(crate) fn advance_branch_by_known(branch: &mut Branch, txn_parents: &[Order], span: OrderSpan) {
    // TODO: Check the branch contains everything in txn_parents, but not txn_id:
    // Check the operation fits. The operation should not be in the branch, but
    // all the operation's parents should be.
    // From braid-kernel:
    // assert(!branchContainsVersion(db, order, branch), 'db already contains version')
    // for (const parent of op.parents) {
    //    assert(branchContainsVersion(db, parent, branch), 'operation in the future')
    // }
    assert!(!branch.contains(&span.order)); // Remove this when branch_contains_version works.

    // TODO: Consider sorting the branch after we do this.
    branch.retain(|o| !txn_parents.contains(o)); // Usually removes all elements.
    branch.push(span.last());
}

pub(crate) fn advance_branch(branch: &mut Branch, history: &RleVec<TxnSpan>, span: OrderSpan) {
    let txn = history.find(span.order).unwrap();
    if let Some(parent) = txn.parent_at_order(span.order) {
        advance_branch_by_known(branch, &[parent], span);
    } else {
        advance_branch_by_known(branch, &txn.parents, span);
    }
}

// TODO: Consider making this function take an OrderSpan instead of first_order / len pair.
pub(crate) fn retreat_branch_by(branch: &mut Branch, history: &RleVec<TxnSpan>, first_order: Order, len: u32) {
    let txn = history.find(first_order).unwrap();
    retreat_branch_known_txn(branch, history, txn, first_order, len);
}

pub(crate) fn retreat_branch_known_txn(branch: &mut Branch, history: &RleVec<TxnSpan>, txn: &TxnSpan, first_order: Order, len: u32) {
    let last_order = first_order + len - 1;
    let idx = branch.iter().position(|&e| e == last_order).unwrap();

    // Now add back any parents.
    debug_assert!(txn.contains(last_order));

    if first_order > txn.order {
        branch[idx] = first_order - 1;
    } else if first_order == txn.order {
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
    use crate::list::{Branch, ROOT_ORDER};
    use smallvec::smallvec;
    use super::*;

    #[test]
    fn branch_movement_smoke_tests() {
        let mut branch: Branch = smallvec![ROOT_ORDER];
        advance_branch_by_known(&mut branch, &[ROOT_ORDER], OrderSpan {
            order: 0, len: 10
        });
        assert_eq!(branch.as_slice(), &[9]);

        let txns = RleVec(vec![
            TxnSpan {
                order: 0, len: 10, shadow: ROOT_ORDER,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![]
            }
        ]);

        retreat_branch_by(&mut branch, &txns, 5, 5);
        assert_eq!(branch.as_slice(), &[4]);

        retreat_branch_by(&mut branch, &txns, 0, 5);
        assert_eq!(branch.as_slice(), &[ROOT_ORDER]);
    }
}