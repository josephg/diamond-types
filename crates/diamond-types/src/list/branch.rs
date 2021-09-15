use crate::list::{Branch, Order};
use crate::rle::RleVec;
use crate::list::txn::TxnSpan;

/// Advance branch frontier by a transaction. This is written creating a new branch, which is
/// somewhat inefficient (especially if the frontier is spilled).
pub(crate) fn advance_branch_by(branch: &mut Branch, txn_parents: &[Order], first_order: Order, len: u32) {
    // TODO: Check the branch contains everything in txn_parents, but not txn_id:
    // Check the operation fits. The operation should not be in the branch, but
    // all the operation's parents should be.
    // From braid-kernel:
    // assert(!branchContainsVersion(db, order, branch), 'db already contains version')
    // for (const parent of op.parents) {
    //    assert(branchContainsVersion(db, parent, branch), 'operation in the future')
    // }
    assert!(!branch.contains(&first_order)); // Remove this when branch_contains_version works.

    // TODO: Consider sorting the branch after we do this.
    branch.retain(|o| !txn_parents.contains(o)); // Usually removes all elements.
    branch.push(first_order + len - 1);
}

pub(crate) fn retreat_branch_by(branch: &mut Branch, history: &RleVec<TxnSpan>, first_order: Order, len: u32) {
    let last_order = first_order + len - 1;
    let idx = branch.iter().position(|&e| e == last_order).unwrap();
    branch.swap_remove(idx);

    // Now add back any parents.
    let txn = history.find_packed(first_order).0;
    if first_order > txn.order {
        branch.push(first_order - 1);
    } else {
        for &parent in &txn.parents {
            if branch.is_empty() || !history.branch_contains_order(branch, parent) {
                branch.push(parent);
            }
        }
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
        advance_branch_by(&mut branch, &[ROOT_ORDER], 0, 10);
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