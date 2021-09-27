use crate::list::{ListCRDT, ROOT_ORDER, Order};
use crate::order::OrderSpan;
use smallvec::{SmallVec, smallvec};
use std::collections::BinaryHeap;
use rle::AppendRle;
use crate::rle::RleVec;
use crate::list::txn::TxnSpan;
use std::ops::Range;
// use smartstring::alias::{String as SmartString};


impl RleVec<TxnSpan> {
    fn shadow_of(&self, order: Order) -> Order {
        if order == ROOT_ORDER {
            ROOT_ORDER
        } else {
            self.find(order).unwrap().shadow
        }
    }

    /// Does the frontier `[a]` contain `[b]` as a direct ancestor according to its shadow?
    fn txn_shadow_contains(&self, a: Order, b: Order) -> bool {
        let a_1 = a.wrapping_add(1);
        let b_1 = b.wrapping_add(1);
        a_1 == b_1 || (a_1 > b_1 && self.shadow_of(a).wrapping_add(1) <= b_1)
    }

    pub(crate) fn branch_contains_order(&self, branch: &[Order], target: Order) -> bool {
        assert!(!branch.is_empty());
        if target == ROOT_ORDER || branch.contains(&target) { return true; }
        if branch == [ROOT_ORDER] { return false; }

        // Fast path. This causes extra calls to find_packed(), but you usually have a branch with
        // a shadow less than target. Usually the root document. And in that case this codepath
        // avoids the allocation from BinaryHeap.
        for &o in branch {
            if o > target {
                let txn = self.find(o).unwrap();
                if txn.shadow_contains(target) { return true; }
            }
        }

        // So I don't *need* to use a priority queue here. The options are:
        // 1. Use a priority queue, scanning from the highest to lowest orders
        // 2. Use a simple list and do DFS, potentially scanning some items twice
        // 3. Use a simple list and do DFS, with another structure to mark which items we've
        //    visited.
        //
        // Honestly any approach should be obnoxiously fast in any real editing session anyway.

        // TODO: Consider moving queue into a threadlocal variable so we don't need to reallocate it
        // with each call to branch_contains_order.
        let mut queue = BinaryHeap::new();

        // This code could be written to use parent_indexes but its a bit tricky, as an index isn't
        // enough specificity. We'd need the parent and the parent_index. Eh...
        for &o in branch {
            debug_assert_ne!(o, target);
            if o > target { queue.push(o); }
        }

        while let Some(order) = queue.pop() {
            debug_assert!(order > target);
            // dbg!((order, &queue));

            // TODO: Skip these calls to find() using parent_index.
            let txn = self.find(order).unwrap();
            if txn.shadow_contains(target) { return true; }

            while let Some(&next_order) = queue.peek() {
                if next_order >= txn.order {
                    // dbg!(next_order);
                    queue.pop();
                } else { break; }
            }

            // dbg!(order);
            for &p in &txn.parents {
                if p == target { return true; }
                else if p != ROOT_ORDER && p > target { queue.push(p); }
            }
        }

        false
    }

    /// Returns (spans only in a, spans only in b). Spans are in reverse (descending) order.
    pub(crate) fn diff(&self, a: &[Order], b: &[Order]) -> (SmallVec<[Range<Order>; 4]>, SmallVec<[Range<Order>; 4]>) {
        assert!(!a.is_empty());
        assert!(!b.is_empty());

        // First some simple short circuit checks to avoid needless work in common cases.
        // Note most of the time this method is called, one of these early short circuit cases will
        // fire.
        if a == b { return (smallvec![], smallvec![]); }

        if a.len() == 1 && b.len() == 1 {
            // Check if either operation naively dominates the other. We could do this for more
            // cases, but we may as well use the code below instead.
            let a = a[0];
            let b = b[0];
            if self.txn_shadow_contains(a, b) {
                return (smallvec![b.wrapping_add(1)..a.wrapping_add(1)], smallvec![]);
            }
            if self.txn_shadow_contains(b, a) {
                return (smallvec![], smallvec![a.wrapping_add(1)..b.wrapping_add(1)]);
            }
        }

        // Otherwise fall through to the slow version.
        self.diff_slow(a, b)
    }

    // Split out for testing.
    fn diff_slow(&self, a: &[Order], b: &[Order]) -> (SmallVec<[Range<Order>; 4]>, SmallVec<[Range<Order>; 4]>) {
        // We need to tag each entry in the queue based on whether its part of a's history or b's
        // history or both, and do so without changing the sort order for the heap.
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
        enum Flag { OnlyA, OnlyB, Shared }

        // Sorted highest to lowest.
        let mut queue: BinaryHeap<(Order, Flag)> = BinaryHeap::new();
        for a_ord in a {
            if *a_ord != ROOT_ORDER { queue.push((*a_ord, Flag::OnlyA)); }
        }
        for b_ord in b {
            if *b_ord != ROOT_ORDER { queue.push((*b_ord, Flag::OnlyB)); }
        }

        let mut num_shared_entries = 0;

        let mut only_a = smallvec![];
        let mut only_b = smallvec![];

        // marks range [ord_start..ord_end] *inclusive* with flag in our output.
        let mut mark_run = |ord_start, ord_end, flag: Flag| {
            let target = match flag {
                Flag::OnlyA => { &mut only_a }
                Flag::OnlyB => { &mut only_b }
                Flag::Shared => { return; }
            };
            // dbg!((ord_start, ord_end));
            target.push_reversed_rle(ord_start..ord_end+1);
        };

        while let Some((mut ord, mut flag)) = queue.pop() {
            if flag == Flag::Shared { num_shared_entries -= 1; }

            // dbg!((ord, flag));
            while let Some((peek_ord, peek_flag)) = queue.peek() {
                if *peek_ord != ord { break; } // Normal case.
                else {
                    // 3 cases if peek_flag != flag. We set flag = Shared in all cases.
                    if *peek_flag != flag { flag = Flag::Shared; }
                    if *peek_flag == Flag::Shared { num_shared_entries -= 1; }
                    queue.pop();
                }
            }

            // Grab the txn containing ord. This will usually be at prev_txn_idx - 1.
            // TODO: Remove usually redundant binary search

            let (containing_txn, _offset) = self.find_with_offset(ord).unwrap();

            // There's essentially 2 cases here:
            // 1. This item and the first item in the queue are part of the same txn. Mark down to
            //    the queue head and continue.
            // 2. Its not. Mark the whole txn and queue parents.

            // 1:
            while let Some((peek_ord, peek_flag)) = queue.peek() {
                // dbg!((peek_ord, peek_flag));
                if *peek_ord < containing_txn.order { break; }
                else {
                    if *peek_flag != flag {
                        // Mark from peek_ord..ord and continue.
                        mark_run(*peek_ord + 1, ord, flag);
                        ord = *peek_ord;
                        // offset -= ord - peek_ord;
                        flag = Flag::Shared;
                    }
                    if *peek_flag == Flag::Shared { num_shared_entries -= 1; }
                    queue.pop();
                }
            }

            // 2: Mark the rest of the txn in our current color and repeat. Note we still need to
            // mark the run even if ord == containing_txn.order because the spans are inclusive.
            mark_run(containing_txn.order, ord, flag);

            for p in containing_txn.parents.iter() {
                if *p != ROOT_ORDER {
                    queue.push((*p, flag));
                    if flag == Flag::Shared { num_shared_entries += 1; }
                }
            }

            // If there's only shared entries left, abort.
            if queue.len() == num_shared_entries { break; }
        }

        // dbg!(&queue);

        (only_a, only_b)
    }
}

/// This file contains tools to manage the document as a time dag. Specifically, tools to tell us
/// about branches, find diffs and move between branches.
impl ListCRDT {
    // Exported for the fuzzer. Not sure if I actually want this exposed.
    pub fn branch_contains_order(&self, branch: &[Order], target: Order) -> bool {
        self.txns.branch_contains_order(branch, target)
    }

    pub fn linear_changes_since(&self, order: Order) -> OrderSpan {
        OrderSpan {
            order,
            len: self.get_next_order() - order
        }
    }
}

#[cfg(test)]
pub mod test {
    use crate::list::{ListCRDT, ROOT_ORDER, Order};
    use smallvec::smallvec;
    use crate::list::external_txn::{RemoteTxn, RemoteId, RemoteCRDTOp};
    use crate::list::txn::TxnSpan;
    use crate::rle::RleVec;
    use std::ops::Range;
    use crate::rangeextra::OrderRange;

    fn assert_diff_eq(txns: &RleVec<TxnSpan>, a: &[Order], b: &[Order], expect_a: &[Range<Order>], expect_b: &[Range<Order>]) {
        let slow_result = txns.diff_slow(a, b);
        let fast_result = txns.diff(a, b);
        assert_eq!(slow_result, fast_result);

        assert_eq!(slow_result.0.as_slice(), expect_a);
        assert_eq!(slow_result.1.as_slice(), expect_b);

        for &(branch, spans, other) in &[(a, expect_a, b), (b, expect_b, a)] {
            for o in spans {
                assert!(txns.branch_contains_order(branch, o.start));
                assert!(txns.branch_contains_order(branch, o.last_order()));
            }

            if branch.len() == 1 {
                // dbg!(&other, branch[0]);
                let expect = spans.is_empty();
                assert_eq!(expect, txns.branch_contains_order(other, branch[0]));
            }
        }
    }

    #[test]
    fn branch_contains_smoke_test() {
        // let mut doc = ListCRDT::new();
        // assert!(doc.txns.branch_contains_order(&doc.frontier, ROOT_ORDER));
        //
        // doc.get_or_create_agent_id("a");
        // doc.local_insert(0, 0, "S".into()); // Shared history.
        // assert!(doc.txns.branch_contains_order(&doc.frontier, ROOT_ORDER));
        // assert!(doc.txns.branch_contains_order(&doc.frontier, 0));
        // assert!(!doc.txns.branch_contains_order(&[ROOT_ORDER], 0));

        let txns = RleVec(vec![
            TxnSpan { // 0-2
                order: 0, len: 3, shadow: 0,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![2, 3],
            },
            TxnSpan { // 3-5
                order: 3, len: 3, shadow: 3,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![2],
            },
            TxnSpan { // 6-8
                order: 6, len: 3, shadow: 6,
                parents: smallvec![1, 4],
                parent_indexes: smallvec![0, 1], child_indexes: smallvec![3],
            },
            TxnSpan { // 9
                order: 9, len: 1, shadow: ROOT_ORDER,
                parents: smallvec![8, 2],
                parent_indexes: smallvec![2, 0], child_indexes: smallvec![],
            },
        ]);

        assert!(txns.branch_contains_order(&[ROOT_ORDER], ROOT_ORDER));
        assert!(txns.branch_contains_order(&[0], 0));
        assert!(txns.branch_contains_order(&[0], ROOT_ORDER));

        assert!(txns.branch_contains_order(&[2], 0));
        assert!(txns.branch_contains_order(&[2], 1));
        assert!(txns.branch_contains_order(&[2], 2));

        assert!(!txns.branch_contains_order(&[0], 1));
        assert!(!txns.branch_contains_order(&[1], 2));

        assert!(txns.branch_contains_order(&[8], 0));
        assert!(txns.branch_contains_order(&[8], 1));
        assert!(!txns.branch_contains_order(&[8], 2));
        assert!(!txns.branch_contains_order(&[8], 5));

        assert!(txns.branch_contains_order(&[1,4], 0));
        assert!(txns.branch_contains_order(&[1,4], 1));
        assert!(!txns.branch_contains_order(&[1,4], 2));
        assert!(!txns.branch_contains_order(&[1,4], 5));

        assert!(txns.branch_contains_order(&[9], 2));
        assert!(txns.branch_contains_order(&[9], 1));
        assert!(txns.branch_contains_order(&[9], 0));
    }

    #[test]
    fn diff_smoke_test() {
        let mut doc1 = ListCRDT::new();
        assert_diff_eq(&doc1.txns, &doc1.frontier, &doc1.frontier, &[], &[]);

        doc1.get_or_create_agent_id("a");
        doc1.local_insert(0, 0, "S".into()); // Shared history.

        let mut doc2 = ListCRDT::new();
        doc2.get_or_create_agent_id("b");
        doc1.replicate_into(&mut doc2); // "S".

        // Ok now make some concurrent history.
        doc1.local_insert(0, 1, "aaa".into());
        let b1 = doc1.frontier.clone();

        assert_diff_eq(&doc1.txns, &b1, &b1, &[], &[]);
        assert_diff_eq(&doc1.txns, &[ROOT_ORDER], &[ROOT_ORDER], &[], &[]);
        // dbg!(&doc1.frontier);

        // There are 4 items in doc1 - "Saaa".
        // dbg!(&doc1.frontier); // [3]
        assert_diff_eq(&doc1.txns, &[1], &[3], &[], &[2..4]);

        doc2.local_insert(0, 1, "bbb".into());

        doc2.replicate_into(&mut doc1);

        // doc1 has "Saaabbb".

        // dbg!(doc1.diff(&b1, &doc1.frontier));

        assert_diff_eq(&doc1.txns, &b1, &doc1.frontier, &[], &[4..7]);
        assert_diff_eq(&doc1.txns, &[3], &[6], &[1..4], &[4..7]);
        assert_diff_eq(&doc1.txns, &[2], &[5], &[1..3], &[4..6]);

        // doc1.replicate_into(&mut doc2); // Also "Saaabbb" but different txns.
        // dbg!(&doc1.txns, &doc2.txns);
    }

    fn root_id() -> RemoteId {
        RemoteId {
            agent: "ROOT".into(),
            seq: u32::MAX
        }
    }

    pub fn complex_multientry_doc() -> ListCRDT {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("a");
        doc.get_or_create_agent_id("b");

        assert_eq!(doc.frontier.as_slice(), &[ROOT_ORDER]);

        doc.local_insert(0, 0, "aaa".into());

        assert_eq!(doc.frontier.as_slice(), &[2]);

        // Need to do this manually to make the change concurrent.
        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "b".into(), seq: 0 },
            parents: smallvec![root_id()],
            ops: smallvec![RemoteCRDTOp::Ins {
                origin_left: root_id(),
                origin_right: root_id(),
                len: 2,
                content_known: true,
            }],
            ins_content: "bb".into(),
        });

        assert_eq!(doc.frontier.as_slice(), &[2, 4]);

        // And need to do this manually to make the change not merge time.
        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "a".into(), seq: 3 },
            parents: smallvec![RemoteId { agent: "a".into(), seq: 2 }],
            ops: smallvec![RemoteCRDTOp::Ins {
                origin_left: RemoteId { agent: "a".into(), seq: 2 },
                origin_right: root_id(),
                len: 2,
                content_known: true,
            }],
            ins_content: "AA".into(),
        });

        assert_eq!(doc.frontier.as_slice(), &[4, 6]);

        if let Some(ref text) = doc.text_content {
            assert_eq!(text, "aaaAAbb");
        }

        doc
    }

    #[test]
    fn diff_with_multiple_entries() {
        let doc = complex_multientry_doc();

        // dbg!(&doc.txns);
        // dbg!(doc.diff(&smallvec![6], &smallvec![ROOT_ORDER]));
        // dbg!(&doc);

        assert_diff_eq(&doc.txns, &[6], &[ROOT_ORDER], &[5..7, 0..3], &[]);
        assert_diff_eq(&doc.txns, &[6], &[4], &[5..7, 0..3], &[3..5]);
        assert_diff_eq(&doc.txns, &[4, 6], &[ROOT_ORDER], &[0..7], &[]);
    }

    #[test]
    fn diff_for_flat_txns() {
        // Regression.
        let history = RleVec(vec![
            TxnSpan {
                order: 0, len: 1, shadow: ROOT_ORDER,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![2]
            },
            TxnSpan {
                order: 1, len: 1, shadow: ROOT_ORDER,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![3]
            },
            TxnSpan {
                order: 2, len: 1, shadow: 2,
                parents: smallvec![0],
                parent_indexes: smallvec![0], child_indexes: smallvec![4]
            },
        ]);

        assert_diff_eq(&history, &[2], &[ROOT_ORDER], &[2..3, 0..1], &[]);
    }

    #[test]
    fn diff_three_root_txns() {
        // Regression.
        let history = RleVec(vec![
            TxnSpan {
                order: 0,
                len: 1,
                shadow: ROOT_ORDER,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![],
            },
            TxnSpan {
                order: 1,
                len: 1,
                shadow: 1,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![],
            },
            TxnSpan {
                order: 2,
                len: 1,
                shadow: 2,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![],
            },
        ]);

        assert_diff_eq(&history, &[0], &[ROOT_ORDER], &[0..1], &[]);
        assert_diff_eq(&history, &[ROOT_ORDER], &[0], &[], &[0..1]);
    }
}