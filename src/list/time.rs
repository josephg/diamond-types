use crate::list::{ListCRDT, Branch, ROOT_ORDER, Order};
use crate::order::OrderSpan;
use smallvec::{SmallVec, smallvec};
use std::collections::BinaryHeap;
use crate::rle::AppendRLE;

/// This file contains tools to manage the document as a time dag. Specifically, tools to tell us
/// about branches, find diffs and move between branches.
impl ListCRDT {
    fn shadow_of(&self, order: Order) -> Order {
        assert_ne!(order, ROOT_ORDER); // The root doesn't have a shadow at all.
        let (txn, _offset) = self.txns.find(order).unwrap();
        txn.shadow
    }

    // Returns (spans only in a, spans only in b).
    pub(crate) fn diff(&self, a: &Branch, b: &Branch) -> (SmallVec<[OrderSpan; 4]>, SmallVec<[OrderSpan; 4]>) {
        assert!(a.len() > 0);
        assert!(b.len() > 0);

        // First some simple short circuit checks to avoid needless work in common cases.
        // Note most of the time this method is called, one of these early short circuit cases will
        // fire.
        if a == b { return (smallvec![], smallvec![]); }
        // let root_branch: Branch = smallvec![ROOT_ORDER];
        if a.as_slice() == &[ROOT_ORDER] {
            // TODO: b or b + 1?
            let max_b = b.iter().max().unwrap();
            return (smallvec![], smallvec![OrderSpan {order: 0, len: max_b + 1}]);
        }
        if b.as_slice() == &[ROOT_ORDER] {
            let max_a = a.iter().max().unwrap();
            return (smallvec![OrderSpan {order: 0, len: max_a + 1}], smallvec![]);
        }

        if a.len() == 1 && b.len() == 1 {
            // Check if either operation naively dominates the other. We could do this for more
            // cases, but we may as well use the code below instead.
            let a = a[0];
            let b = b[0];
            if a < b && self.shadow_of(b) <= a {
                return (smallvec![], smallvec![OrderSpan {order: a + 1, len: b - a}]);
            }
            if b < a && self.shadow_of(a) <= b {
                return (smallvec![OrderSpan {order: b + 1, len: a - b}], smallvec![]);
            }
        }

        // Otherwise fall through to the slow version.
        self.diff_slow(a, b)
    }

    // Split out for testing.
    fn diff_slow(&self, a: &Branch, b: &Branch) -> (SmallVec<[OrderSpan; 4]>, SmallVec<[OrderSpan; 4]>) {
        // We need to tag each entry in the queue based on whether its part of a's history or b's
        // history or both, and do so without changing the sort order for the heap.
        //
        // The reason we need to keep around both items is because we need to burn
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
            target.append_reversed_rle(OrderSpan { order: ord_start, len: ord_end - ord_start + 1});
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

            let (containing_txn, _offset) = self.txns.find(ord).unwrap();

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

            // 2: Mark the rest of the txn in our current color and repeat.
            if ord > containing_txn.order {
                mark_run(containing_txn.order, ord, flag);
            }

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

#[cfg(test)]
mod test {
    use crate::list::{ListCRDT, Branch, ROOT_ORDER};
    use crate::order::OrderSpan;
    use smallvec::smallvec;

    fn assert_diff_eq(doc: &ListCRDT, a: &Branch, b: &Branch, expect_a: &[OrderSpan], expect_b: &[OrderSpan]) {
        let slow_result = doc.diff_slow(a, b);
        let fast_result = doc.diff(a, b);
        assert_eq!(slow_result, fast_result);

        assert_eq!(slow_result.0.as_slice(), expect_a);
        assert_eq!(slow_result.1.as_slice(), expect_b);
    }

    #[test]
    fn diff_smoke_test() {
        let mut doc1 = ListCRDT::new();
        assert_diff_eq(&doc1, &doc1.frontier, &doc1.frontier, &[], &[]);

        doc1.get_or_create_agent_id("a".into());
        doc1.local_insert(0, 0, "S".into()); // Shared history.

        let mut doc2 = ListCRDT::new();
        doc2.get_or_create_agent_id("b".into());
        doc1.replicate_into(&mut doc2); // "S".

        // Ok now make some concurrent history.
        doc1.local_insert(0, 1, "aaa".into());
        let b1 = doc1.frontier.clone();

        assert_diff_eq(&doc1, &b1, &b1, &[], &[]);
        assert_diff_eq(&doc1, &smallvec![ROOT_ORDER], &smallvec![ROOT_ORDER], &[], &[]);
        // dbg!(&doc1.frontier);

        // There are 4 items in doc1 - "Saaa". Frontier is [3].
        // dbg!(&doc1.frontier);
        assert_diff_eq(&doc1, &smallvec![1], &smallvec![3], &[], &[OrderSpan {
            order: 2,
            len: 2
        }]);

        doc2.local_insert(0, 1, "bbb".into());

        doc2.replicate_into(&mut doc1);

        // doc1 has "Saaabbb".

        // dbg!(doc1.diff(&b1, &doc1.frontier));

        assert_diff_eq(&doc1, &b1, &doc1.frontier, &[], &[OrderSpan {
            order: 4,
            len: 3
        }]);

        assert_diff_eq(&doc1, &smallvec![3], &smallvec![6], &[OrderSpan {
            order: 1,
            len: 3
        }], &[OrderSpan {
            order: 4,
            len: 3
        }]);

        assert_diff_eq(&doc1, &smallvec![2], &smallvec![5], &[OrderSpan {
            order: 1,
            len: 2
        }], &[OrderSpan {
            order: 4,
            len: 2
        }]);

        // doc1.replicate_into(&mut doc2); // Also "Saaabbb" but different txns.
        // dbg!(&doc1.txns, &doc2.txns);
    }
}