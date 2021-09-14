use crate::list::{ListCRDT, ROOT_ORDER, Order};
use crate::order::OrderSpan;
use smallvec::{SmallVec, smallvec};
use std::collections::BinaryHeap;
use crate::rle::AppendRLE;
use crate::list::doc::notify_for;
use rle::splitable_span::SplitableSpan;
use crate::entry::Searchable;
// use smartstring::alias::{String as SmartString};

struct LinearIter<'a> {
    list: &'a mut ListCRDT,
    span: OrderSpan,
}

impl<'a> Iterator for LinearIter<'a> {
    type Item = ();

    fn next(&mut self) -> Option<Self::Item> {
        if self.span.len > 0 {
            unsafe {
                let len = self.list.partially_reapply_change(&mut self.span);
                self.span.truncate_keeping_right(len as usize);
            }
            Some(())
        } else {
            None
        }
    }
}

// #[derive(Debug, PartialEq, Eq, Clone, Default)]
// pub struct OpComponent {
//     skip: u32,
//     del: u32,
//     ins: SmartString,
// }


/// This file contains tools to manage the document as a time dag. Specifically, tools to tell us
/// about branches, find diffs and move between branches.
impl ListCRDT {
    fn shadow_of(&self, order: Order) -> Order {
        if order == ROOT_ORDER {
            ROOT_ORDER
        } else {
            let (txn, _offset) = self.txns.find(order).unwrap();
            txn.shadow
        }
    }

    /// Does the frontier `[a]` contain `[b]` as a direct ancestor according to its shadow?
    fn txn_shadow_contains(&self, a: Order, b: Order) -> bool {
        let a_1 = a.wrapping_add(1);
        let b_1 = b.wrapping_add(1);
        a_1 == b_1 || (a_1 > b_1 && self.shadow_of(a).wrapping_add(1) <= b_1)
    }

    /// Returns (spans only in a, spans only in b). Spans are in reverse (descending) order.
    pub(crate) fn diff(&self, a: &[Order], b: &[Order]) -> (SmallVec<[OrderSpan; 4]>, SmallVec<[OrderSpan; 4]>) {
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
                return (smallvec![OrderSpan {order: b + 1, len: a - b}], smallvec![]);
            }
            if self.txn_shadow_contains(b, a) {
                return (smallvec![], smallvec![OrderSpan {order: a + 1, len: b - a}]);
            }
        }

        // Otherwise fall through to the slow version.
        self.diff_slow(a, b)
    }

    // Split out for testing.
    fn diff_slow(&self, a: &[Order], b: &[Order]) -> (SmallVec<[OrderSpan; 4]>, SmallVec<[OrderSpan; 4]>) {
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
            target.push_reversed_rle(OrderSpan { order: ord_start, len: ord_end - ord_start + 1});
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

    /// Safety: This method only unapplies changes to the internal indexes. It does not update
    /// other metadata. Calling doc.check() after this will fail.
    /// Also the passed span is not checked, and must be valid with respect to what else has been
    /// applied / unapplied.
    // #[deprecated(note="Moving this logic into OT code")]
    pub(super) unsafe fn partially_unapply_changes(&mut self, mut span: OrderSpan) {
        while span.len > 0 {
            // Note: This sucks, but we obviously ("obviously") have to unapply the span backwards.
            // So instead of searching for span.offset, we start with span.offset + span.len - 1.

            // First check if the change was a delete or an insert.
            let span_last_order = span.end() - 1;
            if let Some((d, d_offset)) = self.deletes.find(span_last_order) {
                // Its a delete. We need to try to undelete the item, unless the item was deleted
                // multiple times (in which case, it stays deleted.)
                let mut base = u32::max(span.order, d.0);
                let mut undelete_here = span_last_order + 1 - base;
                debug_assert!(undelete_here > 0);

                // d_offset -= span_last_order - base; // equivalent to d_offset -= undelete_here - 1;

                // Ok, undelete here. There's two approaches this implementation could take:
                // 1. Undelete backwards from base + len_here - 1, undeleting as much as we can.
                //    Rely on the outer loop to iterate to the next section
                // 2. Undelete len_here items. The order that we undelete an item in doesn't matter,
                //    so although this approach needs another inner loop, we can go through this
                //    range forwards. This makes the logic simpler, but longer.

                // I'm going with 2.
                span.len -= undelete_here; // equivalent to span.len = base - span.order;

                while undelete_here > 0 {
                    let mut len_here = self.double_deletes.find_zero_range(base, undelete_here);

                    if len_here == 0 { // Unlikely.
                        // We're looking at an item which has been deleted multiple times. Decrement
                        // the deleted count by 1 in double_deletes and advance.
                        let len_dd_here = self.double_deletes.decrement_delete_range(base, undelete_here);
                        debug_assert!(len_dd_here > 0);

                        // What a minefield. O_o
                        undelete_here -= len_dd_here;
                        base += len_dd_here;

                        if undelete_here == 0 { break; } // The entire range was undeleted.

                        len_here = self.double_deletes.find_zero_range(base, undelete_here);
                        debug_assert!(len_here > 0);
                    }

                    // Ok now undelete from the range tree.
                    let base_item = d.1.order + d_offset + 1 - undelete_here;
                    // dbg!(base_item, d.1.order, d_offset, undelete_here, base);
                    let cursor = self.get_cursor_before(base_item);
                    let (len_here, succeeded) = self.range_tree.remote_reactivate(cursor, len_here as _, notify_for(&mut self.index));
                    assert!(succeeded); // If they're active in the content_tree, we're in trouble.
                    undelete_here -= len_here as u32;
                }
            } else {
                // The operation was an insert operation, not a delete operation. Mark as
                // deactivated.
                let mut cursor = self.get_cursor_before(span_last_order);
                cursor.offset += 1; // Dirty. Essentially get_cursor_after(span_last_order) without rolling over.

                // Check how much we can reactivate in one go.
                // let base = u32::min(span.order, span_last_order + 1 - cursor.offset);
                let len_here = u32::min(span.len, cursor.offset as _); // usize? u32? blehh
                debug_assert_ne!(len_here, 0);
                // let base = span_last_order + 1 - len_here; // not needed.
                // let base = u32::max(span.order, span_last_order + 1 - cursor.offset);
                // dbg!(&cursor, len_here);
                cursor.offset -= len_here as usize;

                let (deleted_here, succeeded) = self.range_tree.remote_deactivate(cursor, len_here as _, notify_for(&mut self.index));
                // let len_here = deleted_here as u32;
                debug_assert_eq!(deleted_here, len_here as usize);
                // Deletes of an item have to be chronologically after any insert of that same item.
                // By the time we've gotten to unwinding an insert, all the deletes must be cleared
                // out.
                assert!(succeeded);
                span.len -= len_here;
            }
        }
    }

    pub(super) unsafe fn partially_reapply_change(&mut self, span: &OrderSpan) -> u32 {
        // First check if the change was a delete or an insert.
        if let Some((d, d_offset)) = self.deletes.find(span.order) {
            // Re-delete the item.
            let delete_here = u32::min(span.len, d.1.len - d_offset);
            debug_assert!(delete_here > 0);

            // Note the order in span is the order of the *delete*, not the order of the item
            // being deleted.
            let del_target_order = d.at_offset(d_offset as usize) as u32;

            let (delete_here, _) = self.internal_mark_deleted(del_target_order, delete_here, false);
            debug_assert!(delete_here > 0);
            // span.truncate_keeping_right(delete_here as usize);
            delete_here
        } else {
            // The operation was an insert operation. Re-insert the content.
            let cursor = self.get_cursor_before(span.order);
            let (ins_here, succeeded) = self.range_tree.remote_reactivate(cursor, span.len as _, notify_for(&mut self.index));
            assert!(succeeded); // If they're active in the content_tree, we're in trouble.
            debug_assert!(ins_here > 0);
            // span.truncate_keeping_right(ins_here);
            ins_here as u32
        }
    }

    /// Pair of partially_unapply_changes. After changes are unapplied and reapplied, the document
    /// contents should be identical.
    ///
    /// Safety: This method only unapplies changes to the internal indexes. It does not update
    /// other metadata. Calling doc.check() after this will fail. Also the passed span is not
    /// checked, and must be valid with respect to what else has been applied / unapplied.
    pub(super) unsafe fn partially_reapply_changes(&mut self, mut span: OrderSpan) {
        while span.len > 0 {
            let len = self.partially_reapply_change(&mut span);
            span.truncate_keeping_right(len as usize);
        }
    }

    pub fn num_ops(&self) -> Order {
        self.get_next_order()
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
    use crate::order::OrderSpan;
    use smallvec::smallvec;
    use crate::list::external_txn::{RemoteTxn, RemoteId, RemoteCRDTOp};

    fn assert_diff_eq(doc: &ListCRDT, a: &[Order], b: &[Order], expect_a: &[OrderSpan], expect_b: &[OrderSpan]) {
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

        doc1.get_or_create_agent_id("a");
        doc1.local_insert(0, 0, "S".into()); // Shared history.

        let mut doc2 = ListCRDT::new();
        doc2.get_or_create_agent_id("b");
        doc1.replicate_into(&mut doc2); // "S".

        // Ok now make some concurrent history.
        doc1.local_insert(0, 1, "aaa".into());
        let b1 = doc1.frontier.clone();

        assert_diff_eq(&doc1, &b1, &b1, &[], &[]);
        assert_diff_eq(&doc1, &[ROOT_ORDER], &[ROOT_ORDER], &[], &[]);
        // dbg!(&doc1.frontier);

        // There are 4 items in doc1 - "Saaa".
        // dbg!(&doc1.frontier); // [3]
        assert_diff_eq(&doc1, &[1], &[3], &[], &[OrderSpan {
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

        assert_diff_eq(&doc1, &[3], &[6], &[OrderSpan {
            order: 1,
            len: 3
        }], &[OrderSpan {
            order: 4,
            len: 3
        }]);

        assert_diff_eq(&doc1, &[2], &[5], &[OrderSpan {
            order: 1,
            len: 2
        }], &[OrderSpan {
            order: 4,
            len: 2
        }]);

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

        assert_diff_eq(&doc, &[6], &[ROOT_ORDER], &[
           OrderSpan { order: 5, len: 2 },
           OrderSpan { order: 0, len: 3 },
        ], &[]);

        assert_diff_eq(&doc, &[6], &[4], &[
            OrderSpan { order: 5, len: 2 },
            OrderSpan { order: 0, len: 3 },
        ], &[
            OrderSpan { order: 3, len: 2 },
        ]);

        assert_diff_eq(&doc, &[4, 6], &[ROOT_ORDER], &[
            OrderSpan { order: 0, len: 7 },
        ], &[]);
    }

    #[test]
    fn unapply() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "aaaa".into()); // [0,4)
        doc.local_delete(0, 1, 2); // [4,6)
        dbg!(&doc);
        unsafe {
            // doc.partially_unapply_changes(OrderSpan { order: 4, len: 2 });
            doc.partially_unapply_changes(doc.linear_changes_since(0));
        }

        unsafe {
            doc.partially_reapply_changes(doc.linear_changes_since(0));
        }
        dbg!(&doc);
    }
}