use std::cmp::Ordering;
use std::ops::{Deref, DerefMut};
use std::ptr::{NonNull, null_mut};
use smallvec::{SmallVec, smallvec};
use content_tree::{ContentTreeRaw, ContentTreeWithIndex, Cursor, DEFAULT_IE, DEFAULT_LE, MutCursor, NodeLeaf, null_notify, UnsafeCursor};
use rle::{AppendRle, HasLength, Searchable, SplitableSpan, Trim};
use crate::list::{Branch, Checkout, ListCRDT, OpSet, Time};
use crate::list::m2::{DocRangeIndex, M2Tracker, SpaceIndex};
use crate::list::m2::yjsspan2::{YjsSpan2, YjsSpanState};
use crate::list::operation::{InsDelTag, Operation};
use crate::localtime::TimeSpan;
use crate::rle::{KVPair, RleSpanHelpers};
use crate::{AgentId, ROOT_TIME};
use crate::list::branch::{advance_branch_by, advance_branch_by_known, branch_eq, branch_is_sorted};
use crate::list::history::HistoryEntry;
use crate::list::history_tools::{ConflictZone, Flag};
use crate::list::history_tools::Flag::OnlyB;
use crate::list::list::apply_local_operation;
use crate::list::m2::delete::Delete;
use crate::list::m2::markers::MarkerEntry;
use crate::list::m2::txn_trace::OptimizedTxnsIter;

const ALLOW_FF: bool = true;

pub(super) fn notify_for(index: &mut SpaceIndex) -> impl FnMut(YjsSpan2, NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>>) + '_ {
    // |_entry: YjsSpan2, _leaf| {}
    move |entry: YjsSpan2, leaf| {
        let mut start = entry.id.start;
        let mut len = entry.len();

        let index_len = index.len() as usize;
        if start > index_len {
            // Insert extra dummy data to cover deletes.
            len += start - index_len;
            start = index_len;
        }

        index.replace_range_at_offset(start, MarkerEntry {
            ptr: Some(leaf), len
        });

        // index.replace_range(entry.order as usize, MarkerEntry {
        //     ptr: Some(leaf), len: entry.len() as u32
        // });
    }
}


impl M2Tracker {
    pub(crate) fn new() -> Self {
        let mut range_tree = ContentTreeWithIndex::new();
        let mut index = ContentTreeWithIndex::new();
        range_tree.push_notify(YjsSpan2::new_underwater(), notify_for(&mut index));

        Self {
            range_tree,
            index,
            deletes: Default::default()
        }
    }

    pub(crate) fn new_at_conflict(opset: &OpSet, conflict: ConflictZone) -> (Self, Branch) {
        let mut tracker = Self::new();

        // dbg!(branch_a, branch_b);
        let mut walker = opset.history.known_conflicting_txns_iter(conflict);
        while let Some(walk) = walker.next() {
            for range in walk.retreat {
                tracker.retreat_by_range(range);
            }

            for range in walk.advance_rev.into_iter().rev() {
                tracker.advance_by_range(range);
            }

            debug_assert!(!walk.consume.is_empty());
            tracker.apply_range(opset, walk.consume, None);
        }
        let branch = walker.into_branch();

        (tracker, branch)
    }

    fn marker_at(&self, time: Time) -> NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>> {
        let cursor = self.index.cursor_at_offset_pos(time, false);
        // Gross.
        cursor.get_item().unwrap().unwrap()
    }


    pub(crate) fn get_unsafe_cursor_before(&self, time: Time) -> UnsafeCursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
        if time == ROOT_TIME {
            // Or maybe we should just abort?
            self.range_tree.unsafe_cursor_at_end()
        } else {
            let marker = self.marker_at(time);
            unsafe {
                ContentTreeRaw::cursor_before_item(time, marker)
            }
        }
    }

    pub(crate) fn get_cursor_before(&self, time: Time) -> Cursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
        unsafe { Cursor::unchecked_from_raw(&self.range_tree, self.get_unsafe_cursor_before(time)) }
    }

    pub(super) fn get_unsafe_cursor_after(&self, time: Time, stick_end: bool) -> UnsafeCursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
        if time == ROOT_TIME {
            self.range_tree.unsafe_cursor_at_start()
        } else {
            let marker = self.marker_at(time);
            // let marker: NonNull<NodeLeaf<YjsSpan, ContentIndex>> = self.markers.at(order as usize).unwrap();
            // self.content_tree.
            let mut cursor = unsafe {
                ContentTreeRaw::cursor_before_item(time, marker)
            };
            // The cursor points to parent. This is safe because of guarantees provided by
            // cursor_before_item.
            cursor.offset += 1;
            if !stick_end { cursor.roll_to_next_entry(); }
            cursor
        }
    }

    pub(super) fn integrate(&mut self, opset: &OpSet, agent: AgentId, item: YjsSpan2, mut cursor: UnsafeCursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>) -> usize {
        assert!(item.len() > 0);

        // Ok now that's out of the way, lets integrate!
        cursor.roll_to_next_entry();

        // These are almost never used. Could avoid the clone here... though its pretty cheap.
        let left_cursor = cursor.clone();
        let mut scan_start = cursor.clone();
        let mut scanning = false;

        loop {
            let other_order = match unsafe { cursor.unsafe_get_item() } {
                None => { break; } // End of the document
                Some(o) => { o }
            };

            // Almost always true. Could move this short circuit earlier?
            if other_order == item.origin_right { break; }

            // This code could be better optimized, but its already O(n * log n), and its extremely
            // rare that you actually get concurrent inserts at the same location in the document
            // anyway.

            let other_entry = *cursor.get_raw_entry();
            // let other_order = other_entry.order + cursor.offset as u32;

            debug_assert_eq!(other_entry.state, YjsSpanState::NotInsertedYet);

            let other_left_order = other_entry.origin_left_at_offset(cursor.offset);
            let other_left_cursor = self.get_unsafe_cursor_after(other_left_order, false);

            // YjsMod semantics
            match unsafe { other_left_cursor.unsafe_cmp(&left_cursor) } {
                Ordering::Less => { break; } // Top row
                Ordering::Greater => {} // Bottom row. Continue.
                Ordering::Equal => {
                    if item.origin_right == other_entry.origin_right {
                        // Origin_right matches. Items are concurrent. Order by agent names.
                        let my_name = opset.get_agent_name(agent);
                        let other_loc = opset.client_with_localtime.get(other_order);
                        let other_name = opset.get_agent_name(other_loc.agent);

                        // Its possible for a user to conflict with themself if they commit to
                        // multiple branches. In this case, sort by seq number.
                        let ins_here = match my_name.cmp(other_name) {
                            Ordering::Less => true,
                            Ordering::Equal => {
                                opset.get_crdt_location(item.id.start) < opset.get_crdt_location(other_entry.id.start)
                            }
                            Ordering::Greater => false,
                        };

                        if ins_here {
                            // Insert here.
                            break;
                        } else {
                            scanning = false;
                        }
                    } else {
                        // Set scanning based on how the origin_right entries are ordered.
                        let my_right_cursor = self.get_cursor_before(item.origin_right);
                        let other_right_cursor = self.get_cursor_before(other_entry.origin_right);

                        if other_right_cursor < my_right_cursor {
                            if !scanning {
                                scanning = true;
                                scan_start = cursor.clone();
                            }
                        } else {
                            scanning = false;
                        }
                    }
                }
            }

            // This looks wrong. The entry in the range tree is a run with:
            // - Incrementing orders (maybe from different peers)
            // - With incrementing origin_left.
            // Q: Is it possible that we get different behaviour if we don't separate out each
            // internal run within the entry and visit each one separately?
            //
            // The fuzzer says no, we don't need to do that. I assume its because internal entries
            // have higher origin_left, and thus they can't be peers with the newly inserted item
            // (which has a lower origin_left).
            if !cursor.next_entry() {
                // This is dirty. If the cursor can't move to the next entry, we still need to move
                // it to the end of the current element or we'll prepend. next_entry() doesn't do
                // that for some reason. TODO: Clean this up.
                cursor.offset = other_entry.len();
                break;
            }
        }
        if scanning { cursor = scan_start; }

        if cfg!(debug_assertions) {
            let pos = unsafe { cursor.unsafe_count_content_pos() };
            let len = self.range_tree.content_len();
            assert!(pos <= len);
        }

        // Now insert here.
        let content_pos = unsafe { cursor.unsafe_count_offset_pos() };
        unsafe { ContentTreeRaw::unsafe_insert_notify(&mut cursor, item, notify_for(&mut self.index)); }
        content_pos
    }

    fn apply_range(&mut self, opset: &OpSet, range: TimeSpan, mut to: Option<&mut Checkout>) {
        if range.is_empty() { return; }

        for mut pair in opset.iter_range(range) {
            loop {
                // let span = list.get_crdt_span(TimeSpan { start: pair.0, end: pair.0 + pair.1.len });
                let span = opset.get_crdt_span(pair.span());
                if span.len() < pair.1.len() {
                    let local_pair = pair.truncate_keeping_right(span.len());

                    self.apply(opset, span.agent, &local_pair, to.as_deref_mut());
                } else {
                    self.apply(opset, span.agent, &pair, to.as_deref_mut());
                    break;
                }
            }
        }
    }

    /// This is for advancing us directly based on the edit.
    fn apply(&mut self, opset: &OpSet, agent: AgentId, pair: &KVPair<Operation>, to: Option<&mut Checkout>) {
        // The op must have been applied at the branch that the tracker is currently at.
        let KVPair(time, op) = pair;

        match op.tag {
            InsDelTag::Ins => {
                if op.rev { unimplemented!("Implement me!") }

                // To implement this we need to:
                // 1. Find the item directly before the requested position. This is our origin-left.
                // 2. Scan forward until the next item which isn't in the not yet inserted state.
                // this is our origin right.
                // 3. Use the integrate() method to actually insert - since we need to handle local
                // conflicts.

                let (origin_left, mut cursor) = if op.pos == 0 {
                    (ROOT_TIME, self.range_tree.mut_cursor_at_start())
                } else {
                    let mut cursor = self.range_tree.mut_cursor_at_content_pos((op.pos - 1) as usize, false);
                    let origin_left = cursor.get_item().unwrap();
                    assert!(cursor.next_item());
                    (origin_left, cursor)
                };

                // Origin_right should be the next item which isn't in the NotInsertedYet state.
                // If we reach the end of the document before that happens, use ROOT_TIME.
                let origin_right = if !cursor.roll_to_next_entry() {
                    ROOT_TIME
                } else {
                    let mut c2 = cursor.clone();
                    loop {
                        let e = c2.try_get_raw_entry();
                        if let Some(e) = e {
                            if e.state == YjsSpanState::NotInsertedYet {
                                if !c2.next_entry() { break ROOT_TIME; }
                                // Otherwise keep looping.
                            } else {
                                // We can use this.
                                break e.at_offset(c2.offset);
                            }
                        } else { break ROOT_TIME; }
                    }
                };

                // let origin_right = cursor.get_item().unwrap_or(ROOT_TIME);

                let item = YjsSpan2 {
                    id: TimeSpan::new(*time, *time + op.len()),
                    origin_left,
                    origin_right,
                    state: YjsSpanState::Inserted,
                    ever_deleted: false,
                };
                // dbg!(&item);

                // This is dirty because the cursor's lifetime is not associated with self.
                let cursor = cursor.inner;
                let ins_pos = self.integrate(opset, agent, item, cursor);

                let mut result = op.clone();
                result.pos = ins_pos;
                // act(result);
                if let Some(to) = to {
                    // println!("Insert at {} len {}", ins_pos, op.len());
                    assert!(op.content_known); // Ok if this is false - we'll just fill with junk.
                    to.content.insert(ins_pos, &op.content);
                }
            }

            InsDelTag::Del => {
                let cursor = self.range_tree.mut_cursor_at_content_pos(op.pos, false);
                let mut del_start = cursor.count_offset_pos();
                let deleted_items = unsafe {
                    let inner = cursor.inner;
                    self.range_tree.local_deactivate_notify(inner, op.len(), notify_for(&mut self.index))
                };

                // let deleted_items = self.range_tree.local_deactivate_at_content_notify(op.pos, op.len, notify_for(&mut self.index));

                // dbg!(&deleted_items);
                let mut next_time = *time;
                let mut del_end = del_start;

                for item in deleted_items {
                    self.deletes.push(KVPair(next_time, Delete {
                        target: item.id,
                        rev: op.rev
                    }));
                    next_time += item.len();

                    if !item.ever_deleted {
                        del_end += item.len();
                    } // Otherwise its a double-delete and the content is already deleted.
                }

                if let Some(to) = to {
                    to.content.remove(del_start..del_end);
                }
            }
        }
    }
}

impl Checkout {
    /// Add everything in merge_frontier into the set.
    ///
    /// Reexposed as merge_changes.
    pub(crate) fn merge_changes_m2(&mut self, opset: &OpSet, merge_frontier: &[Time]) {
        // The strategy here looks like this:
        // We have some set of new changes to merge with a unified set of parents.
        // 1. Find the parent set of the spans to merge
        // 2. Generate the conflict set, and make a tracker for it (by iterating all the conflicting
        //    changes).
        // 3. Use OptTxnIter to iterate through the (new) merge set, merging along the way.

        // if cfg!(debug_assertions) {
        //     let (a, b) = opset.history.diff(&self.frontier, merge_frontier);
        //     // We can't go backwards (yet).
        //     assert!(a.is_empty());
        // }

        debug_assert!(branch_is_sorted(&merge_frontier));
        debug_assert!(branch_is_sorted(&self.frontier));

        // let mut diff = opset.history.diff(&self.frontier, merge_frontier);

        // First lets see what we've got. I'll divide the conflicting range into two groups:
        // - The new operations we need to merge
        // - The conflict set. Ie, stuff we need to build a tracker around.
        let mut new_ops: SmallVec<[TimeSpan; 4]> = smallvec![];
        let mut conflict_ops: SmallVec<[TimeSpan; 4]> = smallvec![];

        let mut common_ancestor = opset.history.find_conflicting(&self.frontier, merge_frontier, |span, flag| {
            let target = match flag {
                OnlyB => &mut new_ops,
                _ => &mut conflict_ops
            };
            target.push_reversed_rle(span);
        });

        debug_assert!(branch_is_sorted(&common_ancestor));

        // We don't want to have to make and maintain a tracker, and we don't need to in most
        // situations. We don't need to when all operations in diff.only_b can apply cleanly
        // in-order.
        let mut did_ff = false;
        if ALLOW_FF {
            loop {
                if let Some(span) = new_ops.last() {
                    let txn = opset.history.entries.find_packed(span.start);
                    let can_ff = txn.with_parents(span.start, |parents| {
                        self.frontier == txn.parents
                    });

                    if can_ff {
                        let mut span = new_ops.pop().unwrap();
                        let remainder = span.trim(txn.span.end - span.start);
                        // println!("FF {:?}", &span);
                        self.apply_range_from(opset, span);
                        conflict_ops.push(span);
                        self.frontier = smallvec![span.last()];

                        if let Some(r) = remainder {
                            new_ops.push(r);
                        }
                        did_ff = true;
                    } else {
                        break;
                    }
                } else {
                    // We're done!
                    return;
                }
            }
        }

        if did_ff {
            // Since we ate some of the ops fast-forwarding, reset conflict_ops and common_ancestor
            // so we don't scan unnecessarily.
            conflict_ops.clear();
            common_ancestor = opset.history.find_conflicting(&self.frontier, merge_frontier, |span, flag| {
                if flag != OnlyB {
                    conflict_ops.push_reversed_rle(span);
                }
            });
        }

        // TODO: Also FF at the end!

        // dbg!((&self.frontier, &diff.common_branch, merge_frontier));
        let (mut tracker, branch) = M2Tracker::new_at_conflict(opset, ConflictZone {
            common_ancestor: common_ancestor.clone(),
            spans: conflict_ops
        });

        // Now walk through and merge the new edits.
        let mut walker = OptimizedTxnsIter::new(&opset.history, ConflictZone {
            common_ancestor,
            spans: new_ops
        }, Some(branch));

        while let Some(walk) = walker.next() {
            for range in walk.retreat {
                tracker.retreat_by_range(range);
            }

            for range in walk.advance_rev.into_iter().rev() {
                tracker.advance_by_range(range);
            }

            debug_assert!(!walk.consume.is_empty());
            // TODO: Add txn to walk, so we can use advance_branch_by_known.
            advance_branch_by(&mut self.frontier, &opset.history, walk.consume);
            tracker.apply_range(opset, walk.consume, Some(self));
        }
    }
}

#[cfg(test)]
mod test {
    use crate::list::ListCRDT;
    use super::*;

    #[test]
    fn test_ff() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.ops.push_insert(0, &[ROOT_TIME], 0, "aaa");

        list.checkout.merge_changes_m2(&list.ops, &[1]);
        list.checkout.merge_changes_m2(&list.ops, &[2]);

        assert_eq!(list.checkout.frontier.as_slice(), &[2]);
        assert_eq!(list.checkout.content, "aaa");
    }

    #[test]
    fn test_ff_merge() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        list.ops.push_insert(0, &[ROOT_TIME], 0, "aaa");
        list.ops.push_insert(1, &[ROOT_TIME], 0, "bbb");
        list.checkout.merge_changes_m2(&list.ops, &[2, 5]);

        assert_eq!(list.checkout.frontier.as_slice(), &[2, 5]);
        assert_eq!(list.checkout.content, "aaabbb");

        list.ops.push_insert(0, &[2, 5], 0, "ccc"); // 8
        list.checkout.merge_changes_m2(&list.ops, &[8]);

        assert_eq!(list.checkout.frontier.as_slice(), &[8]);
        assert_eq!(list.checkout.content, "cccaaabbb");
    }

    #[test]
    fn test_merge_inserts() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        list.ops.push_insert(0, &[ROOT_TIME], 0, "aaa");
        list.ops.push_insert(1, &[ROOT_TIME], 0, "bbb");

        list.checkout.merge_changes_m2(&list.ops, &[2, 5]);
        // list.checkout.merge_changes_m2(&list.ops, &[2]);
        // list.checkout.merge_changes_m2(&list.ops, &[5]);

        // dbg!(list.checkout);
        assert_eq!(list.checkout.frontier.as_slice(), &[2, 5]);
        assert_eq!(list.checkout.content, "aaabbb");
    }

    #[test]
    fn test_merge_deletes_1() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        list.local_insert(0, 0, "aaa");
        // list.ops.push_insert(0, &[ROOT_TIME], 0, "aaa");

        list.ops.push_delete(0, &[2], 1, 1); // &[3]
        list.ops.push_delete(1, &[2], 0, 3); // &[6]

        // M2Tracker::apply_to_checkout(&mut list.checkout, &list.ops, (0..list.ops.len()).into());
        // list.checkout.merge_changes_m2(&list.ops, (3..list.ops.len()).into());
        list.checkout.merge_changes_m2(&list.ops, &[3, 6]);
        assert_eq!(list.checkout.content, "");
    }

    #[test]
    fn test_merge_deletes_2() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        let t = list.ops.push_insert(0, &[ROOT_TIME], 0, "aaa");
        list.ops.push_delete(0, &[t], 1, 1); // 3
        list.ops.push_delete(1, &[t], 0, 3); // 6
        // dbg!(&list.ops);

        // list.checkout.merge_changes_m2(&list.ops, (0..list.ops.len()).into());
        list.checkout.merge_changes_m2(&list.ops, &[3, 6]);
        dbg!(&list.checkout);
        // assert_eq!(list.checkout.content, "");
    }

    #[test]
    fn test_concurrent_insert() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");
        // list.local_insert(0, 0, "aaa");

        list.ops.push_insert(0, &[ROOT_TIME], 0, "aaa");
        list.ops.push_insert(1, &[ROOT_TIME], 0, "bbb");

        let mut t = M2Tracker::new();
        t.apply_range(&list.ops, (0..3).into(), None);
        t.retreat_by_range((0..3).into());
        t.apply_range(&list.ops, (3..6).into(), None);
        dbg!(&t);
        // t.apply_range_at_version()
    }

    #[test]
    fn test_concurrent_delete() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        list.local_insert(0, 0, "aaa");

        list.ops.push_delete(0, &[2], 1, 1);
        list.ops.push_delete(1, &[2], 0, 3);

        let mut t = M2Tracker::new();
        t.apply_range(&list.ops, (0..4).into(), None);
        t.retreat_by_range((3..4).into());
        t.apply_range(&list.ops, (4..7).into(), None);
        dbg!(&t);
        // t.apply_range_at_version()
    }

    #[test]
    fn foo() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("seph");
        list.local_insert(0, 0, "hi there");
        list.local_delete(0, 2, 3);

        let mut t = M2Tracker::new();

        let end = list.ops.len();
        dbg!(end);
        t.apply_range(&list.ops, (0..end).into(), None);

        // dbg!(&t);

        // t.retreat_by_range((0..end).into());
        t.retreat_by_range((8..end).into());
        t.retreat_by_range((7..8).into());
        dbg!(&t);
    }

    #[test]
    fn backspace() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("seph");
        let mut t = ROOT_TIME;
        t = list.ops.push_insert(0, &[t], 0, "abc"); // 2
        t = list.ops.push_delete(0, &[t], 2, 1); // 3 -> "ab_"
        t = list.ops.push_delete(0, &[t], 1, 1); // 4 -> "a__"
        t = list.ops.push_delete(0, &[t], 0, 1); // 5 -> "___"

        let mut t = M2Tracker::new();
        t.apply_range(&list.ops, (3..6).into(), None);
        t.retreat_by_range((5..6).into());
        dbg!(&t);

        // list.checkout.merge_branch(&list.ops, &[4]);
        // dbg!(&list.checkout);
    }

    #[test]
    fn ins_back() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("seph");
        let mut t = ROOT_TIME;
        t = list.ops.push_insert(0, &[t], 0, "c");
        t = list.ops.push_insert(0, &[t], 0, "b");
        t = list.ops.push_insert(0, &[t], 0, "a");

        dbg!(&list.ops);
        list.checkout.merge_branch(&list.ops, &[t]);
        dbg!(&list.checkout);
    }
}