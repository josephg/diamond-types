use std::cmp::Ordering;
use std::ptr::NonNull;
use smallvec::{SmallVec, smallvec};
use content_tree::*;
use rle::{AppendRle, HasLength, Searchable, SplitableSpan, Trim};
use crate::list::{Frontier, Branch, OpLog, Time};
use crate::list::m2::{DocRangeIndex, M2Tracker, SpaceIndex};
use crate::list::m2::yjsspan2::{INSERTED, NOT_INSERTED_YET, YjsSpan2};
use crate::list::operation::{InsDelTag, Operation};
use crate::localtime::{is_underwater, TimeSpan};
use crate::rle::{KVPair, RleSpanHelpers};
use crate::{AgentId, ROOT_TIME};
use crate::list::frontier::{advance_frontier_by, frontier_eq, frontier_is_sorted};
use crate::list::history_tools::Flag;
use crate::list::m2::rev_span::TimeSpanRev;

#[cfg(feature = "dot_export")]
use crate::list::m2::dot::{DotColor, name_of};
#[cfg(feature = "dot_export")]
use crate::list::m2::dot::DotColor::*;

use crate::list::m2::markers::Marker::{DelTarget, InsPtr};
use crate::list::m2::markers::MarkerEntry;
use crate::list::m2::metrics::upstream_cursor_pos;
use crate::list::m2::txn_trace::OptimizedTxnsIter;
use crate::list::operation::InsDelTag::Ins;

const ALLOW_FF: bool = true;

fn pad_index_to(index: &mut SpaceIndex, desired_len: usize) {
    // TODO: Use dirty tricks to avoid this for more performance.
    let index_len = index.len() as usize;

    if index_len < desired_len {
        index.push(MarkerEntry {
            len: desired_len - index_len,
            inner: InsPtr(std::ptr::NonNull::dangling()),
        });
    }
}

pub(super) fn notify_for(index: &mut SpaceIndex) -> impl FnMut(YjsSpan2, NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>>) + '_ {
    move |entry: YjsSpan2, leaf| {
        let start = entry.id.start;
        let len = entry.len();

        // Note we can only mutate_entries when we have something to mutate. The list is started
        // with a big placeholder "underwater" entry which will be split up as needed.

        let mut cursor = index.unsafe_cursor_at_offset_pos(start, false);
        unsafe {
            ContentTreeRaw::unsafe_mutate_entries_notify(|marker| {
                // The item should already be an insert entry.
                debug_assert_eq!(marker.inner.tag(), Ins);

                marker.inner = InsPtr(leaf);
            }, &mut cursor, len, null_notify);
        }
    }
}


impl M2Tracker {
    pub(super) fn new() -> Self {
        let mut range_tree = ContentTreeWithIndex::new();
        let mut index = ContentTreeWithIndex::new();
        let underwater = YjsSpan2::new_underwater();
        pad_index_to(&mut index, underwater.id.end);
        range_tree.push_notify(underwater, notify_for(&mut index));

        Self {
            range_tree,
            index,
        }
    }

    pub(super) fn marker_at(&self, time: Time) -> NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>> {
        let cursor = self.index.cursor_at_offset_pos(time, false);
        // Gross.
        cursor.get_item().unwrap().unwrap()
    }

    #[allow(unused)]
    pub(super) fn check_index(&self) {
        // dbg!(&self.index);
        // dbg!(&self.range_tree);
        // Go through each entry in the range tree and make sure we can find it using the index.
        for entry in self.range_tree.raw_iter() {
            let marker = self.marker_at(entry.id.start);
            unsafe { marker.as_ref() }.find(entry.id.start).unwrap();
        }
    }

    fn get_cursor_before(&self, time: Time) -> Cursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
        if time == ROOT_TIME {
            // This case doesn't seem to ever get hit by the fuzzer. It might be equally correct to
            // just panic() here.
            self.range_tree.cursor_at_end()
        } else {
            let marker = self.marker_at(time);
            self.range_tree.cursor_before_item(time, marker)
        }
    }

    // pub(super) fn get_unsafe_cursor_after(&self, time: Time, stick_end: bool) -> UnsafeCursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
    fn get_cursor_after(&self, time: Time, stick_end: bool) -> Cursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
        if time == ROOT_TIME {
            self.range_tree.cursor_at_start()
        } else {
            let marker = self.marker_at(time);
            // let marker: NonNull<NodeLeaf<YjsSpan, ContentIndex>> = self.markers.at(order as usize).unwrap();
            // self.content_tree.
            let mut cursor = self.range_tree.cursor_before_item(time, marker);
            // The cursor points to parent. This is safe because of guarantees provided by
            // cursor_before_item.
            cursor.offset += 1;
            if !stick_end { cursor.roll_to_next_entry(); }
            cursor
        }
    }

    // TODO: Rewrite this to take a MutCursor instead of UnsafeCursor argument.
    pub(super) fn integrate(&mut self, opset: &OpLog, agent: AgentId, item: YjsSpan2, mut cursor: UnsafeCursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>) -> usize {
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
            // We can only be concurrent with other items which haven't been inserted yet at this
            // point in time.
            debug_assert_eq!(other_entry.state, NOT_INSERTED_YET);

            let other_left_time = other_entry.origin_left_at_offset(cursor.offset);
            let other_left_cursor = self.get_cursor_after(other_left_time, false);

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
        let mut cursor = unsafe { MutCursor::unchecked_from_raw(&mut self.range_tree, cursor) };
        let content_pos = upstream_cursor_pos(&cursor);

        // (Safe variant):
        // cursor.insert_notify(item, notify_for(&mut self.index));

        unsafe { ContentTreeRaw::unsafe_insert_notify(&mut cursor, item, notify_for(&mut self.index)); }
        // self.check_index();
        content_pos
    }

    fn apply_range(&mut self, opset: &OpLog, range: TimeSpan, mut to: Option<&mut Branch>) {
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
    fn apply(&mut self, opset: &OpLog, agent: AgentId, op_pair: &KVPair<Operation>, mut to: Option<&mut Branch>) {
        // self.check_index();
        // The op must have been applied at the branch that the tracker is currently at.
        let KVPair(time, op) = op_pair;
        // dbg!(op);
        match op.tag {
            InsDelTag::Ins => {
                if op.reversed { unimplemented!("Implement me!") }

                // To implement this we need to:
                // 1. Find the item directly before the requested position. This is our origin-left.
                // 2. Scan forward until the next item which isn't in the not yet inserted state.
                // this is our origin right.
                // 3. Use the integrate() method to actually insert - since we need to handle local
                // conflicts.

                // UNDERWATER_START = 4611686018427387903

                let (origin_left, mut cursor) = if op.pos == 0 {
                    (ROOT_TIME, self.range_tree.mut_cursor_at_start())
                } else {
                    let mut cursor = self.range_tree.mut_cursor_at_content_pos(op.pos - 1, false);
                    // dbg!(&cursor, cursor.get_raw_entry());
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
                            if e.state == NOT_INSERTED_YET {
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
                    state: INSERTED,
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
                    // dbg!(&self.range_tree);
                    // println!("Insert '{}' at {} (len {})", op.content, ins_pos, op.len());
                    assert!(op.content_known); // Ok if this is false - we'll just fill with junk.
                    assert!(ins_pos <= to.content.len_chars());
                    to.content.insert(ins_pos, &op.content);
                }
            }

            InsDelTag::Del => {
                // We need to loop here because the deleted span might have been broken up by
                // subsequent inserts.
                let mut remaining_len = op.len;

                let pos = op.pos;

                // This is needed because we're walking through the operation's span forwards
                // (because thats simpler). But if the delete is reversed, we need to record the
                // output time values in reverse order too.
                let mut resulting_time = TimeSpanRev {
                    span: (*time..*time + op.len).into(),
                    reversed: op.reversed
                };

                // It would be tempting - and *nearly* correct to just use local_delete inside the
                // range tree. Its hard to bake that logic in here though. We need to:
                // - Skip anything not in the Inserted state
                // - If an item is
                while remaining_len > 0 {
                    // TODO(perf): Reuse cursor. After mutate_single_entry we'll often be at another
                    // entry that we can delete.
                    let mut cursor = self.range_tree.mut_cursor_at_content_pos(pos, false);
                    // dbg!(pos, &cursor);
                    // If we've never been deleted locally, we'll need to do that.
                    let e = cursor.get_raw_entry();
                    assert_eq!(e.state, INSERTED);
                    let ever_deleted = e.ever_deleted;

                    let del_start_check = upstream_cursor_pos(&cursor);

                    let (mut_len, target) = unsafe {
                        ContentTreeRaw::unsafe_mutate_single_entry_notify(|e| {
                            // println!("Delete {:?}", e.id);

                            // This will set the state to deleted, and mark ever_deleted in the
                            // entry.
                            e.delete();
                            e.id
                        }, &mut cursor.inner, remaining_len, notify_for(&mut self.index))
                    };
                    debug_assert_eq!(mut_len, target.len());

                    if cfg!(debug_assertions) && !is_underwater(target.start) {
                        // dbg!(*time, &target);

                        // Deletes must always dominate item they're deleting in the time dag.
                        assert!(opset.history.frontier_contains_time(&[*time], target.start));
                    }

                    if let Some(to) = to.as_deref_mut() {
                        if !ever_deleted {
                            // Actually delete the item locally.
                            // let del_end = cursor.count_offset_pos();

                            // It seems this should be the position after the deleted entry, but the
                            // deleted item will have 0 upstream size.
                            let del_start = upstream_cursor_pos(&cursor);
                            debug_assert_eq!(del_start_check, del_start);
                            // dbg!(&self.range_tree);
                            // let del_start = del_end - mut_len;
                            let del_end = del_start + mut_len;
                            // dbg!(del_start_check, del_end, mut_len, del_start);

                            // dbg!((&op, del_start, mut_len));
                            debug_assert!(to.content.len_chars() >= del_end);
                            // println!("Delete {}..{} (len {}) '{}'", del_start, del_end, mut_len, to.content.slice_chars(del_start..del_end).collect::<String>());
                            to.content.remove(del_start..del_end);
                        } else {
                            // println!("Ignoring double delete of length {}", mut_len);
                        }
                    }

                    let time_here = resulting_time.truncate_keeping_right(mut_len);
                    // pad_index_to(&mut self.index, next_time);
                    self.index.replace_range_at_offset(time_here.span.start, MarkerEntry {
                        len: mut_len,
                        inner: DelTarget(TimeSpanRev {
                            span: target,
                            reversed: time_here.reversed
                        })
                    });

                    remaining_len -= mut_len;
                }
            }
        }

        if cfg!(debug_assertions) {
            self.check_index();
        }
    }

    /// Walk through a set of spans, adding them to this tracker.
    ///
    /// Returns the tracker's frontier after this has happened; which will be at some pretty
    /// arbitrary point in time based on the traversal. I could save that in a tracker field? Eh.
    fn walk(&mut self, opset: &OpLog, start_at: Frontier, rev_spans: &[TimeSpan], mut apply_to: Option<&mut Branch>) -> Frontier {
        let mut walker = OptimizedTxnsIter::new(&opset.history, rev_spans, start_at);

        for walk in &mut walker {
            // dbg!(&walk);
            for range in walk.retreat {
                self.retreat_by_range(range);
            }

            for range in walk.advance_rev.into_iter().rev() {
                self.advance_by_range(range);
            }

            debug_assert!(!walk.consume.is_empty());
            self.apply_range(opset, walk.consume, apply_to.as_deref_mut());
        }

        walker.into_frontier()
    }
}

#[cfg(feature = "dot_export")]
const MAKE_GRAPHS: bool = true;

impl Branch {
    /// Add everything in merge_frontier into the set.
    ///
    /// Reexposed as merge_changes.
    pub(crate) fn merge_changes_m2(&mut self, opset: &OpLog, merge_frontier: &[Time]) {
        // The strategy here looks like this:
        // We have some set of new changes to merge with a unified set of parents.
        // 1. Find the parent set of the spans to merge
        // 2. Generate the conflict set, and make a tracker for it (by iterating all the conflicting
        //    changes).
        // 3. Use OptTxnIter to iterate through the (new) merge set, merging along the way.

        debug_assert!(frontier_is_sorted(merge_frontier));
        debug_assert!(frontier_is_sorted(&self.frontier));

        // let mut diff = opset.history.diff(&self.frontier, merge_frontier);

        // First lets see what we've got. I'll divide the conflicting range into two groups:
        // - The new operations we need to merge
        // - The conflict set. Ie, stuff we need to build a tracker around.
        //
        // Both of these lists are in reverse time order(!).
        let mut new_ops: SmallVec<[TimeSpan; 4]> = smallvec![];
        let mut conflict_ops: SmallVec<[TimeSpan; 4]> = smallvec![];

        #[cfg(feature = "dot_export")]
        let mut dbg_all_ops: SmallVec<[(TimeSpan, DotColor); 4]> = smallvec![];

        let mut common_ancestor = opset.history.find_conflicting(&self.frontier, merge_frontier, |span, flag| {
            // Note we'll be visiting these operations in reverse order.

            // dbg!(&span, flag);
            let target = match flag {
                Flag::OnlyB => &mut new_ops,
                _ => &mut conflict_ops
            };
            target.push_reversed_rle(span);

            #[cfg(feature = "dot_export")]
            if MAKE_GRAPHS {
                let color = match flag {
                    Flag::OnlyA => Blue,
                    Flag::OnlyB => Green,
                    Flag::Shared => Grey,
                };
                dbg_all_ops.push((span, color));
            }
        });

        #[cfg(feature = "dot_export")]
        if MAKE_GRAPHS {
            let s1 = merge_frontier.iter().map(|t| name_of(*t)).collect::<Vec<_>>().join("-");
            let s2 = self.frontier.iter().map(|t| name_of(*t)).collect::<Vec<_>>().join("-");

            let filename = format!("../../svgs/m{}_to_{}.svg", s1, s2);
            let content = self.content.to_string();
            opset.make_graph(&filename, &content, dbg_all_ops.iter().copied());
            println!("Saved graph to {}", filename);
        }

        // dbg!(&opset.history);
        // dbg!((&new_ops, &conflict_ops, &common_ancestor));

        debug_assert!(frontier_is_sorted(&common_ancestor));

        // We don't want to have to make and maintain a tracker, and we don't need to in most
        // situations. We don't need to when all operations in diff.only_b can apply cleanly
        // in-order.
        let mut did_ff = false;
        if ALLOW_FF {
            loop {
                if let Some(span) = new_ops.last() {
                    let txn = opset.history.entries.find_packed(span.start);
                    let can_ff = txn.with_parents(span.start, |parents| {
                        // Previously this said:
                        //   self.frontier == txn.parents
                        // and the tests still passed. TODO: Was that code wrong? If so make a test case.
                        frontier_eq(self.frontier.as_slice(), parents)
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
            //
            // We don't need to reset new_ops because that was updated above.
            conflict_ops.clear();
            common_ancestor = opset.history.find_conflicting(&self.frontier, merge_frontier, |span, flag| {
                if flag != Flag::OnlyB {
                    conflict_ops.push_reversed_rle(span);
                }
            });
        }

        // TODO: Also FF at the end!

        // For conflicting operations, we'll make a tracker starting at the common_ancestor and
        // containing the conflicting_ops set. (Which is everything that is either common, or only
        // in this branch).
        let mut tracker = M2Tracker::new();
        let frontier = tracker.walk(opset, common_ancestor, &conflict_ops, None);

        // Then walk through and merge any new edits.
        tracker.walk(&opset, frontier, &new_ops, Some(self));

        // ... And update our frontier.
        for range in new_ops.into_iter().rev() {
            advance_frontier_by(&mut self.frontier, &opset.history, range);
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

        list.branch.merge(&list.ops, &[1]);
        list.branch.merge(&list.ops, &[2]);

        assert_eq!(list.branch.frontier.as_slice(), &[2]);
        assert_eq!(list.branch.content, "aaa");
    }

    #[test]
    fn test_ff_merge() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        list.ops.push_insert(0, &[ROOT_TIME], 0, "aaa");
        list.ops.push_insert(1, &[ROOT_TIME], 0, "bbb");
        list.branch.merge(&list.ops, &[2, 5]);

        assert_eq!(list.branch.frontier.as_slice(), &[2, 5]);
        assert_eq!(list.branch.content, "aaabbb");

        list.ops.push_insert(0, &[2, 5], 0, "ccc"); // 8
        list.branch.merge(&list.ops, &[8]);

        assert_eq!(list.branch.frontier.as_slice(), &[8]);
        assert_eq!(list.branch.content, "cccaaabbb");
    }

    #[test]
    fn test_merge_inserts() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        list.ops.push_insert(0, &[ROOT_TIME], 0, "aaa");
        list.ops.push_insert(1, &[ROOT_TIME], 0, "bbb");

        list.branch.merge(&list.ops, &[2, 5]);
        // list.checkout.merge_changes_m2(&list.ops, &[2]);
        // list.checkout.merge_changes_m2(&list.ops, &[5]);

        // dbg!(list.checkout);
        assert_eq!(list.branch.frontier.as_slice(), &[2, 5]);
        assert_eq!(list.branch.content, "aaabbb");
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
        list.branch.merge(&list.ops, &[3, 6]);
        assert_eq!(list.branch.content, "");
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
        list.branch.merge(&list.ops, &[3, 6]);
        dbg!(&list.branch);
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
        assert_eq!(t, 5);

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
        list.branch.merge(&list.ops, &[t]);
        dbg!(&list.branch);
    }
}