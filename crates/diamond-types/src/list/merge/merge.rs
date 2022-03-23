// Clippy complains about .as_mut_ref() below. But that construction is needed for the borrow
// checker.
#![allow(clippy::needless_option_as_deref)]

use std::cmp::Ordering;
use std::ptr::NonNull;
use jumprope::JumpRope;
use smallvec::{SmallVec, smallvec};
use smartstring::alias::String as SmartString;
use content_tree::*;
use rle::{AppendRle, HasLength, Searchable, Trim, TrimCtx};
use crate::list::{Frontier, Branch, OpLog, Time};
use crate::list::merge::{DocRangeIndex, M2Tracker, SpaceIndex};
use crate::list::merge::yjsspan::{INSERTED, NOT_INSERTED_YET, YjsSpan};
use crate::list::operation::{InsDelTag, Operation};
use crate::localtime::{is_underwater, TimeSpan};
use crate::rle::{KVPair, RleSpanHelpers};
use crate::{AgentId, ROOT_TIME};
use crate::list::frontier::{advance_frontier_by, frontier_eq, frontier_is_sorted};
use crate::list::history_tools::DiffFlag;
use crate::list::internal_op::OperationInternal;
use crate::list::buffered_iter::BufferedIter;
use crate::rev_span::TimeSpanRev;

#[cfg(feature = "dot_export")]
use crate::list::merge::dot::{DotColor, name_of};
#[cfg(feature = "dot_export")]
use crate::list::merge::dot::DotColor::*;

use crate::list::merge::markers::Marker::{DelTarget, InsPtr};
use crate::list::merge::markers::MarkerEntry;
use crate::list::merge::merge::TransformedResult::{BaseMoved, DeleteAlreadyHappened};
use crate::list::merge::metrics::upstream_cursor_pos;
use crate::list::merge::txn_trace::SpanningTreeWalker;
use crate::list::op_iter::OpMetricsIter;
use crate::list::operation::InsDelTag::Ins;
use crate::unicount::consume_chars;

const ALLOW_FF: bool = true;

#[cfg(feature = "dot_export")]
const MAKE_GRAPHS: bool = false;

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

pub(super) fn notify_for(index: &mut SpaceIndex) -> impl FnMut(YjsSpan, NonNull<NodeLeaf<YjsSpan, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>>) + '_ {
    move |entry: YjsSpan, leaf| {
        debug_assert!(leaf != NonNull::dangling());
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

#[allow(unused)]
fn take_content<'a>(x: Option<&mut &'a str>, len: usize) -> Option<&'a str> {
    if let Some(s) = x {
        Some(consume_chars(s, len))
    } else { None }
}

impl M2Tracker {
    pub(super) fn new() -> Self {
        let mut range_tree = ContentTreeRaw::new();
        let mut index = ContentTreeRaw::new();
        let underwater = YjsSpan::new_underwater();
        pad_index_to(&mut index, underwater.id.end);
        range_tree.push_notify(underwater, notify_for(&mut index));

        Self {
            range_tree,
            index,
        }
    }

    pub(super) fn marker_at(&self, time: Time) -> NonNull<NodeLeaf<YjsSpan, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>> {
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
            debug_assert!(marker != NonNull::dangling());
            unsafe { marker.as_ref() }.find(entry.id.start).unwrap();
        }
    }

    fn get_cursor_before(&self, time: Time) -> Cursor<YjsSpan, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
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
    fn get_cursor_after(&self, time: Time, stick_end: bool) -> Cursor<YjsSpan, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
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
    pub(super) fn integrate(&mut self, oplog: &OpLog, agent: AgentId, item: YjsSpan, mut cursor: UnsafeCursor<YjsSpan, DocRangeIndex>) -> usize {
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
                        let my_name = oplog.get_agent_name(agent);
                        let other_loc = oplog.client_with_localtime.get(other_order);
                        let other_name = oplog.get_agent_name(other_loc.agent);

                        // Its possible for a user to conflict with themself if they commit to
                        // multiple branches. In this case, sort by seq number.
                        let ins_here = match my_name.cmp(other_name) {
                            Ordering::Less => true,
                            Ordering::Equal => {
                                oplog.time_to_crdt_id(item.id.start) < oplog.time_to_crdt_id(other_entry.id.start)
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

    fn apply_range(&mut self, oplog: &OpLog, range: TimeSpan, mut to: Option<&mut Branch>) {
        if range.is_empty() { return; }

        if let Some(to) = to.as_deref_mut() {
            advance_frontier_by(&mut to.frontier, &oplog.history, range);
        }

        let mut iter = oplog.iter_metrics_range(range);
        while let Some(mut pair) = iter.next() {
            loop {
                let span = oplog.get_crdt_span(pair.span());

                let len = span.len();
                let remainder = pair.trim_ctx(len, iter.ctx);

                let content = iter.get_content(&pair);

                self.apply_to(oplog, span.agent, &pair, content, to.as_deref_mut().map(|b| &mut b.content));
                // self.apply_working(oplog, span.agent, &pair, content, to.as_deref_mut().map(|b| &mut b.content));

                if let Some(r) = remainder {
                    pair = r;
                } else { break; }
            }
        }
    }

    fn apply_to(&mut self, oplog: &OpLog, agent: AgentId, op_pair: &KVPair<OperationInternal>, content: Option<&str>, mut to: Option<&mut JumpRope>) {
        let mut op_pair = op_pair.clone();

        loop {
            let (len_here, transformed_pos) = self.apply(oplog, agent, &op_pair, usize::MAX);

            let remainder = op_pair.trim_ctx(len_here, &oplog.operation_ctx);

            // dbg!((&op_pair, len_here, transformed_pos));
            if let BaseMoved(pos) = transformed_pos {
                if let Some(to) = to.as_mut() {
                    // Apply the operation here.
                    match op_pair.1.tag {
                        InsDelTag::Ins => {
                            // dbg!(&self.range_tree);
                            // println!("Insert '{}' at {} (len {})", op.content, ins_pos, op.len());
                            debug_assert!(op_pair.1.content_pos.is_some()); // Ok if this is false - we'll just fill with junk.
                            let content = content.unwrap();
                            assert!(pos <= to.len_chars());
                            to.insert(pos, content);
                        }
                        InsDelTag::Del => {
                            // Actually delete the item locally.
                            let del_end = pos + len_here;
                            debug_assert!(to.len_chars() >= del_end);
                            // println!("Delete {}..{} (len {}) '{}'", del_start, del_end, mut_len, to.content.slice_chars(del_start..del_end).collect::<String>());
                            to.remove(pos..del_end);
                        }
                    }
                }
            }

            if let Some(r) = remainder {
                op_pair = r;
                // Curiously, we don't need to update content because we only use content for
                // inserts, and inserts are always processed in one go. (Ie, there's never a
                // remainder to worry about).
                debug_assert_ne!(op_pair.1.tag, InsDelTag::Ins);
            } else { break; }
        }
    }

    /// This is for advancing us directly based on the edit.
    ///
    /// This method does 2 things:
    ///
    /// 1. Advance the tracker (self) based on the passed operation. This will insert new items in
    ///    to the tracker object, and should only be done exactly once for each operation in the set
    ///    we care about
    /// 2. Figure out where the operation will land in the resulting document (if anywhere).
    ///    The resulting operation could happen never (if its a double delete), once (inserts)
    ///    or generate many individual edits (eg if a delete is split). This method should be called
    ///    in a loop.
    ///
    /// Returns (size here, transformed insert / delete position).
    ///
    /// For inserts, the expected behaviour is this:
    ///
    /// |           | OriginLeft | OriginRight |
    /// |-----------|------------|-------------|
    /// | NotInsYet | Before     | After       |
    /// | Inserted  | After      | Before      |
    /// | Deleted   | Before     | Before      |
    fn apply(&mut self, oplog: &OpLog, agent: AgentId, op_pair: &KVPair<OperationInternal>, max_len: usize) -> (usize, TransformedResult) {
        // self.check_index();
        // The op must have been applied at the branch that the tracker is currently at.
        let len = max_len.min(op_pair.len());
        let op = &op_pair.1;

        // dbg!(op);
        match op.tag {
            InsDelTag::Ins => {
                if !op.span.fwd { unimplemented!("Implement me!") }

                // To implement this we need to:
                // 1. Find the item directly before the requested position. This is our origin-left.
                // 2. Scan forward until the next item which isn't in the not yet inserted state.
                // this is our origin right.
                // 3. Use the integrate() method to actually insert - since we need to handle local
                // conflicts.

                // UNDERWATER_START = 4611686018427387903

                let (origin_left, mut cursor) = if op.start() == 0 {
                    (ROOT_TIME, self.range_tree.mut_cursor_at_start())
                } else {
                    let mut cursor = self.range_tree.mut_cursor_at_content_pos(op.start() - 1, false);
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

                let mut time_span = op_pair.span();
                time_span.trim(len);

                let item = YjsSpan {
                    id: time_span,
                    origin_left,
                    origin_right,
                    state: INSERTED,
                    ever_deleted: false,
                };
                // dbg!(&item);

                // This is dirty because the cursor's lifetime is not associated with self.
                let cursor = cursor.inner;
                let ins_pos = self.integrate(oplog, agent, item, cursor);
                // self.range_tree.check();
                // self.check_index();

                (len, BaseMoved(ins_pos))
            }

            InsDelTag::Del => {
                // Delete as much as we can. We might not be able to delete everything because of
                // double deletes and inserts inside the deleted range. This is extra annoying
                // because we need to move backwards through the deleted items if we're rev.
                debug_assert!(op.len() > 0);
                // let mut remaining_len = op.len();

                let fwd = op.span.fwd;

                let (mut cursor, len) = if fwd {
                    let start_pos = op.start();
                    let cursor = self.range_tree.mut_cursor_at_content_pos(start_pos, false);
                    (cursor, len)
                } else {
                    // We're moving backwards. We need to delete as many items as we can before the
                    // end of the op.
                    let last_pos = op.span.span.last();
                    // Find the last entry
                    let mut cursor = self.range_tree.mut_cursor_at_content_pos(last_pos, false);

                    let entry_origin_start = last_pos - cursor.offset;
                    // let edit_start = entry_origin_start.max(op.start());
                    let edit_start = entry_origin_start.max(op.end() - len);
                    let len = op.end() - edit_start;
                    debug_assert!(len <= max_len);
                    cursor.offset -= len - 1;

                    (cursor, len)
                };

                let e = cursor.get_raw_entry();
                assert_eq!(e.state, INSERTED);

                // If we've never been deleted locally, we'll need to do that.
                let ever_deleted = e.ever_deleted;

                // TODO(perf): Reuse cursor. After mutate_single_entry we'll often be at another
                // entry that we can delete in a run.

                // The transformed position that this delete is at. Only actually needed if we're
                // modifying
                let del_start_xf = upstream_cursor_pos(&cursor);

                let (len2, target) = unsafe {
                    // It would be tempting - and *nearly* correct to just use local_delete inside the
                    // range tree. Its hard to bake that logic in here though.

                    // TODO(perf): Reuse cursor. After mutate_single_entry we'll often be at another
                    // entry that we can delete in a run.
                    ContentTreeRaw::unsafe_mutate_single_entry_notify(|e| {
                        // println!("Delete {:?}", e.id);
                        // This will set the state to deleted, and mark ever_deleted in the entry.
                        e.delete();
                        e.id
                    }, &mut cursor.inner, len, notify_for(&mut self.index))
                };

                // ContentTree should come to the same length conclusion as us.
                if !fwd { debug_assert_eq!(len2, len); }
                let len = len2;

                debug_assert_eq!(len, target.len());
                debug_assert_eq!(del_start_xf, upstream_cursor_pos(&cursor));

                let time_start = op_pair.0;
                if !is_underwater(target.start) {
                    // Deletes must always dominate the item they're deleting in the time dag.
                    debug_assert!(oplog.history.frontier_contains_time(&[time_start], target.start));
                }

                self.index.replace_range_at_offset(time_start, MarkerEntry {
                    len,
                    inner: DelTarget(TimeSpanRev {
                        span: target,
                        fwd
                    })
                });

                if cfg!(debug_assertions) {
                    self.check_index();
                }

                (len, if !ever_deleted {
                    BaseMoved(del_start_xf)
                } else {
                    DeleteAlreadyHappened
                })
            }
        }
    }

    /// Walk through a set of spans, adding them to this tracker.
    ///
    /// Returns the tracker's frontier after this has happened; which will be at some pretty
    /// arbitrary point in time based on the traversal. I could save that in a tracker field? Eh.
    fn walk(&mut self, oplog: &OpLog, start_at: Frontier, rev_spans: &[TimeSpan], mut apply_to: Option<&mut Branch>) -> Frontier {
        let mut walker = SpanningTreeWalker::new(&oplog.history, rev_spans, start_at);

        for walk in &mut walker {
            // dbg!(&walk);
            for range in walk.retreat {
                self.retreat_by_range(range);
            }

            for range in walk.advance_rev.into_iter().rev() {
                self.advance_by_range(range);
            }

            debug_assert!(!walk.consume.is_empty());
            self.apply_range(oplog, walk.consume, apply_to.as_deref_mut());
        }

        walker.into_frontier()
    }
}

#[derive(Debug)]
pub(crate) struct TransformedOpsIter<'a> {
    oplog: &'a OpLog,
    op_iter: Option<BufferedIter<OpMetricsIter<'a>>>,
    ff_mode: bool,
    // ff_idx: usize,
    did_ff: bool, // TODO: Do I really need this?

    merge_frontier: Frontier,

    common_ancestor: Frontier,
    conflict_ops: SmallVec<[TimeSpan; 4]>,
    new_ops: SmallVec<[TimeSpan; 4]>,

    next_frontier: Frontier,

    // TODO: This tracker allocates - which we don't need to do if we're FF-ing.
    phase2: Option<(M2Tracker, SpanningTreeWalker<'a>)>,
}

impl<'a> TransformedOpsIter<'a> {
    fn new(oplog: &'a OpLog, from_frontier: &[Time], merge_frontier: &[Time]) -> Self {
        // The strategy here looks like this:
        // We have some set of new changes to merge with a unified set of parents.
        // 1. Find the parent set of the spans to merge
        // 2. Generate the conflict set, and make a tracker for it (by iterating all the conflicting
        //    changes).
        // 3. Use OptTxnIter to iterate through the (new) merge set, merging along the way.

        debug_assert!(frontier_is_sorted(merge_frontier));
        debug_assert!(frontier_is_sorted(&from_frontier));

        // let mut diff = opset.history.diff(&self.frontier, merge_frontier);

        // First lets see what we've got. I'll divide the conflicting range into two groups:
        // - The new operations we need to merge
        // - The conflict set. Ie, stuff we need to build a tracker around.
        //
        // Both of these lists are in reverse time order(!).
        let mut new_ops: SmallVec<[TimeSpan; 4]> = smallvec![];
        let mut conflict_ops: SmallVec<[TimeSpan; 4]> = smallvec![];

        let common_ancestor = oplog.history.find_conflicting(&from_frontier, merge_frontier, |span, flag| {
            // Note we'll be visiting these operations in reverse order.

            // dbg!(&span, flag);
            let target = match flag {
                DiffFlag::OnlyB => &mut new_ops,
                _ => &mut conflict_ops
            };
            target.push_reversed_rle(span);
        });


        // dbg!(&opset.history);
        // dbg!((&new_ops, &conflict_ops, &common_ancestor));

        debug_assert!(frontier_is_sorted(&common_ancestor));

        Self {
            oplog,
            op_iter: None,
            ff_mode: true,
            did_ff: false,
            merge_frontier: merge_frontier.into(),
            common_ancestor,
            conflict_ops,
            new_ops,
            next_frontier: from_frontier.into(),
            phase2: None,
        }
    }

    pub(crate) fn into_frontier(self) -> Frontier {
        self.next_frontier
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum TransformedResult {
    BaseMoved(usize),
    DeleteAlreadyHappened,
}

type TransformedTriple = (Time, OperationInternal, TransformedResult);

impl TransformedResult {
    fn not_moved(op_pair: KVPair<OperationInternal>) -> TransformedTriple {
        let start = op_pair.1.start();
        (op_pair.0, op_pair.1, TransformedResult::BaseMoved(start))
    }
}

impl<'a> Iterator for TransformedOpsIter<'a> {
    /// Iterator over transformed operations. The KVPair.0 holds the original time of the operation.
    type Item = (Time, OperationInternal, TransformedResult);

    fn next(&mut self) -> Option<Self::Item> {
        // We're done when we've merged everything in self.new_ops.
        if self.op_iter.is_none() && self.new_ops.is_empty() { return None; }

        if self.ff_mode {
            // Keep trying to fast forward. If we have an op_iter while ff_mode is set, we can just
            // eat operations out of it without transforming, as fast as we can.
            if let Some(iter) = self.op_iter.as_mut() {
                // Keep iterating through this iter.
                if let Some(result) = iter.next() {
                    // Could ditch the iterator if its empty now...
                    // return result;
                    return Some(TransformedResult::not_moved(result));
                } else {
                    self.op_iter = None;
                    // This is needed because we could be sitting on an empty op_iter.
                    if self.new_ops.is_empty() { return None; }
                }
            }

            debug_assert!(self.op_iter.is_none());
            debug_assert!(!self.new_ops.is_empty());

            let span = self.new_ops.last().unwrap();
            let txn = self.oplog.history.entries.find_packed(span.start);
            let can_ff = txn.with_parents(span.start, |parents| {
                frontier_eq(&self.next_frontier, parents)
            });

            if can_ff {
                let mut span = self.new_ops.pop().unwrap();

                let remainder = span.trim(txn.span.end - span.start);

                debug_assert!(!span.is_empty());

                self.next_frontier = smallvec![span.last()];

                if let Some(r) = remainder {
                    self.new_ops.push(r);
                }
                self.did_ff = true;

                let mut iter = self.oplog.iter_metrics_range(span);

                // Pull the first item off the iterator and keep it for later.
                // A fresh iterator should always return something!
                let result = iter.next().unwrap();
                // assert!(result.is_some());

                self.op_iter = Some(iter.into());
                return Some(TransformedResult::not_moved(result));
            } else {
                self.ff_mode = false;
                if self.did_ff {
                    // Since we ate some of the ops fast-forwarding, reset conflict_ops and common_ancestor
                    // so we don't scan unnecessarily.
                    //
                    // We don't need to reset new_ops because that was updated above.

                    // This sometimes adds the FF'ed ops to the conflict_ops set so we add them to the
                    // merge set. This is a pretty bad way to do this - if we're gonna add them to
                    // conflict_ops then FF is pointless.
                    self.conflict_ops.clear();
                    self.common_ancestor = self.oplog.history.find_conflicting(&self.next_frontier, &self.merge_frontier, |span, flag| {
                        if flag != DiffFlag::OnlyB {
                            self.conflict_ops.push_reversed_rle(span);
                        }
                    });
                }
            }
        }

        // Ok, time for serious mode.

        // For conflicting operations, we'll make a tracker starting at the common_ancestor and
        // containing the conflicting_ops set. (Which is everything that is either common, or only
        // in this branch).

        // So first we can just call .walk() to setup the tracker "hot".
        let (tracker, walker) = match self.phase2.as_mut() {
            Some(phase2) => phase2,
            None => {
                let mut tracker = M2Tracker::new();
                let frontier = tracker.walk(
                    self.oplog,
                    std::mem::take(&mut self.common_ancestor), // Gross. TODO: Is this better than .clone()
                    &self.conflict_ops,
                    None);

                let walker = SpanningTreeWalker::new(&self.oplog.history, &self.new_ops, frontier);
                self.phase2 = Some((tracker, walker));
                // This is a kinda gross way to do this. TODO: Rewrite without .unwrap() somehow?
                self.phase2.as_mut().unwrap()
            }
        };

        let (mut pair, op_iter) = loop {
            if let Some(op_iter) = self.op_iter.as_mut() {
                if let Some(pair) = op_iter.next() {
                    break (pair, op_iter);
                }
            }

            // Otherwise advance to the next chunk from walker.

            // If this returns None, we're done.
            let walk = walker.next()?;

            // dbg!(&walk);
            for range in walk.retreat {
                tracker.retreat_by_range(range);
            }

            for range in walk.advance_rev.into_iter().rev() {
                tracker.advance_by_range(range);
            }

            assert!(!walk.consume.is_empty());

            // Only really advancing the frontier so we can consume into it. The resulting frontier
            // is interesting in lots of places.
            //
            // The walker can be unwrapped into its inner frontier, but that won't include
            // everything. (TODO: Look into fixing that?)
            advance_frontier_by(&mut self.next_frontier, &self.oplog.history, walk.consume);
            self.op_iter = Some(self.oplog.iter_metrics_range(walk.consume).into());
        };

        // Ok, try to consume as much as we can from pair.
        let span = self.oplog.get_crdt_span(pair.span());
        let len = span.len().min(pair.len());

        let (consumed_here, xf_result) = tracker.apply(&self.oplog, span.agent, &pair, len);

        let remainder = pair.trim_ctx(consumed_here, &self.oplog.operation_ctx);

        // (Time, OperationInternal, TransformedResult)
        let result = (pair.0, pair.1, xf_result);

        if let Some(r) = remainder {
            op_iter.push_back(r);
        }

        Some(result)
        // TODO: Also FF at the end!
    }
}

impl OpLog {
    pub(crate) fn get_xf_operations_full(&self, from: &[Time], merging: &[Time]) -> TransformedOpsIter {
        TransformedOpsIter::new(self, from, merging)
    }

    pub fn get_xf_operations(&self, from: &[Time], merging: &[Time]) -> impl Iterator<Item=(TimeSpan, Option<Operation>)> + '_ {
        TransformedOpsIter::new(self, from, merging)
            .map(|(time, mut origin_op, xf)| {
                let len = origin_op.len();
                let op: Option<Operation> = match xf {
                    BaseMoved(base) => {
                        origin_op.span.span = (base..base+len).into();
                        let content = origin_op.get_content(self);
                        Some((origin_op, content).into())
                    }
                    DeleteAlreadyHappened => None,
                };
                ((time..time+len).into(), op)
            })
    }

    pub fn get_all_xf_operations(&self) -> impl Iterator<Item=(TimeSpan, Option<Operation>)> + '_ {
        self.get_xf_operations(&[ROOT_TIME], &self.frontier)
    }
}

fn reverse_str(s: &str) -> SmartString {
    let mut result = SmartString::new();
    result.extend(s.chars().rev());
    result
}

impl Branch {
    /// Add everything in merge_frontier into the set.
    ///
    /// Reexposed as merge_changes.
    pub fn merge(&mut self, oplog: &OpLog, merge_frontier: &[Time]) {
        // let mut iter = TransformedOpsIter::new(oplog, &self.frontier, merge_frontier);
        let mut iter = oplog.get_xf_operations_full(&self.frontier, merge_frontier);

        for (_time, origin_op, xf) in &mut iter {
            match (origin_op.tag, xf) {
                (InsDelTag::Ins, BaseMoved(pos)) => {
                    // println!("Insert '{}' at {} (len {})", op.content, ins_pos, op.len());
                    debug_assert!(origin_op.content_pos.is_some()); // Ok if this is false - we'll just fill with junk.
                    let content = origin_op.get_content(oplog).unwrap();
                    assert!(pos <= self.content.len_chars());
                    if origin_op.span.fwd {
                        self.content.insert(pos, content);
                    } else {
                        // We need to insert the content in reverse order.
                        let c = reverse_str(content);
                        self.content.insert(pos, &c);
                    }
                }

                (_, DeleteAlreadyHappened) => {}, // Discard.

                (InsDelTag::Del, BaseMoved(pos)) => {
                    let del_end = pos + origin_op.len();
                    debug_assert!(self.content.len_chars() >= del_end);
                    // println!("Delete {}..{} (len {}) '{}'", del_start, del_end, mut_len, to.content.slice_chars(del_start..del_end).collect::<String>());
                    self.content.remove(pos..del_end);
                }
            }
        }

        self.frontier = iter.into_frontier();
    }

    #[deprecated]
    pub fn merge_old(&mut self, oplog: &OpLog, merge_frontier: &[Time]) {
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

        let mut shared_size = 0;
        let mut shared_ranges = 0;

        let mut common_ancestor = oplog.history.find_conflicting(&self.frontier, merge_frontier, |span, flag| {
            // Note we'll be visiting these operations in reverse order.

            if flag == DiffFlag::Shared {
                shared_size += span.len();
                shared_ranges += 1;
            }

            // dbg!(&span, flag);
            let target = match flag {
                DiffFlag::OnlyB => &mut new_ops,
                _ => &mut conflict_ops
            };
            target.push_reversed_rle(span);

            #[cfg(feature = "dot_export")]
            if MAKE_GRAPHS {
                let color = match flag {
                    DiffFlag::OnlyA => Blue,
                    DiffFlag::OnlyB => Green,
                    DiffFlag::Shared => Grey,
                };
                dbg_all_ops.push((span, color));
            }
        });

        #[cfg(feature = "dot_export")]
        if MAKE_GRAPHS {
            let s1 = merge_frontier.iter().map(|t| name_of(*t)).collect::<Vec<_>>().join("-");
            let s2 = self.frontier.iter().map(|t| name_of(*t)).collect::<Vec<_>>().join("-");

            // let filename = format!("../../svgs/m{}_to_{}.svg", s1, s2);
            let filename = format!("svgs/m{}_to_{}.svg", s1, s2);
            let content = self.content.to_string();
            oplog.make_merge_graph(&filename, &content, dbg_all_ops.iter().copied());
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
                    let txn = oplog.history.entries.find_packed(span.start);
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
                        self.apply_range_from(oplog, span);
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

            // This sometimes adds the FF'ed ops to the conflict_ops set so we add them to the
            // merge set. This is a pretty bad way to do this - if we're gonna add them to
            // conflict_ops then FF is pointless.
            conflict_ops.clear();
            shared_size = 0;
            common_ancestor = oplog.history.find_conflicting(&self.frontier, merge_frontier, |span, flag| {
                if flag == DiffFlag::Shared {
                    shared_size += span.len();
                    shared_ranges += 1;
                }

                if flag != DiffFlag::OnlyB {
                    conflict_ops.push_reversed_rle(span);
                }
            });
        }

        // if shared_size > 0 {
        //     // println!("Shared size {} in {} ranges", shared_size, shared_ranges);
        //     if frontier_is_root(&common_ancestor) {
        //         // println!("(Common ancestor is ROOT!)");
        //     }
        // }

        // TODO: Also FF at the end!

        // For conflicting operations, we'll make a tracker starting at the common_ancestor and
        // containing the conflicting_ops set. (Which is everything that is either common, or only
        // in this branch).
        let mut tracker = M2Tracker::new();
        let frontier = tracker.walk(oplog, common_ancestor, &conflict_ops, None);

        // Then walk through and merge any new edits.
        tracker.walk(oplog, frontier, &new_ops, Some(self));

        // ... And update our frontier.
        // for range in new_ops.into_iter().rev() {
        //     advance_frontier_by(&mut self.frontier, &opset.history, range);
        // }
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
        list.oplog.push_insert_at(0, &[ROOT_TIME], 0, "aaa");

        list.branch.merge(&list.oplog, &[1]);
        list.branch.merge(&list.oplog, &[2]);

        assert_eq!(list.branch.frontier.as_slice(), &[2]);
        assert_eq!(list.branch.content, "aaa");
    }

    #[test]
    fn test_ff_merge() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        list.oplog.push_insert_at(0, &[ROOT_TIME], 0, "aaa");
        list.oplog.push_insert_at(1, &[ROOT_TIME], 0, "bbb");
        list.branch.merge(&list.oplog, &[2, 5]);

        assert_eq!(list.branch.frontier.as_slice(), &[2, 5]);
        assert_eq!(list.branch.content, "aaabbb");

        list.oplog.push_insert_at(0, &[2, 5], 0, "ccc"); // 8
        list.branch.merge(&list.oplog, &[8]);

        assert_eq!(list.branch.frontier.as_slice(), &[8]);
        assert_eq!(list.branch.content, "cccaaabbb");
    }

    #[test]
    fn test_merge_inserts() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        list.oplog.push_insert_at(0, &[ROOT_TIME], 0, "aaa");
        list.oplog.push_insert_at(1, &[ROOT_TIME], 0, "bbb");

        list.branch.merge(&list.oplog, &[2, 5]);
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

        list.insert(0, 0, "aaa");
        // list.ops.push_insert(0, &[ROOT_TIME], 0, "aaa");

        list.oplog.push_delete_at(0, &[2], 1, 1); // &[3]
        list.oplog.push_delete_at(1, &[2], 0, 3); // &[6]

        // M2Tracker::apply_to_checkout(&mut list.checkout, &list.ops, (0..list.ops.len()).into());
        // list.checkout.merge_changes_m2(&list.ops, (3..list.ops.len()).into());
        list.branch.merge(&list.oplog, &[3, 6]);
        assert_eq!(list.branch.content, "");
    }

    #[test]
    fn test_merge_deletes_2() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        let t = list.oplog.push_insert_at(0, &[ROOT_TIME], 0, "aaa");
        list.oplog.push_delete_at(0, &[t], 1, 1); // 3
        list.oplog.push_delete_at(1, &[t], 0, 3); // 6
        // dbg!(&list.ops);

        // list.checkout.merge_changes_m2(&list.ops, (0..list.ops.len()).into());
        list.branch.merge(&list.oplog, &[3, 6]);
        dbg!(&list.branch);
        // assert_eq!(list.checkout.content, "");
    }

    #[test]
    fn test_concurrent_insert() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");
        // list.local_insert(0, 0, "aaa");

        list.oplog.push_insert_at(0, &[ROOT_TIME], 0, "aaa");
        list.oplog.push_insert_at(1, &[ROOT_TIME], 0, "bbb");

        let mut t = M2Tracker::new();
        t.apply_range(&list.oplog, (0..3).into(), None);
        t.retreat_by_range((0..3).into());
        t.apply_range(&list.oplog, (3..6).into(), None);
        // dbg!(&t);
        // t.apply_range_at_version()
    }

    #[test]
    fn test_concurrent_delete() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.get_or_create_agent_id("b");

        list.insert(0, 0, "aaa");

        list.oplog.push_delete_at(0, &[2], 1, 1);
        list.oplog.push_delete_at(1, &[2], 0, 3);

        let mut t = M2Tracker::new();
        t.apply_range(&list.oplog, (0..4).into(), None);
        t.retreat_by_range((3..4).into());
        t.apply_range(&list.oplog, (4..7).into(), None);
        dbg!(&t);
        // t.apply_range_at_version()
    }

    #[test]
    fn foo() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("seph");
        list.insert(0, 0, "hi there");
        list.delete(0, 2, 3);

        let mut t = M2Tracker::new();

        let end = list.oplog.len();
        dbg!(end);
        t.apply_range(&list.oplog, (0..end).into(), None);

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
        t = list.oplog.push_insert_at(0, &[t], 0, "abc"); // 2
        t = list.oplog.push_delete_at(0, &[t], 2, 1); // 3 -> "ab_"
        t = list.oplog.push_delete_at(0, &[t], 1, 1); // 4 -> "a__"
        t = list.oplog.push_delete_at(0, &[t], 0, 1); // 5 -> "___"
        assert_eq!(t, 5);

        let mut t = M2Tracker::new();
        t.apply_range(&list.oplog, (3..6).into(), None);
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
        t = list.oplog.push_insert_at(0, &[t], 0, "c");
        t = list.oplog.push_insert_at(0, &[t], 0, "b");
        t = list.oplog.push_insert_at(0, &[t], 0, "a");

        dbg!(&list.oplog);
        list.branch.merge(&list.oplog, &[t]);
        dbg!(&list.branch);
    }

    #[test]
    fn test_ff_2() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("a");
        list.oplog.push_insert_at(0, &[ROOT_TIME], 0, "aaa");

        let iter = TransformedOpsIter::new(&list.oplog, &[ROOT_TIME], &list.oplog.frontier);
        dbg!(&iter);
        for x in iter {
            dbg!(x);
        }
        // list.branch.merge(&list.oplog, &[1]);
        // list.branch.merge(&list.oplog, &[2]);
        //
        // assert_eq!(list.branch.frontier.as_slice(), &[2]);
        // assert_eq!(list.branch.content, "aaa");
    }

}