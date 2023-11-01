// Clippy complains about .as_mut_ref() below. But that construction is needed for the borrow
// checker.
#![allow(clippy::needless_option_as_deref)]

use std::cmp::Ordering;
use std::ptr::NonNull;
use jumprope::JumpRopeBuf;
use smallvec::{SmallVec, smallvec};
use smartstring::alias::String as SmartString;
use content_tree::*;
use rle::{AppendRle, HasLength, MergeableIterator, Searchable, Trim, TrimCtx};
use rle::intersect::rle_intersect_rev;
use crate::listmerge::{DocRangeIndex, M2Tracker, SpaceIndex};
use crate::listmerge::yjsspan::{INSERTED, NOT_INSERTED_YET, FugueSpan};
use crate::list::operation::{ListOpKind, TextOperation};
use crate::dtrange::{DTRange, UNDERWATER_START};
use crate::rle::{KVPair, RleSpanHelpers, RleVec};
use crate::{AgentId, CausalGraph, Frontier, LV};
use crate::causalgraph::agent_assignment::AgentAssignment;
use crate::causalgraph::graph::tools::DiffFlag;
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::buffered_iter::BufferedIter;
use crate::rev_range::RangeRev;

#[cfg(feature = "dot_export")]
use crate::listmerge::dot::{DotColor, name_of};
#[cfg(feature = "dot_export")]
use crate::listmerge::dot::DotColor::*;

use crate::listmerge::markers::Marker::{DelTarget, InsPtr};
use crate::listmerge::markers::MarkerEntry;
use crate::listmerge::merge::TransformedResult::{BaseMoved, DeleteAlreadyHappened};
use crate::listmerge::metrics::upstream_cursor_pos;
use crate::listmerge::txn_trace::SpanningTreeWalker;
use crate::list::op_iter::OpMetricsIter;
use crate::causalgraph::graph::Graph;
use crate::textinfo::TextInfo;
use crate::frontier::local_frontier_eq;
use crate::list::ListOpLog;
#[cfg(feature = "ops_to_old")]
use crate::listmerge::to_old::OldCRDTOpInternal;
use crate::unicount::consume_chars;

const ALLOW_FF: bool = true;

#[cfg(feature = "dot_export")]
const MAKE_GRAPHS: bool = false;

fn pad_index_to(index: &mut SpaceIndex, desired_len: usize) {
    // TODO: Use dirty tricks to avoid this for more performance.
    let index_len = index.len();

    if index_len < desired_len {
        index.push(MarkerEntry {
            len: desired_len - index_len,
            inner: InsPtr(std::ptr::NonNull::dangling()),
        });
    }
}

pub(super) fn notify_for(index: &mut SpaceIndex) -> impl FnMut(FugueSpan, NonNull<NodeLeaf<FugueSpan, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>>) + '_ {
    move |entry: FugueSpan, leaf| {
        debug_assert!(leaf != NonNull::dangling());
        let start = entry.id.start;
        let len = entry.len();

        // Note we can only mutate_entries when we have something to mutate. The list is started
        // with a big placeholder "underwater" entry which will be split up as needed.

        let mut cursor = index.unsafe_cursor_at_offset_pos(start, false);
        unsafe {
            ContentTreeRaw::unsafe_mutate_entries_notify(|marker| {
                // The item should already be an insert entry.
                debug_assert_eq!(marker.inner.tag(), ListOpKind::Ins);

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
        let underwater = FugueSpan::new_underwater();
        pad_index_to(&mut index, underwater.id.end);
        range_tree.push_notify(underwater, notify_for(&mut index));

        Self {
            range_tree,
            index,
            #[cfg(feature = "merge_conflict_checks")]
            concurrent_inserts_collide: false,
            #[cfg(feature = "ops_to_old")]
            dbg_ops: vec![]
        }
    }

    pub(super) fn marker_at(&self, lv: LV) -> NonNull<NodeLeaf<FugueSpan, DocRangeIndex>> {
        let cursor = self.index.cursor_at_offset_pos(lv, false);
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

    fn get_cursor_before(&self, lv: LV) -> Cursor<FugueSpan, DocRangeIndex> {
        if lv == usize::MAX {
            // This case doesn't seem to ever get hit by the fuzzer. It might be equally correct to
            // just panic() here.
            self.range_tree.cursor_at_end()
        } else {
            let marker = self.marker_at(lv);
            self.range_tree.cursor_before_item(lv, marker)
        }
    }

    // pub(super) fn get_unsafe_cursor_after(&self, time: Time, stick_end: bool) -> UnsafeCursor<YjsSpan2, DocRangeIndex> {
    fn get_cursor_after(&self, lv: LV, stick_end: bool) -> Cursor<FugueSpan, DocRangeIndex> {
        if lv == usize::MAX {
            self.range_tree.cursor_at_start()
        } else {
            let marker = self.marker_at(lv);
            // let marker: NonNull<NodeLeaf<YjsSpan, ContentIndex>> = self.markers.at(order as usize).unwrap();
            // self.content_tree.
            let mut cursor = self.range_tree.cursor_before_item(lv, marker);
            // The cursor points to parent. This is safe because of guarantees provided by
            // cursor_before_item.
            cursor.offset += 1;
            if !stick_end { cursor.roll_to_next_entry(); }
            cursor
        }
    }

    // TODO: Rewrite this to take a MutCursor instead of UnsafeCursor argument.
    pub(super) fn integrate(&mut self, aa: &AgentAssignment, agent: AgentId, item: FugueSpan, mut cursor: UnsafeCursor<FugueSpan, DocRangeIndex>) -> usize {
        debug_assert!(item.len() > 0);

        // Ok now that's out of the way, lets integrate!
        cursor.roll_to_next_entry();

        // These are almost never used. Could avoid the clone here... though its pretty cheap.
        let left_cursor = cursor.clone();
        let mut scan_start = cursor.clone();
        let mut scanning = false;

        loop {
            if !cursor.roll_to_next_entry() { break; } // End of the document
            let other_entry: FugueSpan = *cursor.get_raw_entry();

            // When concurrent edits happen, the range of insert locations goes from the insert
            // position itself (passed in through cursor) to the next item which existed at the
            // time in which the insert occurred.
            // This test is almost always true. (Ie, we basically always break here).
            if other_entry.state != NOT_INSERTED_YET { break; }

            debug_assert_eq!(cursor.offset, 0);

            // When preparing example data, its important that the data can merge the same
            // regardless of editing trace (so the output isn't dependent on the algorithm used to
            // merge).
            #[cfg(feature = "merge_conflict_checks")] {
                //println!("Concurrent changes {:?} vs {:?}", item.id, other_entry.id);
                self.concurrent_inserts_collide = true;
            }

            // This code could be better optimized, but its already O(n * log n), and its extremely
            // rare that you actually get concurrent inserts at the same location in the document
            // anyway.

            let other_left_lv = other_entry.origin_left_at_offset(cursor.offset);
            let other_left_cursor = self.get_cursor_after(other_left_lv, false);

            // YjsMod semantics
            match unsafe { other_left_cursor.unsafe_cmp(&left_cursor) } {
                Ordering::Less => { break; } // Top row
                Ordering::Greater => {} // Bottom row. Continue.
                Ordering::Equal => {
                    if item.right_parent == other_entry.right_parent {
                        // Origin_right matches. Items are concurrent. Order by agent names.
                        let my_name = aa.get_agent_name(agent);

                        // It would be "more correct" to take the other entry at the cursor offset,
                        // but the cursor offset is always 0 when comparing items. So eh.
                        // let other_lv = other_entry.at_offset(cursor.offset);
                        let other_lv = other_entry.id.start;

                        let (other_agent, other_seq) = aa.local_to_agent_version(other_lv);
                        let other_name = aa.get_agent_name(other_agent);
                        // eprintln!("concurrent insert at the same place {} ({}) vs {} ({})", item.id.start, my_name, other_lv, other_name);

                        // Its possible for a user to conflict with themself if they commit to
                        // multiple branches. In this case, sort by seq number.
                        let ins_here = match my_name.cmp(other_name) {
                            Ordering::Less => true,
                            Ordering::Equal => {
                                // We can't compare versions here because sequence numbers could be
                                // used out of order, and the relative version ordering isn't
                                // consistent in that case.
                                //
                                // We could cache this but this code doesn't run often anyway.
                                let item_seq = aa.local_to_agent_version(item.id.start).1;
                                item_seq < other_seq
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
                        // Note that this is only valid because cursor offset is always 0 when this
                        // code is executed. (We'd need to take usize::MAX for other_right_cursor
                        // if cursor.offset > 0).
                        let my_right_cursor = self.get_cursor_before(item.right_parent);
                        let other_right_cursor = self.get_cursor_before(other_entry.right_parent);

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

    fn apply_range(&mut self, aa: &AgentAssignment, op_ctx: &ListOperationCtx, ops: &RleVec<KVPair<ListOpMetrics>>, range: DTRange, mut to: Option<&mut JumpRopeBuf>) {
        if range.is_empty() { return; }

        // if let Some(to) = to.as_deref_mut() {
        //     to.version.advance(&cg.parents, range);
        // }

        let mut iter = OpMetricsIter::new(ops, op_ctx, range);
        // let mut iter = OpMetricsIter::new(&text_info.ops, &text_info.ctx, range);
        while let Some(mut pair) = iter.next() {
            loop {
                let span = aa.local_span_to_agent_span(pair.span());

                let len = span.len();
                let remainder = pair.trim_ctx(len, iter.ctx);

                let content = iter.get_content(&pair);

                self.apply_to(aa, op_ctx, span.agent, &pair, content, to.as_deref_mut());

                if let Some(r) = remainder {
                    pair = r;
                } else { break; }
            }
        }
    }

    fn apply_to(&mut self, aa: &AgentAssignment, ctx: &ListOperationCtx, agent: AgentId, op_pair: &KVPair<ListOpMetrics>, content: Option<&str>, mut to: Option<&mut JumpRopeBuf>) {
        let mut op_pair = op_pair.clone();

        loop {
            // STATS.with(|s| {
            //     let mut s = s.borrow_mut();
            //     s.0 += 1;
            // });

            let (len_here, transformed_pos) = self.apply(aa, agent, &op_pair, usize::MAX);

            let remainder = op_pair.trim_ctx(len_here, ctx);

            // dbg!((&op_pair, len_here, transformed_pos));
            if let BaseMoved(pos) = transformed_pos {
                if let Some(to) = to.as_mut() {
                    // Apply the operation here.
                    match op_pair.1.kind {
                        ListOpKind::Ins => {
                            // dbg!(&self.range_tree);
                            // println!("Insert '{}' at {} (len {})", op.content, ins_pos, op.len());
                            debug_assert!(op_pair.1.content_pos.is_some()); // Ok if this is false - we'll just fill with junk.
                            let content = content.unwrap();
                            assert!(pos <= to.len_chars());
                            to.insert(pos, content);
                        }
                        ListOpKind::Del => {
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
                debug_assert_ne!(op_pair.1.kind, ListOpKind::Ins);
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
    fn apply(&mut self, aa: &AgentAssignment, agent: AgentId, op_pair: &KVPair<ListOpMetrics>, max_len: usize) -> (usize, TransformedResult) {
        // self.check_index();
        // The op must have been applied at the branch that the tracker is currently at.
        let len = max_len.min(op_pair.len());
        let op = &op_pair.1;

        // dbg!(op);
        match op.kind {
            ListOpKind::Ins => {
                if !op.loc.fwd { unimplemented!("Implement me!") }

                // To implement this we need to:
                // 1. Find the item directly before the requested position. This is our origin-left.
                // 2. Scan forward until the next item which isn't in the not yet inserted state.
                // this is our origin right.
                // 3. Use the integrate() method to actually insert - since we need to handle local
                // conflicts.

                // UNDERWATER_START = 4611686018427387903

                let (origin_left, mut cursor) = if op.start() == 0 {
                    (usize::MAX, self.range_tree.mut_cursor_at_start())
                } else {
                    let mut cursor = self.range_tree.mut_cursor_at_content_pos(op.start() - 1, false);
                    // dbg!(&cursor, cursor.get_raw_entry());
                    let origin_left = cursor.get_item().unwrap();
                    assert!(cursor.next_item());
                    (origin_left, cursor)
                };

                // Origin_right should be the next item which isn't in the NotInsertedYet state.
                // If we reach the end of the document before that happens, use usize::MAX.
                //
                // TODO: _origin_right is only used when converting to old operations. Remove it
                // from the normal execution path here.
                let (_origin_right, right_parent) = 'block: {
                    if cursor.roll_to_next_entry() { // Check we aren't already at the end of the list
                        // loop through c2 until we find the right hand bound.
                        let mut c2 = cursor.clone();
                        while let Some(e) = c2.try_get_raw_entry() {
                            if e.state != NOT_INSERTED_YET {
                                // We can use this.
                                let origin_right = e.at_offset(c2.offset);

                                let left = c2.get_raw_entry().origin_left_at_offset(c2.offset);
                                // If right.left == origin_left then we're a left child with a right_parent of
                                // right_bound. Otherwise we're a right child, and right_parent
                                // is null. (Our parent is origin_left).
                                let right_parent = if left == origin_left { origin_right } else { usize::MAX };
                                break 'block (origin_right, right_parent);
                            }

                            // if next_entry returns false, we've hit the end of the list.
                            if !c2.next_entry() { break; }
                        }
                    }

                    (usize::MAX, usize::MAX)
                };

                // let origin_right = cursor.get_item().unwrap_or(ROOT_TIME);

                let mut lv_span = op_pair.span();
                lv_span.trim(len);

                let item = FugueSpan {
                    id: lv_span,
                    origin_left,
                    right_parent,
                    state: INSERTED,
                    ever_deleted: false,
                };

                #[cfg(feature = "ops_to_old")] {
                    self.dbg_ops.push_rle(OldCRDTOpInternal::Ins {
                        id: lv_span,
                        origin_left,
                        origin_right: if _origin_right == UNDERWATER_START { usize::MAX } else { _origin_right },
                        content_pos: op.content_pos.unwrap(),
                    });
                }

                // This is dirty because the cursor's lifetime is not associated with self.
                let cursor = cursor.inner;
                let ins_pos = self.integrate(aa, agent, item, cursor);
                // self.range_tree.check();
                // self.check_index();

                (len, BaseMoved(ins_pos))
            }

            ListOpKind::Del => {
                // Delete as much as we can. We might not be able to delete everything because of
                // double deletes and inserts inside the deleted range. This is extra annoying
                // because we need to move backwards through the deleted items if we're rev.
                debug_assert!(op.len() > 0);
                // let mut remaining_len = op.len();

                let fwd = op.loc.fwd;

                let (mut cursor, len) = if fwd {
                    let start_pos = op.start();
                    let cursor = self.range_tree.mut_cursor_at_content_pos(start_pos, false);
                    (cursor, len)
                } else {
                    // We're moving backwards. We need to delete as many items as we can before the
                    // end of the op.
                    let last_pos = op.loc.span.last();
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

                let lv_start = op_pair.0;

                #[cfg(feature = "ops_to_old")] {
                    self.dbg_ops.push_rle(OldCRDTOpInternal::Del {
                        start_v: lv_start,
                        target: RangeRev {
                            span: target,
                            fwd
                        }
                    });
                }

                // if !is_underwater(target.start) {
                //     // Deletes must always dominate the item they're deleting in the time dag.
                //     debug_assert!(cg.parents.version_contains_time(&[lv_start], target.start));
                // }

                self.index.replace_range_at_offset(lv_start, MarkerEntry {
                    len,
                    inner: DelTarget(RangeRev {
                        span: target,
                        fwd
                    })
                });

                // if cfg!(debug_assertions) {
                //     self.check_index();
                // }

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
    pub(super) fn walk(&mut self, graph: &Graph, aa: &AgentAssignment, op_ctx: &ListOperationCtx, ops: &RleVec<KVPair<ListOpMetrics>>, start_at: Frontier, rev_spans: &[DTRange], mut apply_to: Option<&mut JumpRopeBuf>) -> Frontier {
        let mut walker = SpanningTreeWalker::new(graph, rev_spans, start_at);

        for walk in &mut walker {
            for range in walk.retreat {
                self.retreat_by_range(range);
            }

            for range in walk.advance_rev.into_iter().rev() {
                self.advance_by_range(range);
            }

            debug_assert!(!walk.consume.is_empty());
            self.apply_range(aa, op_ctx, ops, walk.consume, apply_to.as_deref_mut());
        }

        walker.into_frontier()
    }
}

#[derive(Debug)]
pub(crate) struct TransformedOpsIter<'a> {
    // oplog: &'a ListOpLog,
    // cg: &'a CausalGraph,
    subgraph: &'a Graph,
    aa: &'a AgentAssignment,
    op_ctx: &'a ListOperationCtx,
    ops: &'a RleVec<KVPair<ListOpMetrics>>,

    op_iter: Option<BufferedIter<OpMetricsIter<'a>>>,
    ff_mode: bool,
    // ff_idx: usize,
    did_ff: bool, // TODO: Do I really need this?

    merge_frontier: Frontier,

    common_ancestor: Frontier,
    conflict_ops: SmallVec<[DTRange; 4]>,
    new_ops: SmallVec<[DTRange; 4]>,

    next_frontier: Frontier,

    // TODO: This tracker allocates - which we don't need to do if we're FF-ing.
    phase2: Option<(M2Tracker, SpanningTreeWalker<'a>)>,
}

impl<'a> TransformedOpsIter<'a> {
    #[allow(unused)]
    pub(crate) fn count_range_tracker_size(&self) -> usize {
        self.phase2.as_ref()
            .map(|(tracker, _)| { tracker.range_tree.count_entries() })
            .unwrap_or_default()
    }

    pub(crate) fn new(subgraph: &'a Graph, aa: &'a AgentAssignment, op_ctx: &'a ListOperationCtx, ops: &'a RleVec<KVPair<ListOpMetrics>>, from_frontier: &[LV], merge_frontier: &[LV]) -> Self {
        // The strategy here looks like this:
        // We have some set of new changes to merge with a unified set of parents.
        // 1. Find the parent set of the spans to merge
        // 2. Generate the conflict set, and make a tracker for it (by iterating all the conflicting
        //    changes).
        // 3. Use OptTxnIter to iterate through the (new) merge set, merging along the way.

        // let mut diff = opset.history.diff(&self.frontier, merge_frontier);

        // First lets see what we've got. I'll divide the conflicting range into two groups:
        // - The new operations we need to merge
        // - The conflict set. Ie, stuff we need to build a tracker around.
        //
        // Both of these lists are in reverse time order(!).
        let mut new_ops: SmallVec<[DTRange; 4]> = smallvec![];
        let mut conflict_ops: SmallVec<[DTRange; 4]> = smallvec![];

        let common_ancestor = subgraph.find_conflicting(from_frontier, merge_frontier, |span, flag| {
            // Note we'll be visiting these operations in reverse order.

            // dbg!(&span, flag);
            let target = match flag {
                DiffFlag::OnlyB => &mut new_ops,
                _ => &mut conflict_ops
            };
            target.push_reversed_rle(span);
        });

        common_ancestor.debug_check_sorted();

        // dbg!(&opset.history);
        // dbg!((&new_ops, &conflict_ops, &common_ancestor));


        Self {
            subgraph,
            aa,
            op_ctx,
            ops,
            op_iter: None,
            ff_mode: true,
            did_ff: false,
            merge_frontier: Frontier::from(merge_frontier),
            common_ancestor,
            conflict_ops,
            new_ops,
            next_frontier: Frontier::from(from_frontier),
            phase2: None,
        }
    }

    pub(crate) fn into_frontier(self) -> Frontier {
        self.next_frontier
    }

    /// Returns if concurrent inserts ever collided at the same location while traversing.
    #[cfg(feature = "merge_conflict_checks")]
    pub(crate) fn concurrent_inserts_collided(&self) -> bool {
        self.phase2.as_ref().map_or(false, |(tracker, _)| {
            tracker.concurrent_inserts_collide
        })
    }
}

// #[derive(Debug)]
// struct FlattenedOps {
//     common_ancestor: Frontier,
//     conflict_ops: SmallVec<[DTRange; 4]>,
//     new_ops: SmallVec<[DTRange; 4]>,
//     from_frontier: Frontier,
//     merge_frontier: Frontier,
// }
//
// impl FlattenedOps {
//     fn new_from_subgraph(cg: &CausalGraph, from_frontier: &[LV], merge_frontier: &[LV], ops: &RleVec<KVPair<ListOpMetrics>>) -> Result<(Graph, Self), Frontier> {
//         // This is a big dirty mess for now, but it should be correct at least.
//         let global_conflict_zone = cg.graph.find_conflicting_simple(from_frontier, merge_frontier);
//         let earliest = global_conflict_zone.common_ancestor.0.get(0).copied().unwrap_or(0);
//
//         let final_frontier_global = cg.graph.find_dominators_2(from_frontier, merge_frontier);
//         // if final_frontier.as_ref() == from { return final_frontier; } // Nothing to do!
//
//         // We actually only need the ops in intersection b
//         let op_spans = ops.iter().map(|e| e.span())
//             .rev()
//             // .merge_spans_rev()
//             .take_while(|r| r.end > earliest);
//         // let iter = rle_intersect_rev_first(op_spans, global_conflict_zone.rev_spans.iter().copied());
//         let iter = op_spans;
//
//         let (subgraph, _ff) = cg.graph.subgraph_raw(iter.clone(), final_frontier_global.as_ref());
//
//         // println!("{}", subgraph.0.0.len());
//         // subgraph.dbg_check_subgraph(true); // For debugging.
//         // dbg!(&subgraph, ff.as_ref());
//
//         let from_frontier = cg.graph.project_onto_subgraph_raw(iter.clone(), from_frontier);
//         let merge_frontier = cg.graph.project_onto_subgraph_raw(iter.clone(), merge_frontier);
//
//         let mut new_ops: SmallVec<[DTRange; 4]> = smallvec![];
//         let mut conflict_ops: SmallVec<[DTRange; 4]> = smallvec![];
//
//         // Process the conflicting edits again, this time just scanning the subgraph.
//         let common_ancestor = subgraph.find_conflicting(from_frontier.as_ref(), merge_frontier.as_ref(), |span, flag| {
//             // Note we'll be visiting these operations in reverse order.
//             let target = match flag {
//                 DiffFlag::OnlyB => &mut new_ops,
//                 _ => &mut conflict_ops
//             };
//             target.push_reversed_rle(span);
//         });
//         // dbg!(&common_ancestor);
//
//         let final_frontier_subgraph = subgraph.find_dominators_2(from_frontier.as_ref(), merge_frontier.as_ref());
//         if final_frontier_subgraph == from_frontier { return Result::Err(final_frontier_global); } // Nothing to do! Just an optimization... Not sure if its necessary.
//
//         // dbg!(ops.iter().map(|e| e.span())
//         //     .rev()
//         //     .take_while(|r| r.end > earliest).collect::<Vec<_>>());
//         // dbg!(&subgraph);
//         // dbg!(&new_ops);
//
//         Result::Ok((subgraph, Self {
//             from_frontier,
//             merge_frontier,
//             common_ancestor,
//             conflict_ops,
//             new_ops,
//         }))
//     }
//
//     fn iter<'a>(self, subgraph: &'a Graph, aa: &'a AgentAssignment, op_ctx: &'a ListOperationCtx, ops: &'a RleVec<KVPair<ListOpMetrics>>) -> TransformedOpsIter<'a> {
//         TransformedOpsIter {
//             subgraph,
//             aa,
//             op_ctx,
//             ops,
//             op_iter: None,
//             ff_mode: true,
//             did_ff: false,
//             merge_frontier: self.merge_frontier,
//             common_ancestor: self.common_ancestor,
//             conflict_ops: self.conflict_ops,
//             new_ops: self.new_ops,
//             next_frontier: self.from_frontier,
//             phase2: None,
//         }
//     }
// }

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum TransformedResult {
    BaseMoved(usize),
    DeleteAlreadyHappened,
}

type TransformedTriple = (LV, ListOpMetrics, TransformedResult);

impl TransformedResult {
    fn not_moved(op_pair: KVPair<ListOpMetrics>) -> TransformedTriple {
        let start = op_pair.1.start();
        (op_pair.0, op_pair.1, TransformedResult::BaseMoved(start))
    }
}

impl<'a> Iterator for TransformedOpsIter<'a> {
    /// Iterator over transformed operations. The KVPair.0 holds the original time of the operation.
    type Item = (LV, ListOpMetrics, TransformedResult);

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
            let txn = self.subgraph.entries.find_packed(span.start);
            let can_ff = txn.with_parents(span.start, |parents: &[LV]| {
                local_frontier_eq(&self.next_frontier, parents)
            });

            if can_ff {
                let mut span = self.new_ops.pop().unwrap();

                let remainder = span.trim(txn.span.end - span.start);

                debug_assert!(!span.is_empty());

                self.next_frontier = Frontier::new_1(span.last());

                if let Some(r) = remainder {
                    self.new_ops.push(r);
                }
                self.did_ff = true;

                let mut iter = OpMetricsIter::new(self.ops, self.op_ctx, span);

                // Pull the first item off the iterator and keep it for later.
                // A fresh iterator should always return something!
                let result = iter.next().unwrap();
                // assert!(result.is_some());

                self.op_iter = Some(iter.into());
                // println!("FF {:?}", result);
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
                    self.common_ancestor = self.subgraph.find_conflicting(self.next_frontier.as_ref(), self.merge_frontier.as_ref(), |span, flag| {
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
            None => {
                // First time through this code we'll end up here. Walk the conflicting
                // operations to populate the tracker and walker structures.
                let mut tracker = M2Tracker::new();
                // dbg!(&self.conflict_ops);
                let frontier = tracker.walk(
                    self.subgraph, self.aa,
                    self.op_ctx,
                    self.ops,
                    std::mem::take(&mut self.common_ancestor),
                    &self.conflict_ops,
                    None);
                // dbg!(&tracker);

                let walker = SpanningTreeWalker::new(self.subgraph, &self.new_ops, frontier);
                self.phase2 = Some((tracker, walker));
                // This is a kinda gross way to do this. TODO: Rewrite without .unwrap() somehow?
                self.phase2.as_mut().unwrap()
            },
            Some(phase2) => phase2,
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

            // dbg!(&walk.consume, &tracker);
            assert!(!walk.consume.is_empty());

            // Only really advancing the frontier so we can consume into it. The resulting frontier
            // is interesting in lots of places.
            //
            // The walker can be unwrapped into its inner frontier, but that won't include
            // everything. (TODO: Look into fixing that?)
            self.next_frontier.advance(self.subgraph, walk.consume);
            self.op_iter = Some(OpMetricsIter::new(self.ops, self.op_ctx, walk.consume).into());
        };

        // Ok, try to consume as much as we can from pair.
        let span = self.aa.local_span_to_agent_span(pair.span());
        let len = span.len().min(pair.len());

        let (consumed_here, xf_result) = tracker.apply(self.aa, span.agent, &pair, len);

        let remainder = pair.trim_ctx(consumed_here, self.op_ctx);

        // (Time, OperationInternal, TransformedResult)
        let result = (pair.0, pair.1, xf_result);

        if let Some(r) = remainder {
            op_iter.push_back(r);
        }

        Some(result)
        // TODO: Also FF at the end!
    }
}

pub fn reverse_str(s: &str) -> SmartString {
    let mut result = SmartString::new();
    result.extend(s.chars().rev());
    result
}

impl TextInfo {
    pub(crate) fn get_xf_operations_full<'a>(&'a self, subgraph: &'a Graph, aa: &'a AgentAssignment, from: &[LV], merging: &[LV]) -> TransformedOpsIter<'a> {
        TransformedOpsIter::new(subgraph, aa, &self.ctx, &self.ops, from, merging)
    }

    pub(crate) fn with_xf_iter<F: FnOnce(TransformedOpsIter, Frontier) -> R, R>(&self, cg: &CausalGraph, from: &[LV], merge_frontier: &[LV], f: F) -> R {
        // This is a big dirty mess for now, but it should be correct at least.
        let conflict = cg.graph.find_conflicting_simple(from, merge_frontier);

        let final_frontier = cg.graph.find_dominators_2(from, merge_frontier);
        // if final_frontier.as_ref() == from { return final_frontier; } // Nothing to do!

        // This looks inefficient - since after all, we only care about the operations in the
        // conflict zone. But because we scan the intersection of these operations and the conflict,
        // and scan them backwards, it works out to be efficient in practice.
        let op_spans = self.ops.iter().map(|e| e.span())
            .rev()
            .merge_spans_rev();

        // We create the subgraph from operations which intersect:
        // - The graph passed in
        // - The conflict zone between from -> merge_frontier
        // - The operations on this text document
        let iter = rle_intersect_rev(op_spans, conflict.rev_spans.iter().copied())
            .map(|pair| pair.0);

        let (subgraph, _ff) = cg.graph.subgraph_raw(iter.clone(), final_frontier.as_ref());

        // println!("{}", subgraph.0.0.len());
        // subgraph.dbg_check_subgraph(true); // For debugging.
        // dbg!(&subgraph, ff.as_ref());

        let from = cg.graph.project_onto_subgraph_raw(iter.clone(), from);
        let merge_frontier = cg.graph.project_onto_subgraph_raw(iter.clone(), merge_frontier);

        // let mut iter = TransformedOpsIter::new(oplog, &self.frontier, merge_frontier);
        let iter = self.get_xf_operations_full(&subgraph, &cg.agent_assignment, from.as_ref(), merge_frontier.as_ref());
        f(iter, final_frontier)
    }

    /// Iterate through all the *transformed* operations from some point in time. Internally, the
    /// OpLog stores all changes as they were when they were created. This makes a lot of sense from
    /// CRDT academic point of view (and makes signatures and all that easy). But its is rarely
    /// useful for a text editor.
    ///
    /// `get_xf_operations` returns an iterator over the *transformed changes*. That is, the set of
    /// changes that could be applied linearly to a document to bring it up to date.
    pub fn xf_operations_from<'a>(&'a self, cg: &'a CausalGraph, from: &[LV], merging: &[LV]) -> Vec<(DTRange, Option<TextOperation>)> {
        self.with_xf_iter(cg, from, merging, |iter, _| {
            iter.map(|(lv, mut origin_op, xf)| {
                let len = origin_op.len();
                let op: Option<TextOperation> = match xf {
                    BaseMoved(base) => {
                        origin_op.loc.span = (base..base+len).into();
                        let content = origin_op.get_content(&self.ctx);
                        Some((origin_op, content).into())
                    }
                    DeleteAlreadyHappened => None,
                };
                ((lv..lv + len).into(), op)
            }).collect()
        })
    }

    /// Get all transformed operations from the start of time.
    ///
    /// This is a shorthand for `oplog.xf_operations_from(&[], oplog.local_version)`, but
    /// I hope that future optimizations make this method way faster.
    pub fn iter_xf_operations<'a>(&'a self, cg: &'a CausalGraph) -> Vec<(DTRange, Option<TextOperation>)> {
        self.xf_operations_from(cg, &[], cg.version.as_ref())
    }

    /// Add everything in merge_frontier into the set..
    pub fn merge_into(&self, into: &mut JumpRopeBuf, cg: &CausalGraph, from: &[LV], merge_frontier: &[LV]) -> Frontier {
        self.with_xf_iter(cg, from, merge_frontier, |iter, final_frontier| {
            for (_lv, origin_op, xf) in iter {
                match (origin_op.kind, xf) {
                    (ListOpKind::Ins, BaseMoved(pos)) => {
                        // println!("Insert '{}' at {} (len {})", op.content, ins_pos, op.len());
                        debug_assert!(origin_op.content_pos.is_some()); // Ok if this is false - we'll just fill with junk.
                        let content = origin_op.get_content(&self.ctx).unwrap();
                        assert!(pos <= into.len_chars());
                        if origin_op.loc.fwd {
                            into.insert(pos, content);
                        } else {
                            // We need to insert the content in reverse order.
                            let c = reverse_str(content);
                            into.insert(pos, &c);
                        }
                    }

                    (_, DeleteAlreadyHappened) => {}, // Discard.

                    (ListOpKind::Del, BaseMoved(pos)) => {
                        let del_end = pos + origin_op.len();
                        debug_assert!(into.len_chars() >= del_end);
                        // println!("Delete {}..{} (len {}) '{}'", del_start, del_end, mut_len, to.content.slice_chars(del_start..del_end).collect::<String>());
                        into.remove(pos..del_end);
                    }
                }
            }

            // iter.into_frontier()
            final_frontier
        })
    }


    // /// Add everything in merge_frontier into the set..
    // pub fn merge_into(&self, into: &mut JumpRopeBuf, cg: &CausalGraph, from: &[LV], merge_frontier: &[LV]) -> Frontier {
    //     let (graph, flat) = match FlattenedOps::new_from_subgraph(cg, from, merge_frontier, &self.ops) {
    //         Ok(flat) => { flat }
    //         Err(final_frontier) => { return final_frontier; }
    //     };
    //
    //     let mut iter = flat.iter(&graph, &cg.agent_assignment, &self.ctx, &self.ops);
    //     for (_lv, origin_op, xf) in &mut iter {
    //         match (origin_op.kind, xf) {
    //             (ListOpKind::Ins, BaseMoved(pos)) => {
    //                 // println!("Insert '{}' at {} (len {})", op.content, ins_pos, op.len());
    //                 debug_assert!(origin_op.content_pos.is_some()); // Ok if this is false - we'll just fill with junk.
    //                 let content = origin_op.get_content(&self.ctx).unwrap();
    //                 assert!(pos <= into.len_chars());
    //                 if origin_op.loc.fwd {
    //                     into.insert(pos, content);
    //                 } else {
    //                     // We need to insert the content in reverse order.
    //                     let c = reverse_str(content);
    //                     into.insert(pos, &c);
    //                 }
    //             }
    //
    //             (_, DeleteAlreadyHappened) => {}, // Discard.
    //
    //             (ListOpKind::Del, BaseMoved(pos)) => {
    //                 let del_end = pos + origin_op.len();
    //                 debug_assert!(into.len_chars() >= del_end);
    //                 // println!("Delete {}..{} (len {}) '{}'", del_start, del_end, mut_len, to.content.slice_chars(del_start..del_end).collect::<String>());
    //                 into.remove(pos..del_end);
    //             }
    //         }
    //     }
    //
    //     iter.into_frontier()
    // }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Read;
    use std::ops::Range;
    use rle::{MergeableIterator, SplitableSpan};
    use crate::dtrange::UNDERWATER_START;
    use crate::list::{ListCRDT, ListOpLog};
    use crate::listmerge::simple_oplog::SimpleOpLog;
    use crate::listmerge::yjsspan::{deleted_n_state, DELETED_ONCE, YjsSpanState};
    use crate::unicount::count_chars;
    use super::*;

    #[test]
    fn test_ff() {
        let mut list = SimpleOpLog::new();
        list.add_insert("a", 0, "aaa");

        let mut result = JumpRopeBuf::new();
        list.merge_raw(&mut result, &[], &[1]);
        list.merge_raw(&mut result, &[1], &[2]);

        assert_eq!(result, "aaa");
    }

    #[test]
    fn test_ff_goop() {
        let mut list = SimpleOpLog::new();
        list.add_insert("a", 0, "a");
        list.goop(5);
        list.add_insert("a", 1, "bb");

        let mut result = JumpRopeBuf::new();
        let f1 = list.merge_raw(&mut result, &[], &[5]);
        list.merge_raw(&mut result, f1.as_ref(), &[7]);

        assert_eq!(result, "abb");
    }

    #[test]
    fn test_ff_merge() {
        let mut list = SimpleOpLog::new();

        list.add_insert_at("a", &[], 0, "aaa");
        list.add_insert_at("b", &[], 0, "bbb");
        assert_eq!("aaabbb", list.to_string());

        list.add_insert_at("a", &[2, 5], 0, "ccc"); // 8
        assert_eq!("cccaaabbb", list.to_string());
    }

    #[test]
    fn test_merge_inserts() {
        let mut list = SimpleOpLog::new();
        list.add_insert_at("a", &[], 0, "aaa");
        list.add_insert_at("b", &[], 0, "bbb");

        assert_eq!(list.to_string(), "aaabbb");
    }

    #[test]
    fn test_merge_deletes_1() {
        let mut list = SimpleOpLog::new();

        list.add_insert("a", 0, "aaa");

        list.add_delete_at("a", &[2], 1..2); // &[3]
        list.add_delete_at("b", &[2], 0..3); // &[6]

        // M2Tracker::apply_to_checkout(&mut list.checkout, &list.ops, (0..list.ops.len()).into());
        // list.checkout.merge_changes_m2(&list.ops, (3..list.ops.len()).into());
        // list.branch.merge(&list.oplog, &[3, 6]);
        assert_eq!(list.to_string(), "");
    }

    #[test]
    fn test_merge_deletes_2() {
        let mut list = SimpleOpLog::new();

        let t = list.add_insert_at("a", &[], 0, "aaa");
        list.add_delete_at("a", &[t], 1..2); // 3
        list.add_delete_at("b", &[t], 0..3); // 6
        // dbg!(&list.ops);

        // list.checkout.merge_changes_m2(&list.ops, (0..list.ops.len()).into());
        // list.branch.merge(&list.oplog, &[3, 6]);
        // dbg!(&list.branch);
        assert_eq!(list.to_string(), "");
    }

    fn items(tracker: &M2Tracker, filter_underwater: usize) -> Vec<FugueSpan> {
        let trim_from = UNDERWATER_START + filter_underwater;

        tracker.range_tree
            .iter()
            .filter_map(|mut i| {
                // dbg!((i.id.end, trim_from, i.id.start));
                if i.id.start >= trim_from {
                    assert_eq!(i.state, INSERTED);
                    return None;
                }

                if i.id.end > trim_from {
                    assert_eq!(i.state, INSERTED);
                    i.truncate(i.id.end - trim_from);
                }

                Some(i)
            })
            .merge_spans()
            .collect()
    }

    fn items_state(tracker: &M2Tracker, filter_underwater: usize) -> Vec<(usize, YjsSpanState)> {
        items(tracker, filter_underwater).iter().map(|i| (i.len(), i.state)).collect()
    }

    #[test]
    fn test_concurrent_insert() {
        let mut list = SimpleOpLog::new();

        list.add_insert_at("a", &[], 0, "aaa");
        list.add_insert_at("b", &[], 0, "bbb");

        let mut content = JumpRopeBuf::new();
        let mut t = M2Tracker::new();
        t.apply_range(&list.cg.agent_assignment, &list.info.ctx, &list.info.ops, (0..3).into(), Some(&mut content));
        t.retreat_by_range((0..3).into());
        t.apply_range(&list.cg.agent_assignment, &list.info.ctx, &list.info.ops, (3..6).into(), Some(&mut content));

        let i: Vec<_> = items(&t, 0).iter().map(|i| (i.id, i.state)).collect();
        assert_eq!(i, &[
            ((0..3).into(), NOT_INSERTED_YET),
            ((3..6).into(), INSERTED),
        ]);
        // dbg!(&t);
        // t.apply_range_at_version()

        assert_eq!(content, "aaabbb");
    }

    #[test]
    fn test_concurrent_delete() {
        let mut list = SimpleOpLog::new();

        list.add_insert("a", 0, "aaa");

        list.add_delete_at("a", &[2], 1..2);
        list.add_delete_at("b", &[2], 0..3);

        let mut content = JumpRopeBuf::new();
        let mut t = M2Tracker::new();
        t.apply_range(&list.cg.agent_assignment, &list.info.ctx, &list.info.ops, (0..4).into(), Some(&mut content));
        t.retreat_by_range((3..4).into());
        t.apply_range(&list.cg.agent_assignment, &list.info.ctx, &list.info.ops, (4..7).into(), Some(&mut content));
        t.advance_by_range((3..4).into());

        assert_eq!(items_state(&t, 0), &[
            (1, deleted_n_state(1)),
            (1, deleted_n_state(2)),
            (1, deleted_n_state(1)),
        ]);
        // dbg!(&t);

        assert_eq!(content, "");
        // t.apply_range_at_version()
    }

    #[test]
    fn unroll_delete() {
        let mut list = SimpleOpLog::new();
        list.add_insert("a", 0, "hi there"); // 0..8
        list.add_delete("a", 2..5); // 8..11

        let mut t = M2Tracker::new();

        let mut content = JumpRopeBuf::new();
        let end = list.cg.len();
        // dbg!(end);
        t.apply_range(&list.cg.agent_assignment, &list.info.ctx, &list.info.ops, (0..end).into(), Some(&mut content));
        assert_eq!(content, "hiere");
        // dbg!(&t);

        // t.retreat_by_range((0..end).into());
        t.retreat_by_range((8..end).into()); // undelete
        t.retreat_by_range((7..8).into()); // Uninsert the last character
        // dbg!(&t);
        // dbg!(items_state(&t, 0));
        assert_eq!(items_state(&t, 0), &[
            // It'd be nice if this collapsed together but whatever.
            (2, INSERTED),
            (3, INSERTED),
            (2, INSERTED),
            (1, NOT_INSERTED_YET),
        ]);
    }

    #[test]
    fn backspace() {
        let mut list = SimpleOpLog::new();
        list.add_insert("seph", 0, "abc"); // 2
        list.add_delete("seph", 2..3); // 3 -> "ab_"
        list.add_delete("seph", 1..2); // 4 -> "a__"
        let t = list.add_delete("seph", 0..1); // 5 -> "___"
        assert_eq!(t, 5);

        let mut t = M2Tracker::new();
        t.apply_range(&list.cg.agent_assignment, &list.info.ctx, &list.info.ops, (3..6).into(), None);
        assert_eq!(items_state(&t, 3), &[(3, DELETED_ONCE)]);

        t.retreat_by_range((5..6).into());
        assert_eq!(items_state(&t, 3), &[(1, INSERTED), (2, DELETED_ONCE)]);
        // dbg!(&t);

        assert_eq!(list.to_string(), "");

        // list.checkout.merge_branch(&list.ops, &[4]);
        // dbg!(&list.checkout);
    }

    #[test]
    fn ins_back() {
        let mut list = SimpleOpLog::new();

        list.add_insert("seph", 0, "c");
        list.add_insert("seph", 0, "b");
        list.add_insert("seph", 0, "a");

        assert_eq!(list.to_string(), "abc");
    }


    #[test]
    #[ignore]
    fn print_stats() {
        // node_nodecc: 72135
        // git-makefile: 23166
        let mut bytes = vec![];
        File::open("benchmark_data/git-makefile.dt").unwrap().read_to_end(&mut bytes).unwrap();
        // File::open("benchmark_data/node_nodecc.dt").unwrap().read_to_end(&mut bytes).unwrap();
        let o = ListOpLog::load_from(&bytes).unwrap();

        o.checkout_tip();
    }
}
