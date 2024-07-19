use std::cmp::Ordering;
use std::mem::replace;
use std::ops::Range;

use humansize::{file_size_opts, FileSize};
// use crate::content_tree::*;
use smallvec::smallvec;

use diamond_core_old::*;
use rle::{HasLength, MergeableIterator, RleRun};
use rle::Searchable;

use crate::crdtspan::CRDTSpan;
use crate::list::*;
use crate::list::branch::advance_branch_by_known;
use crate::list::external_txn::{RemoteCRDTOp, RemoteTxn};
use crate::list::InsDelTag::{Del, Ins};
use crate::list::ot::transform;
use crate::list::ot::traversal::TraversalOp;
use crate::list::positional::PositionalOpRef;
use crate::list::stats::{marker_a, marker_b, marker_c};
// use crate::list::ot::transform;
// use crate::list::ot::traversal::TraversalOp;
// use crate::list::positional::{PositionalComponent, PositionalOpRef};
// use crate::list::positional::InsDelTag::*;
use crate::ost::*;
use crate::ost::content_tree::{Content, ContentCursor, DeltaCursor};
use crate::rangeextra::OrderRange;
use crate::rle::RleVec;
use crate::unicount::{consume_chars, count_chars, split_at_char};

impl ClientData {
    pub fn get_next_seq(&self) -> usize {
        if let Some(KVPair(loc, range)) = self.item_localtime.last() {
            loc + range.len
        } else { 0 }
    }

    pub fn seq_to_order(&self, seq: usize) -> LV {
        let (entry, offset) = self.item_localtime.find_with_offset(seq).unwrap();
        entry.1.start + offset
    }

    pub fn seq_to_order_span(&self, seq: usize, max_len: usize) -> TimeSpan {
        let (entry, offset) = self.item_localtime.find_with_offset(seq).unwrap();
        TimeSpan {
            start: entry.1.start + offset,
            len: max_len.min(entry.1.len - offset),
        }
    }
}

pub(super) fn notify_for<'a>(index: &'a mut SpaceIndex) -> impl FnMut(YjsSpan, LeafIdx) + 'a {
    move |entry: YjsSpan, leaf| {
        index.set_range_2(entry.lv..entry.lv + entry.len(), Marker(leaf));

        // let mut len = entry.len();
        // let mut lv = entry.lv;


        // let index_len = index.len();
        // if entry.lv > index_len {
        //     // Insert extra dummy data to cover deletes.
        //     len += entry.lv - index_len;
        //     lv = index_len;
        // }
        //
        // index.replace_range_at_offset(lv as usize, MarkerEntry {
        //     ptr: Some(leaf), len
        // });
    }
}

impl Clone for ListCRDT {
    fn clone(&self) -> Self {
        let result = ListCRDT {
            frontier: self.frontier.clone(),
            client_with_time: self.client_with_time.clone(),
            client_data: self.client_data.clone(),

            // This is fine.
            range_tree: self.range_tree.clone(),
            index: self.index.clone(),
            deletes: self.deletes.clone(),
            double_deletes: self.double_deletes.clone(),
            txns: self.txns.clone(),
            text_content: self.text_content.clone(),
            deleted_content: self.deleted_content.clone(),
        };

        result
    }
}

impl ListCRDT {
    /// Make a CRDT object which does not store or update the resulting document state
    pub fn new_pure_oplog() -> Self {
        ListCRDT {
            client_with_time: RleVec::new(),
            frontier: smallvec![ROOT_LV],
            client_data: vec![],

            range_tree: ContentTree::new(),
            index: IndexTree::new(),
            // index: SplitList::new(),

            deletes: RleVec::new(),
            double_deletes: RleVec::new(),

            txns: RleVec::new(),

            text_content: None,
            // text_content: None,
            deleted_content: None,
        }
    }

    #[inline]
    pub fn new() -> Self {
        let mut doc = Self::new_pure_oplog();
        doc.text_content = Some(JumpRopeBuf::new());
        doc
    }

    pub fn has_content(&self) -> bool {
        self.text_content.is_some()
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        // Probably a nicer way to write this.
        if name == "ROOT" { return AgentId::MAX; }

        if let Some(id) = self.get_agent_id(name) {
            id
        } else {
            // Create a new id.
            self.client_data.push(ClientData {
                name: SmartString::from(name),
                item_localtime: RleVec::new()
            });
            (self.client_data.len() - 1) as AgentId
        }
    }

    pub(crate) fn get_agent_id(&self, name: &str) -> Option<AgentId> {
        if name == "ROOT" { Some(AgentId::MAX) }
        else {
            self.client_data.iter()
                .position(|client_data| client_data.name == name)
                .map(|id| id as AgentId)
        }
    }

    fn get_agent_name(&self, agent: AgentId) -> &str {
        self.client_data[agent as usize].name.as_str()
    }

    pub fn get_next_agent_seq(&self, agent: AgentId) -> usize {
        self.client_data[agent as usize].get_next_seq()
    }

    pub(crate) fn get_crdt_location(&self, order: LV) -> CRDTId {
        if order == ROOT_LV { CRDT_DOC_ROOT }
        else {
            let (loc, offset) = self.client_with_time.find_with_offset(order).unwrap();
            loc.1.at_offset(offset as usize)
        }
    }

    pub(crate) fn get_crdt_span(&self, order: LV, max_size: usize) -> CRDTSpan {
        if order == ROOT_LV { CRDTSpan { loc: CRDT_DOC_ROOT, len: 0 } }
        else {
            let (loc, offset) = self.client_with_time.find_with_offset(order).unwrap();
            CRDTSpan {
                loc: CRDTId {
                    agent: loc.1.loc.agent,
                    seq: loc.1.loc.seq + offset,
                },
                len: usize::min(loc.1.len - offset, max_size)
            }
        }
    }

    pub(crate) fn crdt_to_localtime(&self, loc: CRDTId) -> LV {
        if loc.agent == ROOT_AGENT { ROOT_LV }
        else { self.client_data[loc.agent as usize].seq_to_order(loc.seq) }
    }

    pub(crate) fn crdt_span_to_localtime(&self, loc: CRDTId, max_len: usize) -> TimeSpan {
        assert_ne!(loc.agent, ROOT_AGENT);
        self.client_data[loc.agent as usize].seq_to_order_span(loc.seq, max_len)
    }

    pub fn get_next_lv(&self) -> LV {
        if let Some(KVPair(base, entry)) = self.client_with_time.last() {
            base + entry.len
        } else { 0 }
    }

    /// Get the frontier as an internal order list
    pub fn get_frontier_as_localtime(&self) -> &[LV] {
        &self.frontier
    }

    pub(super) fn marker_at(&self, time: LV) -> Marker {
        self.index.get_entry(time).val
        // let cursor = self.index.cursor_at_offset_pos(time as usize, false);
        // // Gross.
        // cursor.get_item().unwrap().unwrap()
    }

    // pub(crate) fn get_unsafe_cursor_before(&self, time: LV) -> UnsafeCursor<YjsSpan, DocRangeIndex, DOC_IE, DOC_LE> {
    //     if time == ROOT_LV {
    //         // Or maybe we should just abort?
    //         self.range_tree.unsafe_cursor_at_end()
    //     } else {
    //         let marker = self.marker_at(time);
    //         unsafe {
    //             ContentTreeRaw::unsafe_cursor_before_item(time, marker)
    //         }
    //     }
    // }

    #[inline(always)]
    pub(crate) fn get_cursor_before(&self, lv: LV) -> ContentCursor {
        if lv == usize::MAX {
            // This case doesn't seem to ever get hit by the fuzzer. It might be equally correct to
            // just panic() here.
            self.range_tree.cursor_at_end()
            // panic!()
        } else {
            // self.check(true);
            let leaf_idx = self.marker_at(lv);
            self.range_tree.cursor_before_item(lv, leaf_idx.0)
        }
        // unsafe { Cursor::unchecked_from_raw(&self.range_tree, self.get_unsafe_cursor_before(time)) }
    }
    
    #[inline(always)]
    pub(crate) fn get_mut_cursor_before(&mut self, lv: LV) -> DeltaCursor {
        if lv == usize::MAX {
            // This case doesn't seem to ever get hit by the fuzzer. It might be equally correct to
            // just panic() here.
            self.range_tree.mut_cursor_at_end()
            // panic!()
        } else {
            let leaf_idx = self.marker_at(lv);
            // marker_a();
            self.range_tree.mut_cursor_before_item(lv, leaf_idx.0)
        }
    }

    fn get_cursor_after(&self, lv: LV, stick_end: bool) -> ContentCursor {
        if lv == usize::MAX {
            self.range_tree.cursor_at_start_nothing_emplaced()
        } else {
            let leaf_idx = self.marker_at(lv).0;
            // let marker: NonNull<NodeLeaf<YjsSpan, ContentIndex>> = self.markers.at(order as usize).unwrap();
            // self.content_tree.
            let mut cursor = self.range_tree.cursor_before_item(lv, leaf_idx);
            // The cursor points to parent. This is safe because of guarantees provided by
            // cursor_before_item.
            cursor.inc_offset(&self.range_tree);
            if !stick_end { cursor.roll_next_item(&self.range_tree); }
            cursor
        }
    }
    fn get_mut_cursor_after(&mut self, lv: LV, stick_end: bool) -> DeltaCursor {
        if lv == usize::MAX {
            self.range_tree.mut_cursor_at_start()
        } else {
            let leaf_idx = self.marker_at(lv).0;
            // let marker: NonNull<NodeLeaf<YjsSpan, ContentIndex>> = self.markers.at(order as usize).unwrap();
            // self.content_tree.
            // marker_b();

            let mut cursor = self.range_tree.mut_cursor_before_item(lv, leaf_idx);
            // The cursor points to parent. This is safe because of guarantees provided by
            // cursor_before_item.
            cursor.0.inc_offset(&self.range_tree);
            if !stick_end { cursor.roll_next_item(&mut self.range_tree); }
            cursor
        }
    }



    // // This does not stick_end to the found item.
    // pub(super) fn get_unsafe_cursor_after(&self, time: LV, stick_end: bool) -> UnsafeCursor<YjsSpan, DocRangeIndex, DOC_IE, DOC_LE> {
    //     if time == ROOT_LV {
    //         self.range_tree.unsafe_cursor_at_start()
    //     } else {
    //         let marker = self.marker_at(time);
    //         // let marker: NonNull<NodeLeaf<YjsSpan, ContentIndex>> = self.markers.at(order as usize).unwrap();
    //         // self.content_tree.
    //         let mut cursor = unsafe {
    //             ContentTreeRaw::unsafe_cursor_before_item(time, marker)
    //         };
    //         // The cursor points to parent. This is safe because of guarantees provided by
    //         // cursor_before_item.
    //         cursor.offset += 1;
    //         if !stick_end { cursor.roll_to_next_entry(); }
    //         cursor
    //     }
    // }

    // // TODO: Can I remove the stick_end field here?
    // #[inline(always)]
    // pub(crate) fn get_cursor_after(&self, time: LV, stick_end: bool) -> Cursor<YjsSpan, DocRangeIndex, DOC_IE, DOC_LE> {
    //     unsafe { Cursor::unchecked_from_raw(&self.range_tree, self.get_unsafe_cursor_after(time, stick_end)) }
    // }

    pub(super) fn assign_lv_to_client(&mut self, loc: CRDTId, time: LV, len: usize) {
        self.client_with_time.push(KVPair(time, CRDTSpan {
            loc,
            len: len as _
        }));

        self.client_data[loc.agent as usize].item_localtime.insert(KVPair(loc.seq, TimeSpan {
            start: time,
            len: len as _
        }));
    }

    pub(crate) fn max_span_length(&self, time: LV) -> usize {
        let (span, span_offset) = self.client_with_time.find_with_offset(time).unwrap();
        span.1.len - span_offset
    }

    // pub(super) fn integrate(&mut self, agent: AgentId, item: YjsSpan, ins_content: Option<&str>, cursor_hint: Option<UnsafeCursor<YjsSpan, DocRangeIndex, DOC_IE, DOC_LE>>) {
    pub(super) fn integrate(&mut self, agent: AgentId, item: YjsSpan, ins_content: Option<&str>, mut cursor: DeltaCursor) {
        // if cfg!(debug_assertions) {
        //     let next_order = self.get_next_order();
        //     assert_eq!(item.order, next_order);
        // }

        assert!(item.len > 0);

        // self.assign_order_to_client(loc, item.order, item.len as _);

        // Ok now that's out of the way, lets integrate!
        // let mut cursor = cursor_hint.map_or_else(|| {
        //     self.get_unsafe_cursor_after(item.origin_left, false)
        // }, |mut c| {
        //     // Ideally this wouldn't be necessary.
        //     c.roll_to_next_entry();
        //     c
        // });
        cursor.roll_next_item(&mut self.range_tree);

        // let mut cursor = cursor_hint.unwrap_or_else(|| {
        //     self.get_unsafe_cursor_after(item.origin_left, false)
        // });

        // These are almost never used. Could avoid the clone here... though its pretty cheap.
        let left_cursor = cursor.0.clone();
        let mut scan_cursor = cursor.0.clone();
        let mut scanning = false;

        loop {
            if !cursor.roll_next_item(&mut self.range_tree) { // End of the document
                break;
            }

            // let other_order = match unsafe { cursor.unsafe_get_item() } {
            //     None => { break; } // End of the document
            //     Some(o) => { o }
            // };

            let other_entry = *cursor.0.get_item(&self.range_tree).0;
            let other_lv = other_entry.lv + cursor.0.offset;

            // Almost always true. Could move this short circuit earlier?
            if other_lv == item.origin_right { break; }

            // We're now in the rare case there's actually concurrent inserts. To make the logic
            // simpler, at this point we'll zero out the delta.
            cursor.flush_delta_and_clear(&mut self.range_tree);

            debug_assert_eq!(cursor.1, LenUpdate::default());

            // This code could be better optimized, but its already O(n * log n), and its extremely
            // rare that you actually get concurrent inserts at the same location in the document
            // anyway.

            // let other_entry = *cursor.get_raw_entry();
            // let other_order = other_entry.order + cursor.offset as u32;

            let other_left_order = other_entry.origin_left_at_offset(cursor.0.offset);
            let other_left_cursor = self.get_cursor_after(other_left_order, false);

            // YjsMod semantics
            match other_left_cursor.cmp(&left_cursor, &self.range_tree) {
                Ordering::Less => { break; } // Top row
                Ordering::Greater => { } // Bottom row. Continue.
                Ordering::Equal => {
                    if item.origin_right == other_entry.origin_right {
                        // Items are concurrent and "double siblings". Order by agent names.
                        let my_name = self.get_agent_name(agent);
                        let other_loc = self.client_with_time.get(other_lv);
                        let other_name = self.get_agent_name(other_loc.agent);

                        // Its possible for a user to conflict with themself if they commit to
                        // multiple branches. In this case, sort by seq number.
                        let ins_here = match my_name.cmp(other_name) {
                            Ordering::Less => true,
                            Ordering::Equal => {
                                self.get_crdt_location(item.lv).seq < other_loc.seq
                            }
                            Ordering::Greater => false,
                        };

                        // Insert here.
                        if ins_here { break; }
                        else { scanning = false; }
                    } else {
                        // Set scanning based on how the origin_right entries are ordered.
                        let my_right_cursor = self.get_cursor_before(item.origin_right);
                        let other_right_cursor = self.get_cursor_before(other_entry.origin_right);

                        if other_right_cursor.cmp(&my_right_cursor, &self.range_tree) == Ordering::Less {
                            if !scanning {
                                scanning = true;
                                scan_cursor = cursor.0.clone();
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
            if !cursor.0.next_entry(&self.range_tree).0 {
                // This is dirty. If the cursor can't move to the next entry, we still need to move
                // it to the end of the current element or we'll prepend. next_entry() doesn't do
                // that for some reason. TODO: Clean this up.
                cursor.0.offset = other_entry.len();
                break;
            }
        }
        if scanning { cursor.0 = scan_cursor; }

        // if cfg!(debug_assertions) {
        //     let pos = unsafe { cursor.unsafe_count_content_pos() as usize };
        //     let len = self.range_tree.content_len() as usize;
        //     assert!(pos <= len);
        // }

        let mut pos = cursor.0.get_pos(&self.range_tree);

        if let Some(text) = self.text_content.as_mut() {
            if let Some(ins_content) = ins_content {
                // debug_assert_eq!(count_chars(&ins_content), item.len as usize);
                text.insert(pos, ins_content);
            } else {
                // todo!("Figure out what to do when inserted content not present");
                // This is really dirty. This will happen when we're integrating remote txns which
                // are missing inserted content - usually because the remote peer hasn't kept
                // deleted text.
                //
                // In that case, we're inserting content which is about to be deleted by another
                // incoming operation.
                //
                // Ideally it would be nice to flag the range here and cancel it out with the
                // corresponding incoming delete. But thats really awkward, and this hack is super
                // simple.
                let content = SmartString::from("x").repeat(item.len as usize);
                text.insert(pos, content.as_str());
            }
        }

        // Now insert here.
        // unsafe { ContentTreeRaw::unsafe_insert_notify(&mut cursor, item, notify_for(&mut self.index)); }
        pos += item.content_len();
        self.range_tree.insert(item, &mut cursor, true, &mut notify_for(&mut self.index));
        self.range_tree.emplace_cursor(pos, cursor);
        // cursor
    }

    // For local changes, where we just take the frontier as the new parents list.
    fn insert_txn_local(&mut self, range: Range<LV>) {
        // Fast path for local edits. For some reason the code below is remarkably non-performant.
        if self.frontier.len() == 1 && self.frontier[0] == range.start.wrapping_sub(1) {
            if let Some(last) = self.txns.0.last_mut() {
                let len = range.order_len();
                last.len += len;
                self.frontier[0] += len;
                return;
            }
        }

        // Otherwise use the slow version.
        let txn_parents = replace(&mut self.frontier, smallvec![range.last_order()]);
        self.insert_txn_internal(&txn_parents, range);
    }

    pub(crate) fn insert_txn_full(&mut self, txn_parents: &[LV], range: Range<LV>) {
        advance_branch_by_known(&mut self.frontier, &txn_parents, range.clone());
        self.insert_txn_internal(txn_parents, range);
    }

    fn insert_txn_internal(&mut self, txn_parents: &[LV], range: Range<LV>) {
        // Fast path. The code below is weirdly slow, but most txns just append.
        // My kingdom for https://rust-lang.github.io/rfcs/2497-if-let-chains.html
        if let Some(last) = self.txns.0.last_mut() {
            if txn_parents.len() == 1
                && txn_parents[0] == last.last_time()
                && last.time + last.len == range.start
            {
                last.len += range.order_len();
                return;
            }
        }

        // println!("insert_txn_internal {:?} {:?}", txn_parents, range);

        // let parents = replace(&mut self.frontier, txn_parents);
        let mut shadow = range.start;
        while shadow >= 1 && txn_parents.contains(&(shadow - 1)) {
            shadow = self.txns.find(shadow - 1).unwrap().shadow;
        }
        if shadow == 0 { shadow = ROOT_LV; }

        let will_merge = if let Some(last) = self.txns.last() {
            // TODO: Is this shadow check necessary?
            // This code is from TxnSpan splitablespan impl. Copying it here is a bit ugly but
            // its the least ugly way I could think to implement this.
            txn_parents.len() == 1 && txn_parents[0] == last.last_time() && shadow == last.shadow
        } else { false };

        let mut parent_indexes = smallvec![];
        if !will_merge {
            // The item wasn't merged. So we need to go through the parents and wire up children.
            let new_idx = self.txns.0.len();

            for &p in txn_parents {
                if p == ROOT_LV { continue; }
                let parent_idx = self.txns.find_index(p).unwrap();
                // Interestingly the parent_idx array will always end up the same length as parents
                // because it would be invalid for multiple parents to point to the same entry in
                // txns. (That would imply one parent is a descendant of another.)

                debug_assert!(!parent_indexes.contains(&parent_idx));
                parent_indexes.push(parent_idx);

                let parent_children = &mut self.txns.0[parent_idx].child_indexes;
                if !parent_children.contains(&new_idx) {
                    parent_children.push(new_idx);

                    // This is a tiny optimization for txn_trace. We store the child_indexes in
                    // order of their first parent - which will usually be the order in which we
                    // want to iterate them.
                    // TODO: Make this work and benchmark.
                    // if parent_children.len() > 1 {
                    //     parent_children.sort_unstable_by(|&a, &b| {
                    //         u32::cmp(&self.txns.0[a].parents[0].wrapping_add(1),
                    //                  &self.txns.0[b].parents[0].wrapping_add(1))
                    //     });
                    // }
                }

            }
        }

        let txn = TxnSpan {
            time: range.start,
            len: range.order_len(),
            shadow,
            parents: txn_parents.into_iter().copied().collect(),
            parent_indexes,
            child_indexes: smallvec![]
        };

        let did_merge = self.txns.push(txn);
        assert_eq!(will_merge, did_merge);
    }

    pub(super) fn internal_mark_deleted(&mut self, id: LV, target: LV, max_len: usize, update_content: bool) -> LV {
        // TODO: Make this use mut_cursor instead. The problem is notify_for mutably borrows
        // self.index, and the cursor is borrowing self (rather than self.range_tree).
        // let mut cursor = self.get_unsafe_cursor_before(target);
        let mut cursor = self.get_mut_cursor_before(target);
        let result = self.internal_mark_deleted_at(&mut cursor, id, max_len, update_content);
        self.range_tree.emplace_cursor_unknown(cursor);
        result
    }

    pub(super) fn internal_mark_deleted_at(&mut self, cursor: &mut DeltaCursor, id: LV, max_len: usize, update_content: bool) -> LV {
        // let target = unsafe { cursor.unsafe_get_item().unwrap() };
        let (e, offset) = cursor.0.get_item(&self.range_tree);
        let target = e.lv + offset;

        let (deleted_here, succeeded) = unsafe {
            self.range_tree.mutate_entry(cursor, max_len, &mut notify_for(&mut self.index), |e| {
                if e.len > 0 {
                    e.len = -e.len;
                    true
                } else { false }
            })
            // ContentTreeRaw::unsafe_remote_deactivate_notify(cursor, max_len as _, notify_for(&mut self.index))
        };
        // let deleted_here = deleted_here as u32;

        self.deletes.push(KVPair(id, TimeSpan {
            start: target,
            len: deleted_here
        }));

        if !succeeded {
            // This span was already deleted by a different peer. Mark duplicate delete.
            self.double_deletes.increment_delete_range(target, deleted_here);
        } else if let (Some(text), true) = (&mut self.text_content, update_content) {
            // The call to remote_deactivate will have modified the cursor, but the content position
            // will have stayed the same.
            let pos = cursor.0.get_pos(&self.range_tree);
            // let pos = unsafe { cursor.unsafe_count_content_pos() as usize };
            text.remove(pos..pos + deleted_here);
        }

        deleted_here
    }

    pub fn apply_remote_txn(&mut self, txn: &RemoteTxn) {
        // self.range_tree.dbg_check();

        let agent = self.get_or_create_agent_id(txn.id.agent.as_str());
        let client = &self.client_data[agent as usize];
        // let next_seq = client.get_next_seq();

        // Check that the txn hasn't already been applied.
        debug_assert!(client.item_localtime.find(txn.id.seq).is_none());

        let first_time = self.get_next_lv();
        let mut next_time = first_time;

        // Figure out the order range for this txn and assign
        let mut txn_len = 0;
        let mut expected_content_len = 0;
        for op in txn.ops.iter() {
            match op {
                RemoteCRDTOp::Ins { len, content_known, .. } => {
                    // txn_len += ins_content.chars().count();
                    txn_len += *len as usize;
                    if *content_known {
                        expected_content_len += *len;
                    }
                }
                RemoteCRDTOp::Del { len, .. } => {
                    txn_len += *len as usize;
                }
            }
        }
        // println!("{} txn agent {}, seq {} - {}", first_time, &txn.id.agent, txn.id.seq, txn.id.seq + txn_len as u32);

        assert_eq!(count_chars(&txn.ins_content), expected_content_len as usize);
        let mut content = txn.ins_content.as_str();

        // TODO: This may be premature - we may be left in an invalid state if the txn is invalid.
        self.assign_lv_to_client(CRDTId {
            agent,
            seq: txn.id.seq,
        }, first_time, txn_len);

        // Apply the changes.
        for op in txn.ops.iter() {
            // self.range_tree.dbg_check();

            match op {
                RemoteCRDTOp::Ins { origin_left, origin_right, len, content_known } => {
                    // let ins_len = ins_content.chars().count();

                    let order = next_time;
                    next_time += len;

                    // Convert origin left and right to order numbers
                    let origin_left = self.remote_id_to_order(origin_left);
                    let origin_right = self.remote_id_to_order(origin_right);

                    // if cfg!(debug_assertions) {
                    //     let left = self.get_cursor_after(origin_left, true);
                    //     let right = self.get_cursor_before(origin_right);
                    //     assert!(left <= right);
                    // }

                    let item = YjsSpan {
                        lv: order,
                        origin_left,
                        origin_right,
                        len: *len as isize,
                    };

                    let ins_content = if *content_known {
                        let (ins_here, remainder) = split_at_char(content, *len as usize);
                        content = remainder;
                        Some(ins_here)
                    } else {
                        None
                    };

                    let cursor = self.get_mut_cursor_after(origin_left, false);
                    // cursor.0.inc_offset(&self.range_tree);
                    self.integrate(agent, item, ins_content, cursor);
                    // self.range_tree.dbg_check();
                }

                RemoteCRDTOp::Del { id, len } => {
                    // The order of the item we're deleting
                    // println!("handling remote delete of id {:?} len {}", id, len);
                    let agent = self.get_agent_id(id.agent.as_str()).unwrap() as usize;
                    // let client = &self.client_data[agent as usize];

                    // let mut target_order = self.remote_id_to_order(&id);

                    // We're deleting a span of target_order..target_order+len.

                    let mut target_seq = id.seq;
                    let mut remaining_len = *len;
                    while remaining_len > 0 {
                        // We need to loop here because the deleted items may not be in a run in the
                        // local range tree. They usually will be though. We might also have been
                        // asked to delete a run of sequences which don't match to a run of order
                        // numbers.

                        // So to be clear, each iteration we delete the minimum of:
                        // 1. `len` (passed in from the RemoteTxn above) via remaining_len
                        // 2. The length of the span returned by seq_to_order_span
                        // 3. The contiguous region of items in the range tree
                        let TimeSpan {
                            start: target_order,
                            len, // min(1 and 2)
                        } = self.client_data[agent].seq_to_order_span(target_seq, remaining_len);

                        // I could break this into two loops - and here enter an inner loop,
                        // deleting len items. It seems a touch excessive though.

                        let deleted_here = self.internal_mark_deleted(next_time, target_order, len, true);

                        // println!(" -> managed to delete {}", deleted_here);
                        remaining_len -= deleted_here;
                        target_seq += deleted_here;

                        // This span is locked in once we find the contiguous region of seq numbers.

                        // handled by internal_mark_deleted.
                        // self.deletes.push(KVPair(next_order, OrderSpan {
                        //     order: target_order,
                        //     len: deleted_here
                        // }));
                        next_time += deleted_here;

                    }

                    // TODO: Remove me. This is only needed because SplitList doesn't support gaps.
                    // let mut cursor = self.index.cursor_at_end();
                    // let last_entry = cursor.get_raw_entry();
                    // let entry = MarkerEntry {
                    //     len: *len, ptr: last_entry.ptr
                    // };
                    // self.index.insert(&mut cursor, entry, null_notify);

                    // self.range_tree.dbg_check();
                }
            }

        }

        assert!(content.is_empty());

        debug_assert_eq!(next_time, first_time + txn_len);
        let parents = self.remote_ids_to_branch(&txn.parents);
        self.insert_txn_full(&parents, first_time..next_time);
    }

    pub fn apply_local_txn(&mut self, agent: AgentId, mut op: PositionalOpRef) {
        // local_ops: &[PositionalComponent], mut content: &str
        let first_time = self.get_next_lv();
        let mut next_time = first_time;

        let txn_len = op.components.iter().map(|c| c.len).sum::<usize>() as usize;

        self.assign_lv_to_client(CRDTId {
            agent,
            seq: self.client_data[agent as usize].get_next_seq()
        }, first_time, txn_len);

        // for LocalOp { pos, ins_content, del_span } in local_ops {
        for c in op.components {
            let pos = c.pos;
            let len = c.len;

            match c.tag {
                Ins => {
                    // First we need the insert's base order
                    let time = next_time;
                    next_time += c.len;

                    // self.range_tree.dbg_check();

                    // Find the preceding item and successor
                    let (origin_left, mut cursor) = if pos == 0 {
                        (ROOT_LV, self.range_tree.mut_cursor_at_start())
                    } else {
                        let mut cursor = self.range_tree.mut_cursor_before_cur_pos(pos - 1);
                        let (e, offset) = cursor.0.get_item(&self.range_tree);
                        let origin_left = e.lv + offset;
                        // if CHECK_TREES { assert_eq!(origin_left, origin_left_2); }
                        cursor.0.inc_offset(&self.range_tree);

                        (origin_left, cursor)
                    };

                    // There's an open question of whether this should skip past deleted items.
                    // It would be *correct* both ways, though you get slightly different merging
                    // & pruning behaviour in each case.
                    let origin_right = if cursor.roll_next_item(&mut self.range_tree) {
                        cursor.0.try_get_item(&self.range_tree)
                            .map(|(span, offset)| span.lv + offset)
                            .unwrap_or(ROOT_LV)
                    } else {
                        ROOT_LV
                    };

                    let item = YjsSpan {
                        lv: time,
                        origin_left,
                        origin_right,
                        len: len as isize
                    };
                    // dbg!(item);

                    let ins_content = if c.content_known {
                        Some(consume_chars(&mut op.content, len))
                    } else { None };

                    self.integrate(agent, item, ins_content, cursor);
                }

                Del => {
                    let mut cursor = self.range_tree.mut_cursor_before_cur_pos(pos);
                    cursor.roll_next_item(&mut self.range_tree);

                    let mut len_remaining = len;

                    loop {
                        let del_here = self.range_tree.mutate_entry(&mut cursor, len_remaining, &mut notify_for(&mut self.index), |item| {
                            debug_assert!(item.len > 0);

                            self.deletes.push(KVPair(next_time, TimeSpan {
                                start: item.lv,
                                len: item.len as usize
                            }));

                            item.len = -item.len;
                        }).0;
                        next_time += del_here;

                        len_remaining -= del_here;
                        if len_remaining == 0 { break; }

                        self.range_tree.slide_cursor_to_next_content(&mut cursor.0, &mut cursor.1);
                    }

                    self.range_tree.emplace_cursor(pos, cursor);
                    // let deleted_items = self.range_tree.local_deactivate_at_content_notify(pos, len, notify_for(&mut self.index));

                    // dbg!(&deleted_items);

                    // I might be able to relax this, but we'd need to change del_span above.
                    // assert_eq!(deleted_length, len);

                    if let Some(ref mut text) = self.text_content {
                        if let Some(deleted_content) = self.deleted_content.as_mut() {
                            // TODO: This could be optimized by using chunks.
                            let borrow = text.borrow();
                            let chars = borrow.slice_chars(pos..pos+len);
                            deleted_content.extend(chars);
                        }
                        text.remove(pos..pos + len);
                    }
                }
            }
        }

        self.insert_txn_local(first_time..next_time);
        debug_assert_eq!(next_time, self.get_next_lv());
    }

    pub fn local_insert(&mut self, agent: AgentId, pos: usize, ins_content: &str) {
        self.apply_local_txn(agent, PositionalOpRef {
            components: &[PositionalComponent {
                pos,
                len: count_chars(ins_content),
                content_known: true,
                tag: Ins
            }],
            content: ins_content
        });
    }

    pub fn local_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) {
        self.apply_local_txn(agent, PositionalOpRef {
            components: &[PositionalComponent {
                pos, len: del_span, content_known: true, tag: Del
            }],
            content: ""
        });
    }

    // TODO: Consider refactoring me to use positional operations instead of traversal operations
    pub fn apply_txn_at_ot_order(&mut self, agent: AgentId, op: &TraversalOp, order: LV, is_left: bool) {
        let now = self.get_next_lv();
        if order < now {
            let historical_patches = self.traversal_changes_since(order);
            let mut local_ops = op.traversal.clone();
            for p in historical_patches.components {
                local_ops = transform(local_ops.as_slice(), &p, is_left);
            }
            // let positional = PositionalComponent::from_traversal_components(&local_ops);
            self.apply_local_txn(agent, PositionalOpRef {
                components: PositionalComponent::from_traversal_components(&local_ops).as_slice(),
                content: op.content.as_str()
            });
            // self.apply_local_txn(agent, &positional, op.content.as_str());
        } else {
            // let positional = PositionalComponent::from_traversal_components(&op.traversal);
            // self.apply_local_txn(agent, &positional, op.content.as_str());
            self.apply_local_txn(agent, PositionalOpRef {
                components: PositionalComponent::from_traversal_components(&op.traversal).as_slice(),
                content: op.content.as_str()
            });
        }
    }

    pub fn insert_at_ot_order(&mut self, agent: AgentId, pos: usize, ins_content: &str, order: LV, is_left: bool) {
        self.apply_txn_at_ot_order(agent, &TraversalOp::new_insert(pos, ins_content), order, is_left);
    }
    pub fn delete_at_ot_order(&mut self, agent: AgentId, pos: usize, del_span: usize, order: LV, is_left: bool) {
        self.apply_txn_at_ot_order(agent, &TraversalOp::new_delete(pos, del_span), order, is_left);
    }

    pub fn len(&self) -> usize {
        self.range_tree.total_len()
    }

    pub fn is_empty(&self) -> bool {
        // self.range_tree.content_len() != 0
        self.range_tree.is_empty()
    }

    pub fn print_stats(&self, detailed: bool) {
        println!("Document of length {}", self.len());

        let ins_del_count = self.range_tree.iter()
            .map(|e| RleRun::new(e.takes_up_space(), e.len()))
            .merge_spans()
            .count();
        println!("As alternating inserts and deletes: {} items", ins_del_count);

        if let Some(r) = &self.text_content {
            println!("Content memory size: {}", r.borrow().mem_size().file_size(file_size_opts::CONVENTIONAL).unwrap());
        }

        // self.range_tree.print_stats("content", detailed);

        // self.index.print_stats("index", detailed);
        // self.markers.print_rle_size();
        self.deletes.print_stats("deletes", detailed);
        self.double_deletes.print_stats("double deletes", detailed);
        self.txns.print_stats("txns", detailed);
    }

    #[allow(unused)]
    pub fn debug_print_segments(&self) {
        for entry in self.range_tree.iter() {
            let loc = self.get_crdt_location(entry.lv);
            println!("order {} len {} ({}) agent {} / {} <-> {}", entry.lv, entry.len(), entry.content_len(), loc.agent, entry.origin_left, entry.origin_right);
        }
    }

    #[allow(unused)]
    pub fn debug_print_ids(&self) {
        for span in self.range_tree.iter() {
            let id = self.get_crdt_location(span.lv);
            let left = self.get_crdt_location(span.origin_left);
            let right = self.get_crdt_location(span.origin_right);

            // let parent_order = self.get_right_parent(&span, 0).1;
            // let parent = self.get_crdt_location(parent_order);

            println!("{:?} (len {}) left {:?} right {:?} \t(LV: {})",
                     id, span.len, left, right, span.lv
            );
        }

        // dbg!(&self.deletes);
        // self.debug_print_del();
    }


    #[allow(unused)]
    pub fn debug_print_del(&self) {
        for e in self.deletes.iter() {
            println!("delete {} deletes {} len {}", e.0, e.1.start, e.1.len);
        }
    }

    pub fn get_internal_list_entries<'a>(&'a self) -> impl Iterator<Item=YjsSpan> + 'a {
        self.range_tree.iter()
    }
}

impl ToString for ListCRDT {
    fn to_string(&self) -> String {
        self.text_content.as_ref().unwrap().to_string()
    }
}

impl Default for ListCRDT {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;

    use crate::list::*;
    use crate::list::external_txn::{RemoteCRDTOp, RemoteId, RemoteTxn};
    use crate::list::ot::traversal::TraversalOp;
    // use crate::list::ot::traversal::TraversalOp;
    use crate::root_id;

    #[test]
    fn smoke() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.local_insert(0, 0, "hi".into());
        doc.local_insert(0, 1, "yooo".into());
        // "hyoooi"
        doc.local_delete(0, 1, 3);

        doc.check(true);
        dbg!(doc);
    }

    #[test]
    fn demo() {
        let mut doc1 = ListCRDT::new();
        doc1.get_or_create_agent_id("alice");
        doc1.get_or_create_agent_id("bob");
        let mut doc2 = ListCRDT::new();
        doc2.get_or_create_agent_id("alice");
        doc2.get_or_create_agent_id("bob");


        doc1.local_insert(0, 0, "a".into());
        doc2.local_insert(1, 0, "b".into());
        
        doc1.replicate_into(&mut doc2);
        doc2.replicate_into(&mut doc1);
        dbg!(&doc1);
        // doc1.local_insert(0, 0, "a".into());
    }

    #[test]
    fn deletes_merged() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "abc".into());
        // doc.local_delete(0, 2, 1);
        // doc.local_delete(0, 1, 1);
        // doc.local_delete(0, 0, 1);
        doc.local_delete(0, 0, 1);
        doc.local_delete(0, 0, 1);
        doc.local_delete(0, 0, 1);
        dbg!(doc);
    }

    // #[test]
    // fn shadow() {
    //     let mut doc = ListCRDT::new();
    //     let seph = doc.get_or_create_client_id("seph");
    //     let mike = doc.get_or_create_client_id("mike");
    //
    //     doc.local_insert(seph, 0, "a".into());
    //     assert_eq!(doc.txns.find(0).unwrap().0.shadow, 0);
    // }

    // fn assert_frontier_eq(doc: &ListCRDT, expected: &Branch) {
    //     // The order of frontier is not currently guaranteed.
    //     let mut a = doc.frontier.clone();
    //     a.sort();
    //     let mut b = expected.clone();
    //     b.sort();
    //     assert_eq!(a, b);
    // }

    #[test]
    fn remote_txns() {
        let mut doc_remote = ListCRDT::new();
        doc_remote.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "seph".into(), seq: 0 },
            parents: smallvec![root_id()],
            ops: smallvec![
                RemoteCRDTOp::Ins {
                    origin_left: root_id(),
                    origin_right: root_id(),
                    len: 2,
                    content_known: true,
                    // ins_content: "hi".into()
                }
            ],
            ins_content: "hi".into(),
        });

        let mut doc_local = ListCRDT::new();
        doc_local.get_or_create_agent_id("seph");
        doc_local.local_insert(0, 0, "hi".into());
        // dbg!(&doc_remote);
        assert_eq!(doc_remote, doc_local);
        assert_eq!(doc_remote.deletes, doc_local.deletes); // Not currently checked by Eq.

        doc_remote.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "seph".into(), seq: 2 },
            parents: smallvec![RemoteId {agent: "seph".into(), seq: 1}],
            ops: smallvec![
                RemoteCRDTOp::Del {
                    id: RemoteId {
                        agent: "seph".into(),
                        seq: 0
                    },
                    len: 2,
                }
            ],
            ins_content: SmartString::new(),
        });

        // dbg!(&doc_remote);
        doc_local.local_delete(0, 0, 2);
        // dbg!(&doc_local);

        assert_eq!(doc_remote, doc_local);
        assert_eq!(doc_remote.deletes, doc_local.deletes); // Not currently checked by Eq.

        assert_eq!(doc_remote, doc_remote.clone());
        // dbg!(doc_remote.get_version_vector());
    }

    #[test]
    fn remote_txns_fork() {
        // Two users concurrently type into an empty document
        let mut doc = ListCRDT::new();
        assert_eq!(doc.frontier.as_slice(), &[ROOT_LV]);

        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId {
                agent: "seph".into(),
                seq: 0
            },
            parents: smallvec![root_id()],
            ops: smallvec![
                RemoteCRDTOp::Ins {
                    origin_left: root_id(),
                    origin_right: root_id(),
                    len: 2,
                    content_known: true,
                    // ins_content: "hi".into()
                }
            ],
            ins_content: "aa".into(),
        });
        assert_eq!(doc.frontier.as_slice(), &[1]);

        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId {
                agent: "mike".into(),
                seq: 0
            },
            parents: smallvec![root_id()],
            ops: smallvec![
                RemoteCRDTOp::Ins {
                    origin_left: root_id(),
                    origin_right: root_id(),
                    len: 5,
                    content_known: true,
                    // ins_content: "abcde".into()
                }
            ],
            ins_content: "bbbbb".into(),
        });

        // The frontier is split
        assert_eq!(doc.frontier.as_slice(), &[1, 6]);

        // The transactions shouldn't be merged.
        assert_eq!(doc.txns.len(), 2);

        // Merge the two branches.
        doc.local_insert(0, 7, "x".into());
        assert_eq!(doc.frontier.as_slice(), &[7]);

        // The new txn entry should have both items in the split as parents.
        assert_eq!(doc.txns.0[2].parents.as_slice(), &[1, 6]);

        // dbg!(&doc);

        // Mike is missing all the changes from seph.
        assert_eq!(doc.get_time_spans_since::<Vec<_>>(&vec![RemoteId {
            agent: "mike".into(),
            seq: 5
        }]), vec![TimeSpan {
            start: 0,
            len: 2
        }, TimeSpan {
            start: 7,
            len: 1
        }]);

        assert_eq!(doc, doc.clone());
    }

    #[test]
    fn apply_at_order() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.local_insert(0, 0, "aa".into());

        let op = TraversalOp::new_insert(0, "bb");
        // If we apply the change with is_left = false, the new content goes on the right...
        doc.apply_txn_at_ot_order(0, &op, 0, false);
        if let Some(text) = doc.text_content.as_ref() {
            assert_eq!(text, "aabb");
        }

        let op = TraversalOp::new_insert(0, "cc");
        // And if is_left is true, new content goes left.
        doc.apply_txn_at_ot_order(0, &op, 0, true);
        if let Some(text) = doc.text_content.as_ref() {
            assert_eq!(text, "ccaabb");
        }

        assert_eq!(doc, doc.clone());
    }
}