// TODO: This file ended up being a kitchen sink for the logic here. Separate this logic out into a
// few files!

use std::pin::Pin;
use jumprope::JumpRopeBuf;

use smallvec::SmallVec;

use content_tree::*;
use diamond_core_old::CRDTId;
use rle::AppendRle;
use TraversalComponent::*;

use crate::crdtspan::CRDTSpan;
use crate::list::{DoubleDeleteList, ListCRDT, LV};
use crate::list::double_delete::DoubleDelete;
use crate::list::external_txn::RemoteIdSpan;
use crate::list::positional::{InsDelTag, PositionalComponent, PositionalOp};
use crate::list::ot::traversal::{TraversalComponent, TraversalOp, TraversalOpSequence};
use crate::order::TimeSpan;
use crate::rle::{KVPair, RleKey, RleSpanHelpers};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) struct PrePostIndex;

// TODO: Remove this and replace it with FullIndex, which has identical semantics.
impl TreeMetrics<TraversalComponent> for PrePostIndex {
    type Update = Pair<isize>;
    type Value = Pair<usize>;

    fn increment_marker(marker: &mut Self::Update, entry: &TraversalComponent) {
        marker.0 += entry.pre_len() as isize;
        marker.1 += entry.post_len() as isize;
    }

    fn decrement_marker(marker: &mut Self::Update, entry: &TraversalComponent) {
        marker.0 -= entry.pre_len() as isize;
        marker.1 -= entry.post_len() as isize;
    }

    fn decrement_marker_by_val(marker: &mut Self::Update, val: &Self::Value) {
        marker.0 -= val.0 as isize;
        marker.1 -= val.1 as isize;
    }

    fn update_offset_by_marker(offset: &mut Self::Value, by: &Self::Update) {
        offset.0 = offset.0.wrapping_add(by.0 as usize);
        offset.1 = offset.1.wrapping_add(by.1 as usize);
    }

    fn increment_offset(offset: &mut Self::Value, by: &TraversalComponent) {
        offset.0 += by.pre_len();
        offset.1 += by.post_len();
    }
}

pub(super) type PositionMap = Pin<Box<ContentTreeRaw<TraversalComponent, PrePostIndex, DEFAULT_IE, DEFAULT_LE>>>;

pub(super) fn positionmap_mut_cursor_at_post(map: &mut PositionMap, pos: usize, stick_end: bool) -> MutCursor<TraversalComponent, PrePostIndex, DEFAULT_IE, DEFAULT_LE> {
    map.mut_cursor_at_query(pos, stick_end,
                                |i| i.1 as usize,
                                |e| e.post_len() as usize)
}

fn count_cursor_pre_len(cursor: &Cursor<TraversalComponent, PrePostIndex, DEFAULT_IE, DEFAULT_LE>) -> usize {
    cursor.count_pos_raw(
        |p| p.0,
        |c| c.pre_len(),
        |c, offset| (c.pre_len()).clamp(0, offset)
    )
}

/// This is a simple struct designed to pull some self contained complexity out of
/// make_position_map.
///
/// The way this works is that the list stays empty, and each time a double-delete range in the
/// origin document is visited we increment the corresponding range here in the visitor.
#[derive(Debug, Clone, Default)]
struct DoubleDeleteVisitor(DoubleDeleteList); // TODO: Make allocation lazy here

impl DoubleDeleteVisitor {
    fn new() -> Self { Self::default() }

    fn find_edit_range(&self, needle: RleKey) -> Result<(&KVPair<DoubleDelete>, usize), (RleKey, usize)> {
        match self.0.find_index(needle) {
            Ok(idx) => {
                Ok((&self.0[idx], idx))
            }
            Err(idx) => {
                if idx == 0 {
                    Err((0, idx))
                } else {
                    Err((self.0[idx - 1].end(), idx))
                }
            }
        }
    }

    /// Find the safe range from last_order backwards.
    fn mark_range(&mut self, double_deletes: &DoubleDeleteList, last_order: LV, min_base: usize) -> (bool, usize) {
        match double_deletes.find_sparse(last_order).0 {
            // Most likely case. Indicates there's no double-delete to deal with in this span.
            Err(base) => (true, base.max(min_base)),
            Ok(dd_entry) => {
                let dd_val = dd_entry.1.excess_deletes;
                let (local_base, local_val, idx) = match self.find_edit_range(last_order) {
                    Err((base, idx)) => (base, 0, idx),
                    Ok((e, idx)) => (e.0, e.1.excess_deletes, idx),
                };

                let safe_base = dd_entry.0.max(local_base);
                if dd_val == local_val {
                    // We've visited it the correct number of times already. This delete is allowed.
                    (true, safe_base)
                } else {
                    // Increment the entry and disallow this delete.
                    let len = last_order - safe_base + 1;
                    // Its kinda overkill to use modify_delete_range_idx. Works though!
                    let modified = self.0.modify_delete_range_idx(safe_base, len, idx, 1, len);
                    assert_eq!(len, modified);
                    (false, safe_base)
                }
            }
        }
    }
}

/// An iterator over positional changes. The positional changes are returned in reverse
/// chronological order (last to first).
///
/// TODO: Rewrite this using ListPatchIter.
#[derive(Debug)]
struct PatchIter<'a> {
    doc: &'a ListCRDT,
    span: TimeSpan,
    map: PositionMap,
    deletes_idx: usize,
    marked_deletes: DoubleDeleteVisitor,
}

/// An iterator over positional changes which handles situations where time is non-linear. This
/// is useful when finding an operation to some arbitrary branch in history - which might not have
/// ever existed in the local time linearization. Eg, if two operations were concurrent, this allows
/// fetching the changes from *either* point in time to the current point in time.
#[derive(Debug)]
struct MultiPositionalChangesIter<'a, I: Iterator<Item=TimeSpan>> { // TODO: Change this to Range<Order>
    /// NOTE: The remaining spans iter must yield in reverse order (highest order to lowest order).
    remaining_spans: I,
    state: PatchIter<'a>,
}

impl ListCRDT {
    pub fn positional_changes_since(&self, order: LV) -> PositionalOp {
        let walker = PatchIter::new_since_order(self, order);
        walker.into_positional_op()
    }

    pub fn attributed_positional_changes_since(&self, order: LV) -> (PositionalOp, SmallVec<CRDTSpan, 1>) {
        let walker = PatchWithAuthorIter::new_since_order(self, order);
        walker.into_attributed_positional_op()
    }

    pub fn positional_changes_since_branch(&self, branch: &[LV]) -> PositionalOp {
        let (a, b) = self.txns.diff(branch, &self.frontier);
        assert_eq!(a.len(), 0);

        // Note the spans are guaranteed to be delivered in reverse order (from last to first).
        // This is what walker expects - since we'll be moving in reverse chronological order here
        // too. Otherwise we'd need to wrap the iterator in Reverse() or reverse the contents.
        let walker = MultiPositionalChangesIter::new_from_iter(self, b.iter().map(|r| r.clone().into()));
        walker.into_positional_op()
    }

    pub fn traversal_changes_since(&self, order: LV) -> TraversalOpSequence {
        self.positional_changes_since(order).into()
    }

    pub fn flat_traversal_since(&self, order: LV) -> TraversalOp {
        let walker = PatchIter::new_since_order(self, order);
        walker.into_traversal(self.text_content.as_ref().unwrap())
    }

    pub fn attributed_traversal_changes_since(&self, order: LV) -> (TraversalOpSequence, SmallVec<CRDTSpan, 1>) {
        let (op, attr) = self.attributed_positional_changes_since(order);
        (op.into(), attr)
    }

    pub fn remote_attr_patches_since(&self, order: LV) -> (TraversalOpSequence, SmallVec<RemoteIdSpan, 1>) {
        let (op, attr) = self.attributed_traversal_changes_since(order);
        (op, attr.iter().map(|span| self.crdt_span_to_remote(*span)).collect())
    }

    pub fn traversal_changes_since_branch(&self, branch: &[LV]) -> TraversalOpSequence {
        self.positional_changes_since_branch(branch).into()
    }
}

// I've gone through a lot of potential designs for this code and settled on this one.
//
// Other options:
//
// 1. Scan the changes, make position map by iterating backwards then iterate forwards again
// re-applying changes, and emit / visit on the way forward. The downside of this is it'd be slower
// and require more code (going backwards is enough, combined with a reverse()). But it might be
// less memory intensive if the run of changes is large. It might also be valuable to write that
// code anyway so we can make an operation stream from the document's start.
//
// 2. Add a 'actually delete' flag somewhere for delete operations. This would almost always be
// true, which would let it RLE very well. This would in turn make the code here simpler when
// dealing with deleted items. But we would incur a permanent memory cost, and make it so we can't
// backtrack to arbitrary version vectors in a general way. So OT peers with pending changes would
// be stuck talking to their preferred peer. This would in turn make networking code more complex.
// (Not that I'm supporting that now, but I want the code to be extensible.
//
// 3. Change to a TP2 OT style, where we assume the OT algorithm understands tombstones. The benefit
// of this is that order would no longer really matter here. No matter how the operation stream is
// generated, we could compose all the operations into a single change. This would make the code
// here simpler and faster, but at the expense of a more complex OT system to implement for web
// peers. I'm not going down that road because the whole point of using OT for peers is that they
// need a very small, simple amount of code to interoperate with the rest of the system. If we're
// asking remote peers (web clients and apps) to include complex merging code, I may as well just
// push them to bundle full CRDT implementations.
//
// The result is that this code is very complex. It also probably adds a lot to binary size because
// of the monomorphized content_tree calls. The upside is that this complexity is entirely self
// contained, and the complexity here allows other systems to work "naturally". But its not perfect.
impl<'a> Iterator for PatchIter<'a> {
    // (post_pos, what happened)
    type Item = (usize, PositionalComponent);

    fn next(&mut self) -> Option<(usize, PositionalComponent)> {
        // We go back through history in reverse order. We need to go in reverse order for a few
        // reasons:
        //
        // - Because of duplicate deletes. If an item has been deleted multiple times, we only want
        // to visit it the "first" time chronologically based on the OrderSpan passed in here.
        // - We need to generate the position map anyway. I
        // it for deletion the *first* time it was deleted chronologically according to span.
        // Another approach would be to store in double_deletes the order of the first delete for
        // each entry, but at some point we might want to generate this map from a different time
        // order. This approach uses less memory and generalizes better, at the expense of more
        // complex code.
        while self.span.len > 0 {
            // So instead of searching for span.offset, we start with span.offset + span.len - 1.
            let span_last_order = self.span.end() - 1;

            // First check if the change was a delete or an insert.
            if let Ok(d) = self.doc.deletes.search_scanning_backwards_sparse(span_last_order, &mut self.deletes_idx) {
                // Its a delete. We need to try to undelete the item, unless the item was deleted
                // multiple times (in which case, it stays deleted for now).
                let base = usize::max(self.span.start, d.0);
                let del_span_size = span_last_order + 1 - base; // TODO: Clean me up
                debug_assert!(del_span_size > 0);

                // d_offset -= span_last_order - base; // equivalent to d_offset -= undelete_here - 1;

                // Ok, undelete here. An earlier version of this code iterated *forwards* amongst
                // the deleted span. This worked correctly and was slightly simpler, but it was a
                // confusing API to use and test because delete changes in particular were sometimes
                // arbitrarily reordered.
                let last_del_target = d.1.start + (span_last_order - d.0);

                // I'm also going to limit what we visit each iteration by the size of the visited
                // item in the range tree. For performance I could hold off looking this up until
                // we've got the go ahead from marked_deletes, but given how rare double deletes
                // are, this is fine.

                let rt_cursor = self.doc.get_unsafe_cursor_after(last_del_target, true);
                // Cap the number of items to undelete each iteration based on the span in content_tree.
                let entry = rt_cursor.get_raw_entry();
                debug_assert!(entry.is_deactivated());
                let first_del_target = usize::max(entry.lv, last_del_target + 1 - del_span_size);

                let (allowed, first_del_target) = self.marked_deletes.mark_range(&self.doc.double_deletes, last_del_target, first_del_target);
                let len_here = last_del_target + 1 - first_del_target;
                // println!("Delete from {} to {}", first_del_target, last_del_target);
                self.span.len -= len_here;

                if allowed {
                    // let len_here = len_here.min((-entry.len) as u32 - rt_cursor.offset as u32);
                    let post_pos = unsafe { rt_cursor.unsafe_count_content_pos() };
                    let mut map_cursor = positionmap_mut_cursor_at_post(&mut self.map, post_pos as _, true);
                    // We call insert instead of replace_range here because the delete doesn't
                    // consume "space".

                    let pre_pos = count_cursor_pre_len(&map_cursor);
                    map_cursor.insert(Del(len_here));

                    // The content might have later been deleted.
                    let entry = PositionalComponent {
                        pos: pre_pos,
                        len: len_here,
                        content_known: false,
                        tag: InsDelTag::Del,
                    };
                    return Some((post_pos, entry));
                } // else continue.
            } else {
                // println!("Insert at {:?} (last order: {})", span, span_last_order);
                // The operation was an insert operation, not a delete operation.
                let mut rt_cursor = self.doc.get_unsafe_cursor_after(span_last_order, true);

                // Check how much we can tag in one go.
                let len_here = usize::min(self.span.len, rt_cursor.offset); // usize? u32? blehh
                debug_assert_ne!(len_here, 0);
                // let base = span_last_order + 1 - len_here; // not needed.
                // let base = u32::max(span.order, span_last_order + 1 - cursor.offset);
                // dbg!(&cursor, len_here);
                rt_cursor.offset -= len_here as usize;

                // Where in the final document are we?
                let post_pos = unsafe { rt_cursor.unsafe_count_content_pos() };

                // So this is also dirty. We need to skip any deletes, which have a size of 0.
                let content_known = rt_cursor.get_raw_entry().is_activated();


                // There's two cases here. Either we're inserting something fresh, or we're
                // cancelling out a delete we found earlier.
                let entry = if content_known {
                    // post_pos + 1 is a hack. cursor_at_offset_pos returns the first cursor
                    // location which has the right position.
                    let mut map_cursor = positionmap_mut_cursor_at_post(&mut self.map, post_pos + 1, true);
                    map_cursor.inner.offset -= 1;
                    let pre_pos = count_cursor_pre_len(&map_cursor);
                    map_cursor.replace_range(Ins { len: len_here, content_known });
                    PositionalComponent {
                        pos: pre_pos,
                        len: len_here,
                        content_known: true,
                        tag: InsDelTag::Ins
                    }
                } else {
                    let mut map_cursor = positionmap_mut_cursor_at_post(&mut self.map, post_pos, true);
                    map_cursor.inner.roll_to_next_entry();
                    map_cursor.delete(len_here as usize);
                    PositionalComponent {
                        pos: count_cursor_pre_len(&map_cursor),
                        len: len_here,
                        content_known: false,
                        tag: InsDelTag::Ins
                    }
                };

                // The content might have later been deleted.

                self.span.len -= len_here;
                return Some((post_pos, entry));
            }
        }
        None
    }
}

impl<'a> PatchIter<'a> {
    // TODO: Consider swapping these two new() functions around as new_since_order is more useful.
    fn new(doc: &'a ListCRDT, span: TimeSpan) -> Self {
        let mut iter = PatchIter {
            doc,
            span,
            map: ContentTreeRaw::new(),
            deletes_idx: doc.deletes.len().wrapping_sub(1),
            marked_deletes: DoubleDeleteVisitor::new(),
        };
        iter.map.insert_at_start(Retain(doc.range_tree.content_len() as _));

        iter
    }

    fn new_since_order(doc: &'a ListCRDT, base_order: LV) -> Self {
        Self::new(doc, doc.linear_changes_since(base_order))
    }

    fn drain(&mut self) {
        for _ in self {}
    }

    fn into_map(mut self) -> PositionMap {
        self.drain();
        self.map
    }

    fn into_positional_op(mut self) -> PositionalOp {
        let mut changes: SmallVec<(usize, PositionalComponent), 10> = (&mut self).collect();
        changes.reverse();
        PositionalOp::from_components(changes, self.doc.text_content.as_ref())
    }

    fn into_traversal(self, resulting_doc: &JumpRopeBuf) -> TraversalOp {
        let map = self.into_map();
        map_to_traversal(&map, resulting_doc)
    }
}

/// This is an iterator which wraps PatchIter and yields information about patches, as well as
/// authorship of those patches.
struct PatchWithAuthorIter<'a> {
    actual_base: LV,
    state: PatchIter<'a>,
    client_order_idx: usize,
    crdt_loc: CRDTId,
}

// Internally this is a bit fancy. To avoid adding extra complexity to PatchIter, this drives
// PatchIter with span ranges limited by authorship. This kinda breaks the abstraction boundary of
// PatchIter. And note the way this is written, this can't be used with the multi positional
// changes stuff.
impl<'a> Iterator for PatchWithAuthorIter<'a> {
    type Item = (usize, PositionalComponent, CRDTId);

    fn next(&mut self) -> Option<Self::Item> {
        if self.state.span.len == 0 {
            // Grab the next span back in time and pass to the internal iterator.
            if self.actual_base == self.state.span.start {
                // We're done here.
                return None;
            }

            let span_last_order = self.state.span.start - 1;
            let val = self.state.doc.client_with_time
                .search_scanning_backwards_sparse(span_last_order, &mut self.client_order_idx)
                .unwrap(); // client_with_order is packed, so its impossible to skip entries.

            if val.0 < self.actual_base {
                // Only take down to actual_base.
                self.state.span = TimeSpan {
                    start: self.actual_base,
                    len: self.state.span.start - self.actual_base
                };
                self.crdt_loc = CRDTId {
                    agent: val.1.loc.agent,
                    seq: val.1.loc.seq + (self.actual_base - val.0)
                };
            } else {
                // Take the whole entry.
                self.state.span = TimeSpan {
                    start: val.0,
                    len: self.state.span.start - val.0
                };
                self.crdt_loc = val.1.loc;
            }
            assert_ne!(self.state.span.len, 0);
        }

        if let Some((post_pos, c)) = self.state.next() {
            Some((post_pos, c, CRDTId {
                agent: self.crdt_loc.agent,
                seq: self.crdt_loc.seq + self.state.span.len
            }))
        } else { None }
    }
}

impl<'a> PatchWithAuthorIter<'a> {
    fn new(doc: &'a ListCRDT, span: TimeSpan) -> Self {
        Self {
            actual_base: span.start,
            state: PatchIter::new(doc, TimeSpan { start: span.end(), len: 0 }),
            client_order_idx: doc.client_with_time.0.len().wrapping_sub(1),
            crdt_loc: Default::default()
        }
    }

    fn new_since_order(doc: &'a ListCRDT, base_order: LV) -> Self {
        Self::new(doc, doc.linear_changes_since(base_order))
    }

    fn into_attributed_positional_op(mut self) -> (PositionalOp, SmallVec<CRDTSpan, 1>) {
        let mut changes = SmallVec::<(usize, PositionalComponent), 10>::new();
        let mut attribution = SmallVec::<CRDTSpan, 1>::new();

        for (post_pos, c, loc) in &mut self {
            attribution.push_reversed_rle(CRDTSpan {
                loc,
                len: c.len
            });
            changes.push((post_pos, c));
        }

        changes.reverse();
        attribution.reverse();
        let op = PositionalOp::from_components(changes, self.state.doc.text_content.as_ref());
        (op, attribution)
    }

}

// This code - while correct - is in danger of being removed because it might not be usable due to
// weaknesses in using OT across multiple servers.
impl<'a, I: Iterator<Item=TimeSpan>> Iterator for MultiPositionalChangesIter<'a, I> {
    type Item = (usize, PositionalComponent);

    fn next(&mut self) -> Option<Self::Item> {
        if self.state.span.len == 0 {
            if let Some(span) = self.remaining_spans.next() {
                debug_assert!(span.start < self.state.span.start);
                assert!(span.len > 0);
                self.state.span = span;
            } else { return None; }
        }

        self.state.next()
    }
}

impl<'a, I: Iterator<Item=TimeSpan>> MultiPositionalChangesIter<'a, I> {
    fn new_from_iter(doc: &'a ListCRDT, iter: I) -> Self {
        MultiPositionalChangesIter {
            remaining_spans: iter,
            state: PatchIter::new(doc, TimeSpan::default())
        }
    }

    // These methods are duplicated in PositionalChangesIter. I could do trait magic to share them
    // but I'm not convinced jumping through the extra hoops is worth it.
    fn drain(&mut self) {
        for _ in self {}
    }

    fn into_map(mut self) -> PositionMap {
        self.drain();
        self.state.map
    }

    fn into_positional_op(mut self) -> PositionalOp {
        let mut changes: SmallVec<(usize, PositionalComponent), 10> = (&mut self).collect();
        changes.reverse();
        PositionalOp::from_components(changes, self.state.doc.text_content.as_ref())
    }

    fn into_traversal(self, resulting_doc: &JumpRopeBuf) -> TraversalOp {
        let map = self.into_map();
        map_to_traversal(&map, resulting_doc)
    }
}

fn map_to_traversal(map: &PositionMap, resulting_doc: &JumpRopeBuf) -> TraversalOp {
    use TraversalComponent::*;

    let mut op = TraversalOp::new();
    // TODO: Could use doc.chars() for this, but I think it'll be slower. Benchmark!
    let mut post_len: usize = 0;
    for entry in map.raw_iter() {
        match entry {
            Ins { len, content_known: true } => {
                let range = post_len..(post_len+len);
                op.content.extend(resulting_doc.borrow().slice_chars(range));
                post_len += len;
            }
            Retain(len) => {
                post_len += len;
            }
            _ => {}
        }
        if !entry.is_noop() {
            op.traversal.push_rle(entry);
        }
    }
    op
}

#[cfg(test)]
mod test {
    use jumprope::JumpRope;
    use rand::prelude::*;
    use smallvec::smallvec;

    use rle::AppendRle;

    use crate::list::{ListCRDT, ROOT_LV};
    use crate::list::positional::*;
    use crate::list::ot::positionmap::*;
    use crate::list::ot::traversal::*;
    use crate::test_helpers::make_random_change;

// use crate::list::external_txn::{RemoteTxn, RemoteId};

    #[test]
    fn simple_position_map() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there".into()); // 0-7
        doc.local_delete(0, 2, 3); // "hiere" 8-11

        let (op, attr) = doc.attributed_positional_changes_since(0);
        // dbg!(&op, attr);

        use InsDelTag::*;
        assert_eq!(op, PositionalOp {
            components: smallvec![
                PositionalComponent {pos: 0, len: 2, content_known: true, tag: Ins},
                PositionalComponent {pos: 2, len: 3, content_known: false, tag: Ins},
                PositionalComponent {pos: 5, len: 3, content_known: true, tag: Ins},

                PositionalComponent {pos: 2, len: 3, content_known: false, tag: Del},
            ],
            content: "hiere".into(),
        });

        assert!(attr.iter().eq(&[CRDTSpan {
            loc: CRDTId {
                agent: 0,
                seq: 0,
            },
            len: 11,
        }]));
    }

    #[test]
    fn check_double_deletes() {
        let mut doc1 = ListCRDT::new();
        doc1.get_or_create_agent_id("a");
        doc1.local_insert(0, 0, "hi there".into());

        let mut doc2 = ListCRDT::new();
        doc1.replicate_into(&mut doc2);
        doc2.get_or_create_agent_id("b"); // Agent IDs are consistent to make testing easier.

        // Overlapping but distinct.
        doc1.local_delete(0, 2, 3); // -> 'hiere'
        doc2.local_delete(1, 4, 3); // -> 'hi te'

        doc2.replicate_into(&mut doc1); // 'hie'
        doc1.replicate_into(&mut doc2); // 'hie'

        // "hi there" -> "hiere" -> "hie"

        let walker = PatchWithAuthorIter::new_since_order(&doc2, 0);
        let (positional_op, attr) = walker.into_attributed_positional_op();

        // dbg!(&doc2.client_with_order);
        // dbg!(&attr);

        assert!(attr.iter().eq(&[
            CRDTSpan { loc: CRDTId { agent: 0, seq: 0 }, len: 8 },
            CRDTSpan { loc: CRDTId { agent: 1, seq: 0 }, len: 3 },
            CRDTSpan { loc: CRDTId { agent: 0, seq: 8 }, len: 2 },
        ]));

        use InsDelTag::*;
        if doc2.text_content.is_some() {
            assert_eq!(positional_op, PositionalOp {
                components: smallvec![
                    PositionalComponent { pos: 0, len: 2, content_known: true, tag: Ins },
                    PositionalComponent { pos: 2, len: 5, content_known: false, tag: Ins },
                    PositionalComponent { pos: 7, len: 1, content_known: true, tag: Ins },

                    PositionalComponent { pos: 2, len: 5, content_known: false, tag: Del },
                ],
                content: "hie".into(),
            })
        }

        // There's no good reason to iterate twice except for API convenience.
        let walker = PatchIter::new_since_order(&doc2, 0);
        let map = walker.into_map();

        assert!(&map.iter().eq(std::iter::once(TraversalComponent::Ins {
            len: 3,
            content_known: true,
        })));

        if let Some(text_content) = doc2.text_content.as_ref() {
            // The fuzzer will do a much better job of testing this.
            let traversal = map_to_traversal(&map, text_content);
            assert_eq!(traversal, TraversalOp {
                traversal: smallvec![TraversalComponent::Ins {len: 3, content_known: true}],
                content: "hie".into(),
            });
        }
    }

    fn ot_single_doc_fuzz(rng: &mut SmallRng, num_ops: usize) {
        let mut doc = ListCRDT::new();

        let agent_0 = doc.get_or_create_agent_id("0");
        let agent_1 = doc.get_or_create_agent_id("1");

        for _i in 0..50 {
            make_random_change(&mut doc, None, agent_0, rng);
        }

        let midpoint_order = doc.get_next_lv();
        let midpoint_content = if doc.has_content() { Some(doc.to_string()) } else { None };

        let mut expect_author = vec![];
        // Actually if all the changes above are given to agent_0, this should be doc.next_order() / 0
        let mut next_seq_0 = doc.client_data[agent_0 as usize].get_next_seq();
        let mut next_seq_1 = doc.client_data[agent_1 as usize].get_next_seq();

        for _i in 0..num_ops {
            // Most changes from agent 0 to keep things frothy.
            let agent = if rng.gen_bool(0.9) { agent_0 } else { agent_1 };
            let op_len = make_random_change(&mut doc, None, agent, rng).len();

            let next_seq = if agent == agent_0 { &mut next_seq_0 } else { &mut next_seq_1 };
            expect_author.push_rle(CRDTSpan {
                loc: CRDTId { agent, seq: *next_seq },
                len: op_len
            });
            *next_seq += op_len;
        }
        // dbg!(ops);

        let walker = PatchWithAuthorIter::new_since_order(&doc, midpoint_order);
        let (positional_op, attr) = walker.into_attributed_positional_op();

        assert!(attr.iter().eq(&expect_author));

        // Bleh we don't need to iterate twice here except the API is awks.
        let walker = PatchIter::new_since_order(&doc, midpoint_order);
        let map = walker.into_map();

        // Ok we have a few things to check:
        // 1. The returned map shouldn't contain any inserts with unknown content
        for e in map.raw_iter() {
            if let Ins { content_known, .. } = e {
                assert!(content_known);
            }
        }

        if let (Some(text_content), Some(midpoint_content)) = (doc.text_content.as_ref(), midpoint_content) {
            // 2. The returned map should be able to be converted to a traversal operation and applied
            //    to the midpoint, returning the current document state.
            let traversal = map_to_traversal(&map, text_content);
            // dbg!(&traversal);

            let result = traversal.apply_to_string(midpoint_content.as_str());
            // dbg!(doc.text_content, result);
            assert_eq!(text_content, &result);

            // 3. We should also be able to apply all the changes one by one to the midpoint state and
            //    arrive at the same result.
            let mut midpoint_rope = JumpRope::from(midpoint_content.as_str());
            positional_op.apply_to_rope(&mut midpoint_rope);
            assert_eq!(text_content, &midpoint_rope);
        } else {
            eprintln!("WARNING: Cannot test properly due to missing text content");
        }
    }

    #[test]
    fn ot_single_document_fuzz() {
        // Check that when we query all the changes from a single document, the result is the same
        // (same values, same order) as we get from ot_changes_since.

        for i in 0..100 {
            let mut rng = SmallRng::seed_from_u64(i);
            println!("{}", i);
            ot_single_doc_fuzz(&mut rng, 50);
        }
    }

    #[test]
    fn ot_single_doc_fuzz_once() {
        let mut rng = SmallRng::seed_from_u64(5);
        ot_single_doc_fuzz(&mut rng, 5);
    }

    #[test]
    #[ignore]
    fn ot_single_document_fuzz_forever() {
        for i in 0.. {
            if i % 1000 == 0 { println!("{}", i); }
            let mut rng = SmallRng::seed_from_u64(i);
            ot_single_doc_fuzz(&mut rng, 50);
        }
    }

    #[test]
    fn midpoint_cursor_has_correct_count() {
        // Regression for a bug in range tree.
        let mut tree: PositionMap = ContentTreeRaw::new();
        tree.insert_at_start(TraversalComponent::Retain(10));

        let cursor = positionmap_mut_cursor_at_post(&mut tree, 4, true);
        assert_eq!(count_cursor_pre_len(&cursor), 4);
        // TODO: And check post_len is also 4...
    }

    #[test]
    fn complex_edits() {
        let doc = crate::list::time::history::test::complex_multientry_doc();

        // Ok, now there's a bunch of interesting diffs to generate here. Frontier is [4,6] but
        // we have two branches - with orders [0-2, 5-6] and [3-4]

        let full_history = doc.positional_changes_since_branch(&[ROOT_LV]);
        use InsDelTag::*;
        assert_eq!(full_history, PositionalOp {
            components: smallvec![
                PositionalComponent { pos: 0, len: 5, content_known: true, tag: Ins },
                PositionalComponent { pos: 3, len: 2, content_known: true, tag: Ins },
            ],
            content: "aaabbAA".into(),
        });

        let left_history = doc.positional_changes_since_branch(&[6]);
        assert_eq!(left_history, PositionalOp {
            components: smallvec![
                PositionalComponent { pos: 5, len: 2, content_known: true, tag: Ins },
            ],
            content: "bb".into(),
        });

        let right_history = doc.positional_changes_since_branch(&[4]);
        assert_eq!(right_history, PositionalOp {
            components: smallvec![
                PositionalComponent { pos: 0, len: 5, content_known: true, tag: Ins },
            ],
            content: "aaaAA".into(),
        });

        // dbg!(right_history);
    }
}