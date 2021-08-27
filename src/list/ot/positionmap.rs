use crate::list::{Order, ListCRDT, DoubleDeleteList};
use crate::range_tree::*;
use crate::order::OrderSpan;
use std::pin::Pin;
use crate::list::double_delete::DoubleDelete;
use crate::rle::{KVPair, RleKey, RleSpanHelpers, AppendRLE, RleKeyed, Rle};
use crate::list::ot::traversal::{TraversalComponent, TraversalOp, TraversalOpSequence};
use ropey::Rope;
use TraversalComponent::*;
use crate::list::ot::positional::{PositionalComponent, InsDelTag, PositionalOp};
use smallvec::SmallVec;
use std::iter::Empty;
use crate::splitable_span::SplitableSpan;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) struct PrePostIndex;

impl TreeIndex<TraversalComponent> for PrePostIndex {
    type IndexUpdate = Pair<i32>;
    type IndexValue = Pair<u32>;

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &TraversalComponent) {
        marker.0 += entry.pre_len() as i32;
        marker.1 += entry.post_len() as i32;
    }

    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &TraversalComponent) {
        marker.0 -= entry.pre_len() as i32;
        marker.1 -= entry.post_len() as i32;
    }

    fn decrement_marker_by_val(marker: &mut Self::IndexUpdate, val: &Self::IndexValue) {
        marker.0 -= val.0 as i32;
        marker.1 -= val.1 as i32;
    }

    fn update_offset_by_marker(offset: &mut Self::IndexValue, by: &Self::IndexUpdate) {
        offset.0 = offset.0.wrapping_add(by.0 as u32);
        offset.1 = offset.1.wrapping_add(by.1 as u32);
    }

    fn increment_offset(offset: &mut Self::IndexValue, by: &TraversalComponent) {
        offset.0 += by.pre_len();
        offset.1 += by.post_len();
    }
}

pub(super) type PositionMap = Pin<Box<RangeTree<TraversalComponent, PrePostIndex, DEFAULT_IE, DEFAULT_LE>>>;

impl RangeTree<TraversalComponent, PrePostIndex, DEFAULT_IE, DEFAULT_LE> {
    // pub fn content_len(&self) -> usize {
    //     self.count as usize
    // }

    pub fn cursor_at_post(&self, pos: usize, stick_end: bool) -> Cursor<TraversalComponent, PrePostIndex, DEFAULT_IE, DEFAULT_LE> {
        self.cursor_at_query(pos, stick_end,
                             |i| i.1 as usize,
                             |e| e.post_len() as usize)
    }
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

    // fn swap_index(idx: RleKey) -> RleKey { RleKey::MAX - idx }

    fn find_edit_range(&self, needle: RleKey) -> Result<(&KVPair<DoubleDelete>, usize), (RleKey, usize)> {
        match self.0.search(needle) {
            Ok(idx) => {
                Ok((&self.0.0[idx], idx))
            }
            Err(idx) => {
                if idx == 0 {
                    Err((0, idx))
                } else {
                    Err((self.0.0[idx - 1].end(), idx))
                }
            }
        }
    }

    /// Find the safe range from last_order backwards.
    fn mark_range(&mut self, double_deletes: &DoubleDeleteList, last_order: Order, min_base: u32) -> (bool, u32) {
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
// of the monomorphized range_tree calls. The upside is that this complexity is entirely self
// contained, and the complexity here allows other systems to work "naturally". But its not perfect.

impl<V: SplitableSpan + RleKeyed + Clone + Sized> Rle<V> {
    fn search_backwards(&self, needle: RleKey, idx: &mut usize) -> Option<(&V, RleKey)> {
        // This conditional looks inverted given we're looping backwards, but I'm using
        // wrapping_sub - so when we reach the end the index wraps around and we'll hit usize::MAX.
        while *idx < self.0.len() {
            let e = &self.0[*idx];
            if e.get_rle_key() <= needle {
                return if e.end() > needle {
                    Some((e, needle - e.get_rle_key()))
                } else {
                    None
                };
            }
            *idx = idx.wrapping_sub(1);
        }
        None
    }
}

impl ListCRDT {
    fn next_positional_change(
        &self,
        span: &OrderSpan,
        map: &mut PositionMap,
        marked_deletes: &mut DoubleDeleteVisitor,
        deletes_idx: &mut usize
    ) -> (u32, Option<(u32, PositionalComponent)>) {
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
        assert!(span.len > 0);

        // dbg!(&map, &marked_deletes, &span);

        // So instead of searching for span.offset, we start with span.offset + span.len - 1.

        // First check if the change was a delete or an insert.
        let span_last_order = span.end() - 1;

        if let Some((d, d_offset)) = self.deletes.search_backwards(span_last_order, deletes_idx) {
            // Its a delete. We need to try to undelete the item, unless the item was deleted
            // multiple times (in which case, it stays deleted for now).
            let base = u32::max(span.order, d.0);
            let del_span_size = span_last_order + 1 - base; // TODO: Clean me up
            debug_assert!(del_span_size > 0);

            // d_offset -= span_last_order - base; // equivalent to d_offset -= undelete_here - 1;

            // Ok, undelete here. An earlier version of this code iterated *forwards* amongst
            // the deleted span. This worked correctly and was slightly simpler, but it was a
            // confusing API to use and test because delete changes in particular were sometimes
            // arbitrarily reordered.

            let last_del_target = d.1.order + d_offset;

            // I'm also going to limit what we visit each iteration by the size of the visited
            // item in the range tree. For performance I could hold off looking this up until
            // we've got the go ahead from marked_deletes, but given how rare double deletes
            // are, this is fine.

            let rt_cursor = self.get_cursor_after(last_del_target, true);
            // Cap the number of items to undelete each iteration based on the span in range_tree.
            let entry = rt_cursor.get_raw_entry();
            debug_assert!(entry.is_deactivated());
            let first_del_target = u32::max(entry.order, last_del_target + 1 - del_span_size);

            let (allowed, first_del_target) = marked_deletes.mark_range(&self.double_deletes, last_del_target, first_del_target);
            let len_here = last_del_target + 1 - first_del_target;
            // println!("Delete from {} to {}", first_del_target, last_del_target);

            let op = if allowed {
                // let len_here = len_here.min((-entry.len) as u32 - rt_cursor.offset as u32);
                let post_pos = unsafe { rt_cursor.count_pos() };
                let mut map_cursor = map.cursor_at_post(post_pos as _, true);
                // We call insert instead of replace_range here because the delete doesn't
                // consume "space".

                let pre_pos = unsafe { map_cursor.count_pos() }.0;
                unsafe { map.insert(&mut map_cursor, Del(len_here), null_notify); }

                // The content might have later been deleted.
                let entry = PositionalComponent {
                    pos: pre_pos,
                    len: len_here,
                    content_known: false,
                    tag: InsDelTag::Del,
                };
                Some((post_pos, entry))
            } else { None };

            (len_here, op)
        } else {
            // println!("Insert at {:?} (last order: {})", span, span_last_order);
            // The operation was an insert operation, not a delete operation.
            let mut rt_cursor = self.get_cursor_after(span_last_order, true);

            // Check how much we can tag in one go.
            let len_here = u32::min(span.len, rt_cursor.offset as _); // usize? u32? blehh
            debug_assert_ne!(len_here, 0);
            // let base = span_last_order + 1 - len_here; // not needed.
            // let base = u32::max(span.order, span_last_order + 1 - cursor.offset);
            // dbg!(&cursor, len_here);
            rt_cursor.offset -= len_here as usize;

            // Where in the final document are we?
            let post_pos = unsafe { rt_cursor.count_pos() };

            // So this is also dirty. We need to skip any deletes, which have a size of 0.
            let content_known = rt_cursor.get_raw_entry().is_activated();


            // There's two cases here. Either we're inserting something fresh, or we're
            // cancelling out a delete we found earlier.
            let entry = if content_known {
                // post_pos + 1 is a hack. cursor_at_offset_pos returns the first cursor
                // location which has the right position.
                let mut map_cursor = map.cursor_at_post(post_pos as usize + 1, true);
                map_cursor.offset -= 1;
                let pre_pos = unsafe { map_cursor.count_pos() }.0;
                unsafe { map.replace_range(&mut map_cursor, Ins { len: len_here, content_known }, null_notify); }
                PositionalComponent {
                    pos: pre_pos,
                    len: len_here,
                    content_known: true,
                    tag: InsDelTag::Ins
                }
            } else {
                let mut map_cursor = map.cursor_at_post(post_pos as usize, true);
                map_cursor.roll_to_next_entry();
                unsafe { map.delete(&mut map_cursor, len_here as usize, null_notify); }
                PositionalComponent {
                    pos: unsafe { map_cursor.count_pos() }.0,
                    len: len_here,
                    content_known: false,
                    tag: InsDelTag::Ins
                }
            };

            // The content might have later been deleted.

            (len_here, Some((post_pos, entry)))
        }
    }

    pub fn positional_changes_since(&self, order: Order) -> PositionalOp {
        let mut walker = ReversePositionalOpWalker::new_since_order(self, order);
        walker.get_positional_op()
    }

    pub fn positional_changes_since_branch(&self, branch: &[Order]) -> PositionalOp {
        let (a, b) = self.diff(branch, &self.frontier);
        assert_eq!(a.len(), 0);

        // Note the spans are guaranteed to be delivered in reverse order (from last to first).
        // This is what walker expects - since we'll be moving in reverse chronological order here
        // too. Otherwise we'd need to wrap the iterator in Reverse() or reverse the contents.
        let mut walker = ReversePositionalOpWalker::new_from_iter(self, b.iter().copied());
        walker.get_positional_op()
    }

    pub fn traversal_changes_since(&self, order: Order) -> TraversalOpSequence {
        self.positional_changes_since(order).into()
    }

    pub fn traversal_changes_since_branch(&self, branch: &[Order]) -> TraversalOpSequence {
        self.positional_changes_since_branch(branch).into()
    }
}

#[derive(Debug)]
struct ReversePositionalOpWalker<'a, I: Iterator<Item=OrderSpan>> {
    doc: &'a ListCRDT,
    /// NOTE: The remaining spans list must be in reverse order.
    remaining_spans: I,
    span: OrderSpan,
    map: PositionMap,
    deletes_idx: usize,
    marked_deletes: DoubleDeleteVisitor,
}

impl<'a, I: Iterator<Item=OrderSpan>> Iterator for ReversePositionalOpWalker<'a, I> {
    type Item = (u32, PositionalComponent);

    fn next(&mut self) -> Option<Self::Item> {
        if self.span.len == 0 {
            if let Some(span) = self.remaining_spans.next() {
                debug_assert!(span.order < self.span.order);
                assert!(span.len > 0);
                self.span = span;
            } else { return None; }
        }

        while self.span.len != 0 {
            let (len_here, op) = self.doc.next_positional_change(&self.span, &mut self.map, &mut self.marked_deletes, &mut self.deletes_idx);
            self.span.len -= len_here;
            if op.is_some() || self.span.len == 0 {
                return op;
            } // Else we're in a span of double deleted items. Keep scanning.
        }

        None
    }
}

impl<'a> ReversePositionalOpWalker<'a, Empty<OrderSpan>> {
    fn new_since_order(doc: &'a ListCRDT, base_order: Order) -> Self {
        // Alternately I could use std::iter::once. I think this is a bit cleaner but I could go
        // either way on it.
        let mut walker = Self::new_from_iter(doc, std::iter::empty());
        walker.span = doc.linear_changes_since(base_order);
        walker
    }
}

impl<'a, I: Iterator<Item=OrderSpan>> ReversePositionalOpWalker<'a, I> {
    fn new_from_iter(doc: &'a ListCRDT, iter: I) -> Self {
        let mut iter = ReversePositionalOpWalker {
            doc,
            remaining_spans: iter,
            span: OrderSpan::default(),
            map: RangeTree::new(),
            deletes_idx: doc.deletes.0.len().wrapping_sub(1),
            marked_deletes: DoubleDeleteVisitor::new(),
        };

        iter.map.insert_at_start(Retain(doc.range_tree.content_len() as _), null_notify);
        iter
    }

    fn drain(&mut self) {
        while let Some(_) = self.next() {}
    }

    fn into_map(mut self) -> PositionMap {
        self.drain();
        self.map
    }

    fn get_positional_op(&mut self) -> PositionalOp {
        let mut changes: SmallVec<[(u32, PositionalComponent); 10]> = SmallVec::new();
        while let Some((post_pos, e)) = self.next() {
            changes.push((post_pos, e));
        }
        changes.reverse();
        PositionalOp::from_components(&changes, self.doc.text_content.as_ref())
    }

    fn into_traversal(self, resulting_doc: &Rope) -> TraversalOp {
        let map = self.into_map();
        map_to_traversal(&map, resulting_doc)
    }
}

fn map_to_traversal(map: &PositionMap, resulting_doc: &Rope) -> TraversalOp {
    use TraversalComponent::*;

    let mut op = TraversalOp::new();
    // TODO: Could use doc.chars() for this, but I think it'll be slower. Benchmark!
    let mut post_len: u32 = 0;
    for entry in map.iter() {
        match entry {
            Ins { len, content_known: true } => {
                op.content.extend(resulting_doc.chars_at(post_len as usize).take(len as usize));
                post_len += len;
            }
            Retain(len) => {
                post_len += len;
            }
            _ => {}
        }
        op.traversal.push_rle(entry);
    }
    op
}

#[cfg(test)]
mod test {
    use crate::list::{ListCRDT, ROOT_ORDER};
    use rand::prelude::SmallRng;
    use rand::SeedableRng;
    use crate::fuzz_helpers::make_random_change;
    use crate::list::ot::positionmap::{map_to_traversal, PositionMap, ReversePositionalOpWalker};
    use super::TraversalComponent::*;
    use crate::range_tree::{RangeTree, null_notify, Pair};
    use crate::list::ot::traversal::{TraversalComponent, TraversalOp};
    use crate::list::ot::positional::{PositionalOp, PositionalComponent, InsDelTag};
    use ropey::Rope;
    use smallvec::smallvec;
    // use crate::list::external_txn::{RemoteTxn, RemoteId};

    #[test]
    fn simple_position_map() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there".into()); // 0-7
        doc.local_delete(0, 2, 3); // "hiere" 8-11

        let op = doc.positional_changes_since(0);
        // dbg!(&op);

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
    }

    #[test]
    fn check_double_deletes() {
        let mut doc1 = ListCRDT::new();
        doc1.get_or_create_agent_id("a");
        doc1.local_insert(0, 0, "hi there".into());

        let mut doc2 = ListCRDT::new();
        doc2.get_or_create_agent_id("b");
        doc1.replicate_into(&mut doc2);

        // Overlapping but distinct.
        doc1.local_delete(0, 2, 3); // -> 'hiere'
        doc2.local_delete(0, 4, 3); // -> 'hi te'

        doc2.replicate_into(&mut doc1); // 'hie'
        doc1.replicate_into(&mut doc2); // 'hie'

        // "hi there" -> "hiere" -> "hie"

        let mut walker = ReversePositionalOpWalker::new_since_order(&doc2, 0);
        let positional_op = walker.get_positional_op();

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

        let map = walker.into_map();

        assert!(&map.merged_iter().eq(std::iter::once(TraversalComponent::Ins {
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

        let agent = doc.get_or_create_agent_id("seph");

        for _i in 0..50 {
            make_random_change(&mut doc, None, agent, rng);
        }

        let midpoint_order = doc.get_next_order();
        let midpoint_content = if doc.has_content() { Some(doc.to_string()) } else { None };

        let mut ops = vec![];
        for _i in 0..num_ops {
            let op = make_random_change(&mut doc, None, agent, rng);
            ops.push(op);
        }
        // dbg!(ops);

        let mut walker = ReversePositionalOpWalker::new_since_order(&doc, midpoint_order);
        let positional_op = walker.get_positional_op();
        let map = walker.into_map();

        // Ok we have a few things to check:
        // 1. The returned map shouldn't contain any inserts with unknown content
        for e in map.iter() {
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
            let mut midpoint_rope = Rope::from(midpoint_content.as_str());
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
        let mut tree: PositionMap = RangeTree::new();
        tree.insert_at_start(TraversalComponent::Retain(10), null_notify);

        let cursor = tree.cursor_at_post(4, true);
        assert_eq!(unsafe { cursor.count_pos() }, Pair(4, 4));
    }

    #[test]
    fn complex_edits() {
        let doc = crate::list::time::test::complex_multientry_doc();

        // Ok, now there's a bunch of interesting diffs to generate here. Frontier is [4,6] but
        // we have two branches - with orders [0-2, 5-6] and [3-4]

        let full_history = doc.positional_changes_since_branch(&[ROOT_ORDER]);
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