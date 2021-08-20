use crate::list::{Order, ListCRDT, DoubleDeleteList};
use crate::range_tree::*;
use crate::order::OrderSpan;
use std::pin::Pin;
use crate::list::double_delete::DoubleDelete;
use crate::rle::{KVPair, RleKey, RleSpanHelpers, AppendRLE};
use crate::list::ot::{TraversalOp, TraversalComponent};
use ropey::Rope;

use TraversalComponent::*;

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

pub(super) type PositionMap = Pin<Box<RangeTree<TraversalComponent, PrePostIndex>>>;

impl RangeTree<TraversalComponent, PrePostIndex> {
    // pub fn content_len(&self) -> usize {
    //     self.count as usize
    // }

    pub fn cursor_at_post(&self, pos: usize, stick_end: bool) -> Cursor<TraversalComponent, PrePostIndex> {
        self.cursor_at_query(pos, stick_end,
                             |i| i.1 as usize,
                             |e| e.post_len() as usize)
        // self.cursor_at_query(pos, stick_end,
        //                      |i| i as usize,
        //                      |e| e.content_len())
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

impl ListCRDT {
    pub(super) fn ot_changes_since<V>(&self, mut span: OrderSpan, mut visit: V) -> PositionMap
    where V: FnMut(u32, u32, TraversalComponent) {
        // println!("ot_changes_since {:?}", span);
        // I've gone through a lot of potential designs for this code and settled on this one.
        //
        // Other options:
        //
        // 1. Scan the changes, make position map by iterating backwards then iterate forwards again
        // re-applying changes, and emit / visit on the way forward. The downside of this is it'd be
        // slower and require more code (going backwards is enough, combined with a reverse()). But
        // it might be less memory intensive if the run of changes is large. It might also be
        // valuable to write that code anyway so we can make an operation stream from the document's
        // start.
        //
        // 2. Add a 'actually delete' flag somewhere for delete operations. This would almost always
        // be true, which would let it RLE very well. This would in turn make the code here simpler
        // when dealing with deleted items. But we would incur a permanent memory cost, and make it
        // so we can't backtrack to arbitrary version vectors in a general way. So OT peers with
        // pending changes would be stuck talking to their preferred peer. This would in turn make
        // networking code more complex. (Not that I'm supporting that now, but I want the code to
        // be extensible.
        //
        // 3. Change to a TP2 OT style, where we assume the OT algorithm understands tombstones. The
        // benefit of this is that order would no longer really matter here. No matter how the
        // operation stream is generated, we could compose all the operations into a single change.
        // This would make the code here simpler and faster, but at the expense of a more complex OT
        // system to implement for web peers. I'm not going down that road because the whole point
        // of using OT for peers is that they need a very small, simple amount of code to
        // interoperate with the rest of the system. If we're asking remote peers (web clients and
        // apps) to include complex merging code, I may as well just push them to bundle full CRDT
        // implementations.
        //
        // The result is that this code is very complex. It also probably adds a lot to binary size
        // because of the monomorphized range_tree calls. The upside is that this complexity is
        // entirely self contained, and the complexity here allows other systems to work
        // "naturally". But its not perfect.

        assert_eq!(span.end(), self.get_next_order());

        let mut map: PositionMap = RangeTree::new();
        map.insert_at_start(Retain(self.range_tree.content_len() as _), null_notify);

        // So the way this works is if we find an item has been double-deleted, we'll mark in this
        // empty range until marked_deletes equals self.double_deletes.
        // let mut marked_deletes = DoubleDeleteList::new();
        let mut marked_deletes = DoubleDeleteVisitor::new();

        // Now we go back through history in reverse order. We need to go in reverse order for a few reasons:
        //
        // - Because of duplicate deletes. If an item has been deleted multiple times, we only want
        // to visit it the "first" time chronologically based on the OrderSpan passed in here.
        // - We need to generate the position map anyway. I
        // it for deletion the *first* time it was deleted chronologically according to span.
        // Another approach would be to store in double_deletes the order of the first delete for
        // each entry, but at some point we might want to generate this map from a different time
        // order. This approach uses less memory and generalizes better, at the expense of more
        // complex code.

        while span.len > 0 {
            // dbg!(&map, &marked_deletes, &span);

            // So instead of searching for span.offset, we start with span.offset + span.len - 1.

            // First check if the change was a delete or an insert.
            let span_last_order = span.end() - 1;

            // TODO: Replace with a search iterator. We're binary searching with ordered search keys.
            if let Some((d, d_offset)) = self.deletes.find(span_last_order) {
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

                if allowed {
                    // let len_here = len_here.min((-entry.len) as u32 - rt_cursor.offset as u32);
                    let post_pos = rt_cursor.count_pos();
                    let mut map_cursor = map.cursor_at_post(post_pos as _, true);
                    // We call insert instead of replace_range here because the delete doesn't
                    // consume "space".
                    let entry = Del(len_here);
                    let pre_pos = map_cursor.count_pos().0;
                    map.insert(&mut map_cursor, entry, null_notify);

                    // The content might have later been deleted.
                    visit(post_pos, pre_pos, entry);
                }

                span.len -= len_here;
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
                let post_pos = rt_cursor.count_pos();

                // So this is also dirty. We need to skip any deletes, which have a size of 0.
                let content_known = rt_cursor.get_raw_entry().is_activated();

                let entry = Ins { len: len_here, content_known };

                // There's two cases here. Either we're inserting something fresh, or we're
                // cancelling out a delete we found earlier.
                let pre_pos = if content_known {
                    // post_pos + 1 is a hack. cursor_at_offset_pos returns the first cursor
                    // location which has the right position.
                    let mut map_cursor = map.cursor_at_post(post_pos as usize + 1, true);
                    map_cursor.offset -= 1;
                    let pre_pos = map_cursor.count_pos().0;
                    map.replace_range(&mut map_cursor, entry, null_notify);
                    pre_pos
                } else {
                    let mut map_cursor = map.cursor_at_post(post_pos as usize, true);
                    map_cursor.roll_to_next_entry();
                    map.delete(&mut map_cursor, len_here as usize, null_notify);
                    map_cursor.count_pos().0
                };

                // The content might have later been deleted.
                visit(post_pos, pre_pos, entry);

                span.len -= len_here;
            }
        }

        map
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
        op.traversal.append_rle(entry);
    }
    op
}

#[cfg(test)]
mod test {
    use crate::list::ListCRDT;
    use rand::prelude::SmallRng;
    use rand::SeedableRng;
    use crate::fuzz_helpers::make_random_change;
    use crate::list::ot::positionmap::map_to_traversal;
    use super::TraversalComponent::*;

    #[test]
    fn simple_position_map() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there".into()); // 0-7
        doc.local_delete(0, 2, 3); // "hiere" 8-11

        doc.ot_changes_since(doc.linear_changes_since(0), |post_pos, pre_pos, e| {
            dbg!((post_pos, pre_pos, e));
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

        dbg!(&doc1.range_tree);
        dbg!(&doc1.deletes);
        dbg!(&doc1.double_deletes);

        let mut changes = Vec::new();
        let map = doc2.ot_changes_since(doc2.linear_changes_since(0), |post_pos, pre_pos, e| {
            dbg!((post_pos, pre_pos, e));
            // if e.tag == OpTag::Insert {pre_pos -= e.len;}
            changes.push((post_pos, pre_pos, e));
        });
        changes.reverse();
        dbg!(&changes);
        dbg!(&map);
    }

    fn ot_single_doc_fuzz(rng: &mut SmallRng) {
        let mut doc = ListCRDT::new();

        let agent = doc.get_or_create_agent_id("seph");

        for _i in 0..50 {
            make_random_change(&mut doc, None, agent, rng);
        }

        let midpoint_order = doc.get_next_order();
        let midpoint_content = doc.to_string();

        let mut ops = vec![];
        for _i in 0..50 {
            let op = make_random_change(&mut doc, None, agent, rng);
            ops.push(op);
        }
        // dbg!(ops);

        let mut ops2 = vec![];
        // let map = doc.ot_changes_since(doc.linear_changes_since(0), |post_pos, pre_pos, e, has_content| {
        let map = doc.ot_changes_since(doc.linear_changes_since(midpoint_order), |_post_pos, _pre_pos, e| {
            ops2.push(e);
            // let content = if e.tag == OpTag::Insert {
            //     if e.has_content {
            //         doc.text_content.as_ref().unwrap()
            //             .chars_at(post_pos as usize).take(e.len as usize)
            //             .collect::<SmartString>()
            //     } else {
            //         std::iter::repeat('X').take(e.len as usize).collect::<SmartString>()
            //     }
            // } else { SmartString::default() };
            //
            // let c = LocalOp {
            //     pos: pre_pos as usize,
            //     ins_content: content,
            //     del_span: if e.tag == OpTag::Delete { e.len as usize } else { 0 },
            // };
            // ops2.push(c);
        });

        // Ok we have a few things to check:
        // 1. The returned map shouldn't contain any inserts with unknown content
        for e in map.iter() {
            if let Ins { content_known, .. } = e {
                assert!(content_known);
            }
        }

        // 2. The returned map should be able to be converted to a traversal operation and applied
        //    to the midpoint, returning the current document state.
        let traversal = map_to_traversal(&map, doc.text_content.as_ref().unwrap());
        // dbg!(&traversal);
        let result = traversal.apply_to_string(midpoint_content.as_str());
        // dbg!(doc.text_content.unwrap(), result);
        assert_eq!(doc.text_content.unwrap(), result);


        // 3. We should also be able to apply all the changes one by one to the midpoint state and
        //    arrive at the same result.


        // ops2.reverse();
        // dbg!(ops2);
        //
        // assert_eq!(map.len().1 as usize, doc.len());

    }

    #[test]
    fn ot_single_document_fuzz() {
        // Check that when we query all the changes from a single document, the result is the same
        // (same values, same order) as we get from ot_changes_since.

        let mut rng = SmallRng::seed_from_u64(7);

        for _j in 0..100 {
            println!("{}", _j);
            ot_single_doc_fuzz(&mut rng);
        }
    }

    #[test]
    fn ot_single_doc_fuzz_once() {
        let mut rng = SmallRng::seed_from_u64(8);
        ot_single_doc_fuzz(&mut rng);
    }
}