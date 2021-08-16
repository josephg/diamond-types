use crate::list::{Order, ListCRDT, DoubleDeleteList};
use crate::range_tree::*;
use crate::splitable_span::SplitableSpan;
use OpTag::*;
use crate::order::OrderSpan;
use std::pin::Pin;
use std::ops::{AddAssign, SubAssign};
use crate::list::double_delete::DoubleDelete;
use crate::rle::{KVPair, RleKey, RleSpanHelpers};

// Length of the item before and after the operation sequence has been applied.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub(super) enum OpTag {
    Retain,
    Insert,
    Delete,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub(super) struct PositionMapEntry {
    tag: OpTag,
    len: Order,
}

impl PositionMapEntry {
    fn pre_len(&self) -> Order {
        match self.tag {
            Retain | Delete => self.len,
            Insert => 0,
        }
    }

    fn post_len(&self) -> Order {
        match self.tag {
            Retain | Insert => self.len,
            Delete => 0,
        }
    }
}

impl Default for PositionMapEntry {
    fn default() -> Self {
        Self {
            tag: OpTag::Retain,
            len: Order::MAX
        }
    }
}

impl SplitableSpan for PositionMapEntry {
    fn len(&self) -> usize {
        // self.post_len() as _
        self.len as usize
    }

    fn truncate(&mut self, at_post_len: usize) -> Self {
        let remainder = self.len - at_post_len as Order;
        self.len = at_post_len as u32;
        Self {
            tag: self.tag,
            len: remainder
        }
    }

    fn can_append(&self, other: &Self) -> bool { self.tag == other.tag }
    fn append(&mut self, other: Self) { self.len += other.len; }
    fn prepend(&mut self, other: Self) { self.len += other.len; }
}

// impl EntryWithContent for PositionMapEntry {
//     fn content_len(&self) -> usize {
//         self.pre_len() as _
//     }
// }

impl EntryTraits for PositionMapEntry {
    type Item = (); // TODO: Remove this.

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        // The struct is symmetric. There's no difference truncating left or right.
        self.truncate(at)
    }

    fn contains(&self, _loc: Self::Item) -> Option<usize> {
        unimplemented!()
    }

    fn is_valid(&self) -> bool {
        self.len != Order::MAX
    }

    fn at_offset(&self, _offset: usize) -> Self::Item {
        unimplemented!()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) struct PrePostIndex;

// Not sure why tuples of integers don't have AddAssign and SubAssign.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct Pair<V: Copy + Clone + Default + AddAssign + SubAssign + PartialEq + Eq>(V, V);

impl<V: Copy + Clone + Default + AddAssign + SubAssign + PartialEq + Eq> AddAssign for Pair<V> {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
        self.1 += rhs.1;
    }
}
impl<V: Copy + Clone + Default + AddAssign + SubAssign + PartialEq + Eq> SubAssign for Pair<V> {
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
        self.1 -= rhs.1;
    }
}

impl TreeIndex<PositionMapEntry> for PrePostIndex {
    type IndexUpdate = Pair<i32>;
    type IndexOffset = Pair<u32>;

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &PositionMapEntry) {
        marker.0 += entry.pre_len() as i32;
        marker.1 += entry.post_len() as i32;
    }

    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &PositionMapEntry) {
        marker.0 -= entry.pre_len() as i32;
        marker.1 -= entry.post_len() as i32;
    }

    fn update_offset_by_marker(offset: &mut Self::IndexOffset, by: &Self::IndexUpdate) {
        offset.0 = offset.0.wrapping_add(by.0 as u32);
        offset.1 = offset.1.wrapping_add(by.1 as u32);
    }

    fn increment_offset(offset: &mut Self::IndexOffset, by: &PositionMapEntry) {
        offset.0 += by.pre_len();
        offset.1 += by.post_len();
    }
}

pub(super) type PositionMap = Pin<Box<RangeTree<PositionMapEntry, PrePostIndex>>>;

impl RangeTree<PositionMapEntry, PrePostIndex> {
    // pub fn content_len(&self) -> usize {
    //     self.count as usize
    // }

    pub fn cursor_at_post(&self, pos: usize, stick_end: bool) -> Cursor<PositionMapEntry, PrePostIndex> {
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
    pub(super) fn make_position_map<V>(&self, mut span: OrderSpan, mut visit: V) -> PositionMap
    where V: FnMut(u32, u32, PositionMapEntry, bool) {
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
        map.insert_at_start(PositionMapEntry {
            tag: OpTag::Retain,
            len: self.range_tree.content_len() as _,
            // len: u32::MAX / 2,
        }, null_notify);

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
                let first_del_target = u32::max(entry.order, last_del_target - del_span_size + 1);

                let (allowed, first_order) = marked_deletes.mark_range(&self.double_deletes, last_del_target, first_del_target);
                let len_here = last_del_target - first_order + 1;

                if allowed {
                    // let len_here = len_here.min((-entry.len) as u32 - rt_cursor.offset as u32);
                    let post_pos = rt_cursor.count_pos();
                    let map_cursor = map.cursor_at_post(post_pos as _, true);
                    // We call insert instead of replace_range here because the delete doesn't
                    // consume "space".
                    let entry = PositionMapEntry {
                        tag: OpTag::Delete,
                        len: len_here
                    };
                    map.insert(map_cursor, entry, null_notify);

                    // The content might have later been deleted.
                    visit(post_pos, map_cursor.count_pos().0, entry, false);
                }

                span.len -= len_here;
            } else {
                // The operation was an insert operation, not a delete operation.
                let mut rt_cursor = self.get_cursor_before(span_last_order);
                rt_cursor.offset += 1; // Dirty. Essentially get_cursor_after(span_last_order) without rolling over.

                // Check how much we can tag in one go.
                let len_here = u32::min(span.len, rt_cursor.offset as _); // usize? u32? blehh
                debug_assert_ne!(len_here, 0);
                // let base = span_last_order + 1 - len_here; // not needed.
                // let base = u32::max(span.order, span_last_order + 1 - cursor.offset);
                // dbg!(&cursor, len_here);
                rt_cursor.offset -= len_here as usize;

                // Where in the final document are we?
                let post_pos = rt_cursor.count_pos();

                let entry = PositionMapEntry {
                    tag: OpTag::Insert,
                    len: len_here
                };

                // So this is also dirty. We need to skip any deletes, which have a size of 0.
                let has_content = rt_cursor.get_raw_entry().is_activated();
                // dbg!((&entry, has_content));

                // There's two cases here. Either we're inserting something fresh, or we're
                // cancelling out a delete we found earlier.
                let pre_pos = if has_content {
                    // post_pos + 1 is a hack. cursor_at_offset_pos returns the first cursor
                    // location which has the right position.
                    let mut map_cursor = map.cursor_at_post(post_pos as usize + 1, true);
                    map_cursor.offset -= 1;
                    // dbg!(&map_cursor);
                    map.replace_range(map_cursor, entry, null_notify);
                    map_cursor.count_pos().0
                } else {
                    let mut map_cursor = map.cursor_at_post(post_pos as usize, true);
                    map_cursor.roll_to_next_entry();
                    // dbg!(&map_cursor, len_here);
                    assert_eq!(map_cursor.get_raw_entry().tag, Delete);
                    map.delete(&mut map_cursor, len_here as usize, null_notify);
                    map_cursor.count_pos().0
                };

                // The content might have later been deleted.
                visit(post_pos, pre_pos, entry, has_content);

                span.len -= len_here;
            }
        }

        map
    }
}

#[cfg(test)]
mod test {
    use crate::list::ListCRDT;

    #[test]
    fn simple_position_map() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there".into()); // 0-7
        doc.local_delete(0, 2, 3); // "hiere" 8-11

        doc.make_position_map(doc.linear_changes_since(0), |post_pos, pre_pos, e, has_content| {
            dbg!((post_pos, pre_pos, e, has_content));
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
        let map = doc2.make_position_map(doc2.linear_changes_since(0), |post_pos, pre_pos, e, has_content| {
            dbg!((post_pos, pre_pos, e, has_content));
            // if e.tag == OpTag::Insert {pre_pos -= e.len;}
            changes.push((post_pos, pre_pos, e, has_content));
        });
        changes.reverse();
        dbg!(&changes);
        dbg!(&map);
    }
}