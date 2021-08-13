use crate::list::{Order, ListCRDT, DoubleDeleteList};
use crate::range_tree::*;
use crate::splitable_span::SplitableSpan;
use OpTag::*;
use crate::order::OrderSpan;
use std::pin::Pin;
use std::ops::{AddAssign, SubAssign};

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


impl ListCRDT {
    pub(super) fn make_position_map<V>(&self, mut span: OrderSpan, mut visit: V) -> PositionMap
    where V: FnMut(u32, u32, PositionMapEntry, bool) {
        // For now.
        assert_eq!(span.end(), self.get_next_order());

        let mut map: PositionMap = RangeTree::new();
        map.insert_at_start(PositionMapEntry {
            tag: OpTag::Retain,
            len: self.range_tree.content_len() as _,
            // len: u32::MAX / 2,
        }, null_notify);

        // So the way this works is if we find an item has been double-deleted, we'll mark in this
        // empty range until marked_deletes equals self.double_deletes.
        let mut marked_deletes = DoubleDeleteList::new();

        // Now we go back through history in reverse order. We need to go in reverse order because
        // of duplicate deletes. If an item has been deleted multiple times, we only want to visit
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

            if let Some((d, _d_offset)) = self.deletes.find(span_last_order) {
                // Its a delete. We need to try to undelete the item, unless the item was deleted
                // multiple times (in which case, it stays deleted.)
                let mut base = u32::max(span.order, d.0);
                let mut del_span_size = span_last_order + 1 - base;
                debug_assert!(del_span_size > 0);

                // d_offset -= span_last_order - base; // equivalent to d_offset -= undelete_here - 1;

                // Ok, undelete here. There's two approaches this implementation could take:
                // 1. Undelete backwards from base + len_here - 1, undeleting as much as we can.
                //    Rely on the outer loop to iterate to the next section
                // 2. Undelete len_here items. The order that we undelete an item in doesn't matter,
                //    so although this approach needs another inner loop, we can go through this
                //    range forwards. This makes the logic simpler, but longer.

                // I'm going with 2.
                span.len -= del_span_size; // equivalent to span.len = base - span.order;

                while del_span_size > 0 {
                    // dbg!((base, del_span_size));
                    let delete_target_base = d.1.order + base - d.0;
                    let mut len_here = self.double_deletes.find_zero_range(delete_target_base, del_span_size);
                    // dbg!((delete_target_base, del_span_size, len_here));

                    if len_here == 0 { // Unlikely.
                        // We're looking at an item which has been deleted multiple times.
                        // There's two cases here:
                        // 1. We have no equivalent entry in marked_deletes. Increment
                        // marked_deletes and continue. Or
                        // 2. We've marked this delete in marked_deletes. Proceed.

                        // Note this code is pretty inefficient. We're doing multiple binary
                        // searches in a row and not caching anything. But this case is really rare
                        // in practice, so ... eh, its probably fine.

                        let (dd, dd_offset) = self.double_deletes.find(delete_target_base).unwrap();
                        let dd_range = u32::min(dd.1.len - dd_offset, del_span_size);

                        let (del_here, len_dd_here) = if let Some((entry, entry_offset)) = marked_deletes.find(delete_target_base) {
                            let local_len = entry.1.len - entry_offset;
                            let mark_range = u32::min(local_len, dd_range);
                            debug_assert!(mark_range > 0);

                            if entry.1.excess_deletes == dd.1.excess_deletes {
                                (true, mark_range)
                            } else {
                                (false, mark_range)
                            }
                        } else {
                            (false, dd_range)
                        };

                        // dbg!(del_here);

                        // What a minefield. O_o
                        if !del_here {
                            let len = marked_deletes.increment_delete_range_to(delete_target_base, len_dd_here, dd.1.excess_deletes);
                            debug_assert!(len > 0);
                            del_span_size -= len;
                            base += len;

                            // dbg!(&marked_deletes);
                            continue;
                        }

                        // len_here = self.double_deletes.find_zero_range(base, undelete_here);
                        len_here = len_dd_here;
                        debug_assert!(len_here > 0);
                    }

                    // Ok now undelete from the range tree.
                    // let base_item = d.1.order + d_offset + 1 - del_span_size;
                    // d.1.order + base + d_offset - span_last_order

                    // dbg!(base_item, d.1.order, d_offset, undelete_here, base);

                    let rt_cursor = self.get_cursor_before(delete_target_base);
                    // Cap the number of items to undelete each iteration based on the span in range_tree.
                    let entry = rt_cursor.get_raw_entry();
                    len_here = len_here.min((-entry.len) as u32 - rt_cursor.offset as u32);

                    let post_pos = rt_cursor.count_pos();
                    let entry = PositionMapEntry {
                        tag: OpTag::Delete,
                        len: len_here
                    };
                    let map_cursor = map.cursor_at_post(post_pos as _, true);
                    // We call insert instead of replace_range here because the delete doesn't
                    // consume "space".
                    map.insert(map_cursor, entry, null_notify);

                    // The content might have later been deleted.
                    visit(post_pos, map_cursor.count_pos().0, entry, false);

                    // let (len_here, succeeded) = self.range_tree.remote_reactivate(cursor, len_here as _, notify_for(&mut self.index));
                    // assert!(succeeded); // If they're active in the range_tree, we're in trouble.
                    del_span_size -= len_here as u32;
                    base += len_here;
                }
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

        // "hi there" -> "hiere" -> "hie"

        dbg!(&doc1.range_tree);
        dbg!(&doc1.deletes);
        dbg!(&doc1.double_deletes);

        let mut changes = Vec::new();
        let map = doc1.make_position_map(doc1.linear_changes_since(0), |post_pos, pre_pos, e, has_content| {
            // dbg!((post_pos, pre_pos, e, has_content));
            // if e.tag == OpTag::Insert {pre_pos -= e.len;}
            changes.push((post_pos, pre_pos, e, has_content));
        });
        changes.reverse();
        dbg!(&changes);
        dbg!(&map);
    }
}