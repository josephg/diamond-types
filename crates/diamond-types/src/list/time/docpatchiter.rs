use std::ops::Range;
use std::pin::Pin;

use content_tree::{SplitableSpan, Toggleable};

use crate::list::{DoubleDeleteList, ListCRDT, Order};
use crate::list::ot::positional::{InsDelTag, PositionalComponent};
use crate::list::time::patchiter::{ListPatchItem, ListPatchIter};
use crate::list::time::txn_trace::OptimizedTxnsIter;
use crate::rangeextra::OrderRange;
use crate::list::time::positionmap::{PositionMap, PositionRun};
use crate::list::time::positionmap::MapTag::*;


// #[derive(Debug)]
// pub(crate) struct OrderToRawInsertMap<'a>(Vec<(&'a RangeTreeLeaf, u32)>);
//
// impl<'a> OrderToRawInsertMap<'a> {
//     fn ord_refs(a: &RangeTreeLeaf, b: &RangeTreeLeaf) -> Ordering {
//         let a_ptr = a as *const _;
//         let b_ptr = b as *const _;
//
//         if a_ptr == b_ptr { Ordering::Equal }
//         else if a_ptr < b_ptr { Ordering::Less }
//         else { Ordering::Greater }
//     }
//
//     fn new(range_tree: &'a RangeTree) -> (Self, u32) {
//         let mut nodes = Vec::new();
//         let mut insert_position = 0;
//
//         for node in range_tree.node_iter() {
//             nodes.push((node, insert_position));
//             let len_here: u32 = node.as_slice().iter().map(|e| e.order_len()).sum();
//             insert_position += len_here;
//         }
//
//         nodes.sort_unstable_by(|a, b| {
//             Self::ord_refs(a.0, b.0)
//         });
//
//         // dbg!(nodes.iter().map(|n| n.0 as *const _).collect::<Vec<_>>());
//
//         (Self(nodes), insert_position)
//     }
//
//     /// Returns the raw insert position (as if no deletes ever happened) of the requested item. The
//     /// returned range always starts with the requested order and the end is the maximum range.
//     fn order_to_raw(&self, doc: &ListCRDT, ins_order: Order) -> (InsDelTag, Range<Order>) {
//         let marker = doc.marker_at(ins_order);
//
//         let leaf = unsafe { marker.as_ref() };
//         if cfg!(debug_assertions) {
//             // The requested item must be in the returned leaf.
//             leaf.find(ins_order).unwrap();
//         }
//
//         // TODO: Check if this is actually more efficient compared to a linear scan.
//         let idx = self.0.binary_search_by(|elem| {
//             Self::ord_refs(elem.0, leaf)
//         }).unwrap();
//
//         let mut start_position = self.0[idx].1;
//         for e in leaf.as_slice() {
//             if let Some(offset) = e.contains(ins_order) {
//                 let tag = if e.is_activated() { InsDelTag::Ins } else { InsDelTag::Del };
//                 return (tag, (start_position + offset as u32)..(start_position + e.order_len()));
//             } else {
//                 start_position += e.order_len();
//             }
//         }
//
//         unreachable!("Marker tree is invalid");
//     }
//
//     // /// Same as raw_insert_order, but constrain the return value based on the length
//     // fn raw_insert_order_limited(&self, doc: &ListCRDT, order: Order, max_len: Order) -> Range<Order> {
//     //     let mut result = self.raw_insert_order(list, order);
//     //     result.end = result.end.min(result.start + max_len);
//     //     result
//     // }
// }

impl ListCRDT {
    pub fn iter_original_patches(&self) -> OrigPatchesIter {
        OrigPatchesIter::new(self)
    }
}


/// An iterator over original insert positions - which tells us *where* each insert and delete
/// happened in the document, at the time when that edit happened. This code would all be much
/// cleaner and simpler using coroutines.
#[derive(Debug)]
pub struct OrigPatchesIter<'a> {
    txn_iter: OptimizedTxnsIter<'a>,

    /// Helpers to map from Order -> raw positions -> position at the current point in time
    map: Pin<Box<PositionMap>>,
    // order_to_raw_map: OrderToRawInsertMap<'a>,

    // There's two ways we could handle double deletes:
    // 1. Use a double delete list. Have the map simply store whether or not an item was deleted
    // at all, and if something is deleted multiple times, mark as such in double_deletes.
    // 2. Make store the number of times each item has been deleted. This would be better if
    // double deletes were common, but they're vanishingly rare in practice.
    double_deletes: DoubleDeleteList,

    // TODO: Consider / try to lower this to a tighter reference.
    list: &'a ListCRDT,
    /// Inside a txn we iterate over each rle patch with this.
    current_inner: Option<ListPatchIter<'a, true>>,

    current_item: ListPatchItem,
    // current_op_type: InsDelTag,
    // current_range: Range<Order>,
    // current_target_offset: Order,
}

// impl<'a> Drop for OrigPatchesIter<'a> {
//     fn drop(&mut self) {
//         println!("Map entries {} nodes {:?}", self.map.count_entries(), self.map.count_nodes());
//         // dbg!(&self.map);
//     }
// }

impl<'a> OrigPatchesIter<'a> {
    fn new(list: &'a ListCRDT) -> Self {
        let mut map = PositionMap::new();

        let total_post_len = list.range_tree.offset_len();
        // let (order_to_raw_map, total_post_len) = OrderToRawInsertMap::new(&list.range_tree);
        // TODO: This is something we should cache somewhere.
        map.push(PositionRun::new_void(total_post_len));

        Self {
            txn_iter: list.txns.txn_spanning_tree_iter(),
            map,
            double_deletes: DoubleDeleteList::new(),
            list,
            current_inner: None,
            // current_op_type: Default::default(),
            // current_target_offset: 0,
            current_item: Default::default()
        }
    }

    fn next_inner(&mut self) -> Option<ListPatchItem> {
        if let Some(current_inner) = &mut self.current_inner {
            if let Some(op_item) = current_inner.next() {
                return Some(op_item)
            }
        }

        // current_inner is either empty or None. Iterate to the next txn.
        let walk = self.txn_iter.next()?;

        for range in walk.retreat {
            for op in self.list.patch_iter_in_range(range) {
                let mut target = op.target_range();
                // dbg!(&op, &target);
                while !target.is_empty() {
                    let len = self.retreat_by_range(target.clone(), op.op_type);
                    target.start += len;
                }
            }
        }

        for range in walk.advance_rev.into_iter().rev() {
            for op in self.list.patch_iter_in_range_rev(range) {
                let mut target = op.target_range();
                while !target.is_empty() {
                    let len = self.advance_by_range(target.clone(), op.op_type, true).1;
                    target.start += len;
                }
            }
        }

        // self.consuming = walk.consume;
        debug_assert!(!walk.consume.is_empty());
        let mut inner = self.list.patch_iter_in_range(walk.consume);
        let next = inner.next();
        debug_assert!(next.is_some()); // The walk cannot be empty.

        self.current_inner = Some(inner);
        return next;
    }

    fn order_to_raw(&self, order: Order) -> (InsDelTag, Range<Order>) {
        let cursor = self.list.get_cursor_before(order);
        let base = cursor.count_offset_pos() as Order;

        let e = cursor.get_raw_entry();
        let tag = if e.is_activated() { InsDelTag::Ins } else { InsDelTag::Del };
        (tag, base..(base + e.order_len() - cursor.offset as Order))
    }

    fn retreat_by_range(&mut self, target: Range<Order>, op_type: InsDelTag) -> Order {
        // dbg!(&target, self.map.iter().collect::<Vec<_>>());
        // This variant is only actually used in one place - which makes things easier.

        let (final_tag, raw_range) = self.order_to_raw(target.start);
        let raw_start = raw_range.start;
        let mut len = Order::min(raw_range.order_len(), target.order_len());

        let mut cursor = self.map.mut_cursor_at_offset_pos(raw_start as usize, false);
        if op_type == InsDelTag::Del {
            let e = cursor.get_raw_entry();
            len = len.min((e.final_len - cursor.offset) as u32);
            debug_assert!(len > 0);

            // Usually there's no double-deletes, but we need to check just in case.
            let allowed_len = self.double_deletes.find_zero_range(raw_start, len);
            if allowed_len == 0 { // Unlikely. There's a double delete here.
                let len_dd_here = self.double_deletes.decrement_delete_range(raw_start, len);
                debug_assert!(len_dd_here > 0);

                // What a minefield. O_o
                return len_dd_here;
            } else {
                len = allowed_len;
            }
        }

        debug_assert!(len >= 1);
        // So the challenge here is we need to un-merge upstream position runs into their
        // constituent parts. We can't use replace_range for this because that calls truncate().
        // let mut len_remaining = len;
        // while len_remaining > 0 {
        //
        // }
        if op_type == InsDelTag::Ins && final_tag == InsDelTag::Del {
            // The easy case. The entry in PositionRun will be Inserted.
            debug_assert_eq!(cursor.get_raw_entry().tag, Inserted);
            cursor.replace_range(PositionRun::new_void(len as _));
        } else {
            // We have merged everything into Upstream. We need to pull it apart, which is bleh.
            debug_assert_eq!(cursor.get_raw_entry().tag, Upstream);
            debug_assert_eq!(op_type, final_tag); // Ins/Ins or Del/Del.
            // TODO: Is this a safe assumption? Let the fuzzer verify it.
            assert!(cursor.get_raw_entry().len() - cursor.offset >= len as usize);

            let (new_entry, eat_content) = match op_type {
                InsDelTag::Ins => (PositionRun::new_void(len as _), len as usize),
                InsDelTag::Del => (PositionRun::new_ins(len as _), 0),
            };

            let current_entry = cursor.get_raw_entry();

            // So we want to replace the cursor entry with [start, X, end]. The trick is figuring
            // out where we split the content in the current entry.
            if cursor.offset == 0 {
                // dbg!(&new_entry, current_entry);
                // Cursor is at the start of this entry. This variant is easier.
                let remainder = PositionRun::new_upstream(
                    current_entry.final_len - new_entry.final_len,
                    current_entry.content_len - eat_content
                );
                // dbg!(remainder);
                if remainder.final_len > 0 {
                    cursor.replace_entry(&[new_entry, remainder]);
                } else {
                    cursor.replace_entry(&[new_entry]);
                }
            } else {
                // TODO: Accidentally this whole thing. Clean me up buttercup!

                // The cursor isn't at the start. We need to figure out how much to slice off.
                // Basically, we need to know how much content is in cursor.offset.

                // TODO(opt): A cursor comparator function would make this much more performant.
                let entry_start_offset = raw_start as usize - cursor.offset;
                let start_cursor = self.list.range_tree.cursor_at_offset_pos(entry_start_offset, true);
                let start_content = start_cursor.count_content_pos();

                // TODO: Reuse the cursor from order_to_raw().
                let midpoint_cursor = self.list.range_tree.cursor_at_offset_pos(raw_start as _, true);
                let midpoint_content = midpoint_cursor.count_content_pos();

                let content_chomp = midpoint_content - start_content;

                let start = PositionRun::new_upstream(cursor.offset, content_chomp);

                let remainder = PositionRun::new_upstream(
                    current_entry.final_len - new_entry.final_len - cursor.offset,
                    current_entry.content_len - eat_content - content_chomp
                );

                if remainder.final_len > 0 {
                    cursor.replace_entry(&[start, new_entry, remainder]);
                } else {
                    cursor.replace_entry(&[start, new_entry]);
                }
            }
        }
        // dbg!(self.map.iter().collect::<Vec<_>>());

        // let reversed_map_component = match op_type {
        //     InsDelTag::Ins => NotInsertedYet,
        //     InsDelTag::Del => Inserted,
        // };
        // cursor.replace_range(PositionRun::new(reversed_map_component, len as _));
        len
    }

    fn advance_by_range(&mut self, target: Range<Order>, op_type: InsDelTag, handle_dd: bool) -> (Option<PositionalComponent>, Order) {
        // We know the order of the range of the items which have been inserted.
        // Walk through them. For each, find out the global insert position, then
        // replace in map.

        let (final_tag, raw_range) = self.order_to_raw(target.start);
        let raw_start = raw_range.start;
        let mut len = Order::min(raw_range.order_len(), target.order_len());

        let mut cursor = self.map.mut_cursor_at_offset_pos(raw_start as usize, false);

        if op_type == InsDelTag::Del {
            // So the item will usually be in the Inserted state. If its in the Deleted
            // state, we need to mark it as double-deleted.
            let e = cursor.get_raw_entry();

            if handle_dd {
                // Handling double-deletes is only an issue while consuming. Never advancing.
                len = len.min((e.final_len - cursor.offset) as u32);
                debug_assert!(len > 0);
                if e.tag == Upstream { // This can never happen while consuming. Only while advancing.
                    self.double_deletes.increment_delete_range(raw_start, len);
                    return (None, len);
                }
            } else {
                // When the insert was created, the content must exist in the document.
                // TODO: Actually verify this assumption when integrating remote txns.
                debug_assert_eq!(e.tag, Inserted);
            }
        }

        let content_pos = cursor.count_content_pos() as u32;
        // Life could be so simple...
        // cursor.replace_range(PositionRun::new(op_type.into(), len as _));

        // So there's kinda 3 different states
        if final_tag == op_type {
            // Transition into the Upstream state
            let content_len: usize = if op_type == InsDelTag::Del { 0 } else { len as usize };
            cursor.replace_range(PositionRun::new_upstream(len as _, content_len));
            // Calling compress_node (in just this branch) improves performance by about 1%.
            cursor.inner.compress_node();
        } else {
            debug_assert_eq!(op_type, InsDelTag::Ins);
            debug_assert_eq!(final_tag, InsDelTag::Del);
            cursor.replace_range(PositionRun::new_ins(len as _));
        }

        // println!("{} {} {}", self.map.count_entries(), self.map.count_nodes().1, self.map.iter().count());
        // dbg!(("after advance", self.map.iter().collect::<Vec<_>>()));

        (Some(PositionalComponent {
            pos: content_pos,
            len,
            content_known: false,
            tag: op_type.into(),
        }), len)
    }
}

impl<'a> Iterator for OrigPatchesIter<'a> {
    type Item = (Range<Order>, PositionalComponent);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_item.range.is_empty() {
            self.current_item = self.next_inner()?;
            // self.current_op_type = item.op_type;
            // self.current_target_offset = item.target_offset();
            // debug_assert!(!self.current_target_offset.is_empty());
            debug_assert!(!self.current_item.range.is_empty());
        }

        let (result, len) = self.advance_by_range(self.current_item.target_range(), self.current_item.op_type, false);
        // self.current_item.range.start += len;
        let consumed_range = self.current_item.range.truncate_keeping_right(len as _);
        self.current_item.del_target += len; // TODO: Could be avoided by storing the offset...
        // debug_assert!(result.is_some());

        debug_assert!(len > 0);
        Some((consumed_range, result.unwrap()))
    }
}

#[cfg(test)]
mod test {
    use crate::list::ListCRDT;
    use smallvec::smallvec;
    use crate::list::external_txn::{RemoteCRDTOp, RemoteId, RemoteTxn};

    #[test]
    fn foo() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there");
        doc.local_delete(0, 2, 3);

        // for _i in 0..10 {
        //     doc.local_insert(0, 0, "xy");
        // }

        dbg!(doc.patch_iter().collect::<Vec<_>>());
        dbg!(doc.iter_original_patches().collect::<Vec<_>>());
    }

    #[test]
    fn foo2() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "xxx");

        // Ok now two users concurrently delete.
        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "a".into(), seq: 0 },
            parents: smallvec![RemoteId { agent: "seph".into(), seq: 2 }],
            ops: smallvec![RemoteCRDTOp::Del {
                id: RemoteId { agent: "seph".into(), seq: 0 },
                len: 3
            }],
            ins_content: "".into(),
        });

        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "b".into(), seq: 0 },
            parents: smallvec![RemoteId { agent: "seph".into(), seq: 2 }],
            ops: smallvec![RemoteCRDTOp::Del {
                id: RemoteId { agent: "seph".into(), seq: 0 },
                len: 3
            }],
            ins_content: "".into(),
        });

        dbg!(doc.patch_iter().collect::<Vec<_>>());

        // dbg!(doc.iter_original_patches().collect::<Vec<_>>());
        let mut iter = doc.iter_original_patches();
        while let Some(item) = iter.next() {
            dbg!(item);
        }
        iter.map.check();
        dbg!(&iter.map);
    }

    #[test]
    fn forwards_backwards() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "xx");
        doc.local_insert(0, 1, "XX");

        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "a".into(), seq: 0 },
            parents: smallvec![RemoteId { agent: "seph".into(), seq: 2 }],
            ops: smallvec![RemoteCRDTOp::Del {
                id: RemoteId { agent: "seph".into(), seq: 1 },
                len: 1
            }],
            ins_content: "".into(),
        });
        doc.check(true);

        dbg!(doc.patch_iter().collect::<Vec<_>>());

        // dbg!(doc.iter_original_patches().collect::<Vec<_>>());
        let mut iter = doc.iter_original_patches();
        while let Some(item) = iter.next() {
            dbg!(item);
        }
        iter.map.check();
        // dbg!(&iter.map);
    }
}