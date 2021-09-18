
// There's 3 states a position component can be in:
// - Not inserted (yet), with a postlen
// - Inserted (and in the document)
// - Inserted then deleted

use content_tree::{ContentLength, ContentTreeWithIndex, FullIndex};
use rle::{SplitableSpan, Searchable};
use std::cmp::Ordering;

use crate::list::{ListCRDT, RangeTree, RangeTreeLeaf, Order, DoubleDeleteList};
use crate::list::time::patchiter::{OpContent, ListPatchIter, OpItem};
use crate::list::time::positionmap::PositionMapComponent::*;
use std::ops::Range;
use crate::rangeextra::OrderRange;
use std::pin::Pin;
use crate::list::time::txn_trace::OriginTxnIter;
use crate::list::ot::positional::{PositionalComponent, InsDelTag};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum PositionMapComponent {
    NotInsertedYet,
    Inserted,
    Deleted,
}

// It would be nicer to just use RleRun but I want to customize
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
struct PositionRun {
    val: PositionMapComponent,
    len: usize // This is the full length that we take up in the final document
}

impl Default for PositionMapComponent {
    fn default() -> Self { NotInsertedYet }
}

impl From<PositionMapComponent> for InsDelTag {
    fn from(c: PositionMapComponent) -> Self {
        match c {
            NotInsertedYet => panic!("Invalid component for conversion"),
            Inserted => InsDelTag::Ins,
            Deleted => InsDelTag::Del,
        }
    }
}

impl PositionRun {
    fn new(val: PositionMapComponent, len: usize) -> Self {
        Self { val, len }
    }
}

impl SplitableSpan for PositionRun {
    fn len(&self) -> usize { self.len }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder = self.len - at;
        self.len = at;
        Self { val: self.val, len: remainder }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.val == other.val
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }
}

impl ContentLength for PositionRun {
    fn content_len(&self) -> usize {
        // This is the amount of space we take up right now.
        if self.val == Inserted { self.len } else { 0 }
    }
}

type PositionMap = ContentTreeWithIndex<PositionRun, FullIndex>;

#[derive(Debug)]
struct OrderToRawInsertMap<'a>(Vec<(&'a RangeTreeLeaf, u32)>);

impl<'a> OrderToRawInsertMap<'a> {
    fn ord_refs(a: &RangeTreeLeaf, b: &RangeTreeLeaf) -> Ordering {
        let a_ptr = a as *const _;
        let b_ptr = b as *const _;

        if a_ptr == b_ptr { Ordering::Equal }
        else if a_ptr < b_ptr { Ordering::Less }
        else { Ordering::Greater }
    }

    fn new(range_tree: &'a RangeTree) -> (Self, u32) {
        let mut nodes = Vec::new();
        let mut insert_position = 0;

        for node in range_tree.node_iter() {
            nodes.push((node, insert_position));
            let len_here: u32 = node.as_slice().iter().map(|e| e.order_len()).sum();
            insert_position += len_here;
        }

        nodes.sort_unstable_by(|a, b| {
            Self::ord_refs(a.0, b.0)
        });

        dbg!(nodes.iter().map(|n| n.0 as *const _).collect::<Vec<_>>());

        (Self(nodes), insert_position)
    }

    /// Returns the raw insert order (as if no deletes ever happened) of the passed range. The
    /// returned range always starts with the requested order and has its size capped by the insert
    /// run in the document.
    fn raw_insert_order(&self, doc: &ListCRDT, order: Order) -> Range<Order> {
        let marker = doc.marker_at(order);
        unsafe { marker.as_ref() }.find(order).unwrap();
        let leaf = unsafe { marker.as_ref() };
        let idx = self.0.binary_search_by(|elem| {
            Self::ord_refs(elem.0, leaf)
        }).unwrap();

        let mut start_position = self.0[idx].1;
        for e in leaf.as_slice() {
            if let Some(offset) = e.contains(order) {
                return (start_position + offset as u32)..(start_position + e.order_len());
            } else {
                start_position += e.order_len();
            }
        }

        unreachable!("Marker tree is invalid");
    }
}

/// An iterator over original insert positions - which tells us *where* each insert and delete
/// happened in the document, at the time when that edit happened. This code would all be much
/// cleaner and simpler using coroutines.
#[derive(Debug)]
struct OrigPositionIter<'a> {
    txn_iter: OriginTxnIter<'a>,
    map: Pin<Box<PositionMap>>,
    order_to_raw_map: OrderToRawInsertMap<'a>,

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

    current_component: PositionMapComponent,
    current_target: Range<Order>,
}

impl<'a> OrigPositionIter<'a> {
    fn new(list: &'a ListCRDT) -> Self {
        let mut map = PositionMap::new();

        let (order_to_raw_map, total_post_len) = OrderToRawInsertMap::new(&list.range_tree);
        // TODO: This is something we should cache somewhere.
        map.push(PositionRun::new(NotInsertedYet, total_post_len as usize));

        Self {
            txn_iter: list.txns.txn_spanning_tree_iter(),
            map,
            order_to_raw_map,
            double_deletes: DoubleDeleteList::new(),
            list,
            current_inner: None,
            current_component: PositionMapComponent::NotInsertedYet,
            current_target: Default::default()
        }
    }

    fn next_inner(&mut self) -> Option<OpItem> {
        if let Some(current_inner) = &mut self.current_inner {
            if let Some(op_item) = current_inner.next() {
                return Some(op_item)
            }
        }

        // current_inner is either empty or None. Iterate to the next txn.
        let walk = self.txn_iter.next()?;

        for _range in walk.retreat {
            unimplemented!();
        }

        for _range in walk.advance_rev {
            unimplemented!();
        }

        // self.consuming = walk.consume;
        debug_assert!(!walk.consume.is_empty());
        let mut inner = self.list.patch_iter_in_range(walk.consume);
        let next = inner.next();
        debug_assert!(next.is_some()); // The walk cannot be empty.

        self.current_inner = Some(inner);
        return next;
    }
}

impl<'a> Iterator for OrigPositionIter<'a> {
    type Item = PositionalComponent;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_target.is_empty() {
            let OpItem { range, content } = self.next_inner()?;

            match content {
                OpContent::Del(target) => {
                    self.current_target = target..target + range.order_len();
                    self.current_component = Deleted;
                }
                OpContent::Ins => {
                    self.current_target = range;
                    self.current_component = Inserted;
                }
            };

            debug_assert!(!self.current_target.is_empty());
        }

        // We know the order of the range of the items which have been inserted.
        // Walk through them. For each, find out the global insert position, then
        // replace in map.
        dbg!(&self.current_target, self.current_component);

        let raw_range = self.order_to_raw_map.raw_insert_order(self.list, self.current_target.start);
        // raw_range.end = Order::max(raw_range.end, raw_range.start + self.current_target.order_len());
        let len = raw_range.order_len();
        let raw_start = raw_range.start;

        let mut cursor = self.map.mut_cursor_at_offset_pos(raw_start as usize, false);
        if self.current_component == Deleted {
            // So the item will usually be in the Inserted state. If its in the Deleted
            // state, we need to mark it as double-deleted.
            let e = cursor.get_raw_entry();
            debug_assert_eq!(e.val, Inserted);
            // if e.val == Deleted { // Actually this can never happen while consuming. Only while advancing.
            //     len = len.max((e.len - cursor.offset) as u32);
            //     double_deletes.increment_delete_range(raw_start, len);
            //     self.current_target.start += len;
            //     continue;
            // }
        }

        let content_pos = cursor.count_content_pos() as u32;
        cursor.replace_range(PositionRun::new(self.current_component, len as _));

        self.current_target.start += len;

        Some(PositionalComponent {
            pos: content_pos,
            len,
            content_known: false,
            tag: self.current_component.into(),
        })
        // println!("consume {:?} at {:?}", self.current_component, content_pos..content_pos+len as usize);
    }
}

impl ListCRDT {
    pub fn foo(&self) {
        for patch in OrigPositionIter::new(self) {
            dbg!(patch);
        }
    }
}

#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;

    use super::*;

    #[test]
    fn positionrun_is_splitablespan() {
        test_splitable_methods_valid(PositionRun::new(NotInsertedYet, 5));
        test_splitable_methods_valid(PositionRun::new(Inserted, 5));
        test_splitable_methods_valid(PositionRun::new(Deleted, 5));

        // assert!(PositionRun::new(Deleted(1), 1)
        //     .can_append(&PositionRun::new(Deleted(1), 2)));
        // assert!(!PositionRun::new(Deleted(1), 1)
        //     .can_append(&PositionRun::new(Deleted(999), 2)));
    }

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

        doc.foo();
    }
}