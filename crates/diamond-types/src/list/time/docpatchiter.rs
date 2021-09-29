use std::ops::Range;
use content_tree::SplitableSpan;

use crate::list::{ListCRDT, Order};
use crate::list::positional::PositionalComponent;
use crate::list::time::patchiter::{ListPatchItem, ListPatchIter};
use crate::list::time::txn_trace::OptimizedTxnsIter;
use crate::list::time::positionmap::PositionMap;

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
    map: PositionMap,

    // TODO: Consider / try to lower this to a tighter reference.
    list: &'a ListCRDT,
    /// Inside a txn we iterate over each rle patch with this.
    current_inner: Option<ListPatchIter<'a, true>>,

    current_item: ListPatchItem,
}

impl<'a> OrigPatchesIter<'a> {
    fn new(list: &'a ListCRDT) -> Self {
        Self {
            txn_iter: list.txns.txn_spanning_tree_iter(),
            map: PositionMap::new_void(list),
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
                self.map.retreat_all_by_range(self.list, op);
            }
        }

        for range in walk.advance_rev.into_iter().rev() {
            for op in self.list.patch_iter_in_range_rev(range) {
                self.map.advance_all_by_range(self.list, op);
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

        let (result, len) = self.map.advance_first_by_range(self.list, self.current_item.target_range(), self.current_item.op_type, false);
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