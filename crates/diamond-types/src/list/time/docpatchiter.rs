use std::ops::Range;
use smallvec::SmallVec;
use smartstring::alias::{String as SmartString};
use rle::AppendRle;

use crate::list::{ListCRDT, Order};
use crate::list::positional::PositionalComponent;
use crate::list::time::patchiter::{ListPatchItem, ListPatchIter};
use crate::list::time::txn_trace::OptimizedTxnsIter;
use crate::list::time::positionmap::PositionMap;

/// This is similar to PositionalOp, but where positional ops can be applied in sequence, when
/// applying a walk like this the components need to be interpreted from the perspective of the
/// document as it is at the corresponding origin_order.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct PositionalOpWalk {
    pub components: SmallVec<[PositionalComponent; 1]>,
    pub origin_order: SmallVec<[Range<Order>; 2]>,
    pub content: SmartString,
}

impl PositionalOpWalk {
    fn new() -> Self {
        Self {
            components: Default::default(),
            origin_order: Default::default(),
            content: Default::default()
        }
    }
}

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
    current_item: ListPatchItem,
    current_inner: Option<ListPatchIter<'a, true> >, // extra space to work around intellij-rust bug
}

impl<'a> OrigPatchesIter<'a> {
    fn new(list: &'a ListCRDT) -> Self {
        Self {
            txn_iter: list.txns.txn_spanning_tree_iter(),
            map: PositionMap::new_void(list),
            list,
            current_item: Default::default(),
            current_inner: None,
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

        debug_assert!(!walk.consume.is_empty());
        let mut inner = self.list.patch_iter_in_range(walk.consume);
        let next = inner.next();
        debug_assert!(next.is_some()); // The walk cannot be empty.

        self.current_inner = Some(inner);
        return next;
    }

    fn fill_current_item(&mut self) -> Option<()> { // Option instead of bool so we can use try
        if self.current_item.range.is_empty() {
            if let Some(item) = self.next_inner() {
                debug_assert!(!item.is_empty());
                self.current_item = item;
            } else { return None; }
        }
        Some(())
    }

    pub(crate) fn next_patch_with_content(&mut self) -> Option<(Range<Order>, PositionalComponent, Option<SmartString>)> {
        self.fill_current_item()?;

        let consumed_start = self.current_item.range.start;
        let (c, str) = self.map.advance_and_consume_with_content(self.list, &mut self.current_item);
        Some((consumed_start .. consumed_start + c.len, c, str))
    }

    pub(crate) fn into_patch(mut self) -> PositionalOpWalk {
        let mut result = PositionalOpWalk::new();
        while let Some((range, component, str)) = self.next_patch_with_content() {
            result.origin_order.push_rle(range);
            result.components.push(component);
            if let Some(str) = str {
                result.content.push_str(&str);
            }
        }

        result
    }
}

impl<'a> Iterator for OrigPatchesIter<'a> {
    type Item = (Range<Order>, PositionalComponent);

    fn next(&mut self) -> Option<Self::Item> {
        self.fill_current_item()?;

        let consumed_start = self.current_item.range.start;
        let result = self.map.advance_and_consume(self.list, &mut self.current_item);
        Some((consumed_start .. consumed_start + result.len, result))
    }
}

impl<'a> From<OrigPatchesIter<'a>> for PositionalOpWalk {
    fn from(iter: OrigPatchesIter<'a>) -> Self {
        iter.into_patch()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Range;
    use crate::list::{ListCRDT, Order, PositionalComponent};
    use smallvec::{smallvec, SmallVec};
    use rle::{AppendRle, MergeableIterator};
    use crate::list::external_txn::{RemoteCRDTOp, RemoteId, RemoteTxn};
    use crate::list::positional::InsDelTag::*;
    use crate::list::time::docpatchiter::PositionalOpWalk;
    use crate::list::time::patchiter::ListPatchItem;

    fn assert_patches_matches(doc: &ListCRDT, expected: &PositionalOpWalk) {
        let actual: PositionalOpWalk = doc.iter_original_patches().into();
        assert_eq!(expected, &actual);

        // Also check we get the same thing if we don't ask for content.
        let expected_c = expected.components.iter().cloned().map(|mut c| {
            c.content_known = false;
            c
        }).merge_spans();

        let mut from: SmallVec<[Range<Order>; 1]> = smallvec![];
        let actual_c = doc.iter_original_patches().map(|(origin, c)| {
            from.push_rle(origin);
            c
        }).merge_spans();

        // dbg!(expected_c.collect::<Vec<_>>());
        // dbg!(actual_c.collect::<Vec<_>>());
        assert!(actual_c.eq(expected_c));
        assert_eq!(from, expected.origin_order);
    }

    #[test]
    fn patch_smoke() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there");
        doc.local_delete(0, 2, 3); // hiere

        assert!(doc.patch_iter().eq([
            ListPatchItem {
                range: 0..8,
                op_type: Ins,
                target_start: 0,
            },
            ListPatchItem {
                range: 8..11,
                op_type: Del,
                target_start: 2,
            },
        ]));

        let expected = PositionalOpWalk {
            components: smallvec![
                PositionalComponent {pos: 0, len: 2, content_known: true, tag: Ins},
                PositionalComponent {pos: 2, len: 3, content_known: false, tag: Ins},
                PositionalComponent {pos: 5, len: 3, content_known: true, tag: Ins},

                PositionalComponent {pos: 2, len: 3, content_known: false, tag: Del},
            ],
            origin_order: smallvec![0..11],
            content: "hiere".into(),
        };

        assert_patches_matches(&doc, &expected);
    }

    #[test]
    fn concurrent_deletes() {
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

        let expected = PositionalOpWalk {
            components: smallvec![
                PositionalComponent { pos: 0, len: 3, content_known: false, tag: Ins },
                PositionalComponent { pos: 0, len: 3, content_known: false, tag: Del },
                PositionalComponent { pos: 0, len: 3, content_known: false, tag: Del },
            ],
            origin_order: smallvec![0..9], // Disentangling this is the job of the reader.
            content: "".into(),
        };

        assert_patches_matches(&doc, &expected);
    }

    #[test]
    fn forwards_backwards() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("a");
        doc.local_insert(0, 0, "aa");
        doc.local_insert(0, 1, "bb"); // abba

        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "b".into(), seq: 0 },
            parents: smallvec![RemoteId { agent: "a".into(), seq: 2 }],
            ops: smallvec![RemoteCRDTOp::Del {
                id: RemoteId { agent: "a".into(), seq: 1 }, // delete the last a
                len: 1
            }],
            ins_content: "".into(),
        }); // abb
        doc.check(true);

        let expected = PositionalOpWalk {
            components: smallvec![
                PositionalComponent { pos: 0, len: 1, content_known: true, tag: Ins },
                PositionalComponent { pos: 1, len: 1, content_known: false, tag: Ins },
                PositionalComponent { pos: 1, len: 2, content_known: true, tag: Ins },
                PositionalComponent { pos: 2, len: 1, content_known: false, tag: Del },
            ],
            origin_order: smallvec![0..5],
            content: "abb".into(),
        };

        assert_patches_matches(&doc, &expected);
    }
}