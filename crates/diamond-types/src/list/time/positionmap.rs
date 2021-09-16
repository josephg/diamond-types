
// There's 3 states a position component can be in:
// - Not inserted (yet), with a postlen
// - Inserted (and in the document)
// - Inserted then deleted

use rle::{SplitableSpan, Searchable};
use crate::list::{ListCRDT, Order};
use content_tree::{ContentTreeWithIndex, FullIndex, ContentLength};
use crate::list::time::positionmap::PositionComponent::*;
use std::ops::Range;
use crate::rle::RleSpanHelpers;
use crate::rangeextra::OrderRange;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum PositionComponent {
    NotInsertedYet,
    Inserted,
    Deleted(u32) // Storing the number of times this has been deleted. >0.
}

// It would be nicer to just use RleRun but I want to customize
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
struct PositionRun {
    val: PositionComponent,
    len: usize // This is the full length that we take up in the final document
}

impl Default for PositionComponent {
    fn default() -> Self { NotInsertedYet }
}

impl PositionRun {
    fn new(val: PositionComponent, len: usize) -> Self {
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

/// This is a simple iterator which iterates through the modifications made to a document, in time
/// (Order) order across a single contiguous span of time.
#[derive(Debug)]
pub(crate) struct ListPatchIter<'a> {
    doc: &'a ListCRDT,
    range: Range<Order>,
    fwd_del_idx: usize,
    back_del_idx: usize,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum OpContent {
    Del(Order),
    Ins
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct OpItem {
    pub range: Range<Order>,
    pub content: OpContent,
}

impl<'a> ListPatchIter<'a> {
    fn new(doc: &'a ListCRDT, range: Range<Order>) -> Self {
        let fwd_del_idx = if range.start == 0 { 0 }
        else {
            doc.deletes.find_index(range.start).unwrap_or_else(|idx| idx)
        };

        // TODO: Test me!
        let back_del_idx = doc.deletes
            .find_index(range.end)
            .unwrap_or_else(|idx| idx.wrapping_sub(1));

        Self {
            doc,
            range,
            fwd_del_idx,
            back_del_idx,
        }
    }
}

impl<'a> Iterator for ListPatchIter<'a> {
    type Item = OpItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range.start < self.range.end {
            match self.doc.deletes.search_scanning_sparse(self.range.start, &mut self.fwd_del_idx) {
                Ok(d) => {
                    // Its a delete.
                    debug_assert!(d.0 <= self.range.start && self.range.start < d.end());

                    let offset = self.range.start - d.0;
                    let target = d.1.order + offset;

                    let end = u32::min(self.range.end, d.end());
                    let range = self.range.start..end;

                    self.range.start = end; // Advance us.
                    Some(OpItem { range, content: OpContent::Del(target) })
                },
                Err(next_del) => {
                    // Its an insert.
                    let end = u32::min(self.range.end, next_del);
                    let range = self.range.start..end;
                    self.range.start = end;
                    Some(OpItem { range, content: OpContent::Ins })
                }
            }
        } else { None }
    }
}

impl<'a> DoubleEndedIterator for ListPatchIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.range.start < self.range.end {
            let last_order = self.range.last_order();
            match self.doc.deletes.search_scanning_backwards_sparse(last_order, &mut self.back_del_idx) {
                Ok(d) => {
                    // Its a delete.
                    debug_assert!(d.0 <= last_order && last_order < d.end());
                    let start = u32::max(self.range.start, d.0);
                    debug_assert!(start < self.range.end);
                    let offset = start - d.0;
                    let target = d.1.order + offset;

                    let range = start..self.range.end;
                    self.range.end = start; // Advance us.
                    Some(OpItem { range, content: OpContent::Del(target) })
                },
                Err(last_del) => {
                    // Its an insert.
                    let start = u32::max(self.range.start, last_del);

                    let range = start..self.range.end;
                    self.range.end = start;
                    Some(OpItem { range, content: OpContent::Ins })
                }
            }
        } else { None }
    }
}

impl ListCRDT {
    pub(crate) fn patch_iter(&self) -> ListPatchIter {
        ListPatchIter::new(self, 0..self.get_next_order())
    }

    pub(crate) fn patch_iter_in_range(&self, range: Range<Order>) -> ListPatchIter {
        ListPatchIter::new(self, range)
    }

    pub fn foo(&self) {
        // let mut p = PositionMap::new();
        //
        // // TODO: This is something we should cache somewhere.
        // let total_post_len: usize = self.range_tree.raw_iter().map(|e| e.len()).sum();
        // p.push(PositionRun::new(NotInsertedYet, total_post_len));
        //
        // for walk in self.txns.txn_spanning_tree_iter() {
        //     for _range in walk.retreat {
        //         unimplemented!();
        //     }
        //
        //     for _range in walk.advance_rev {
        //         unimplemented!();
        //     }
        //
        //     let mut r = walk.consume;
        //     while r.start < r.end {
        //
        //     }
        // }
    }
}

#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;
    use super::*;
    use OpContent::*;

    #[test]
    fn positionrun_is_splitablespan() {
        test_splitable_methods_valid(PositionRun::new(NotInsertedYet, 5));
        test_splitable_methods_valid(PositionRun::new(Inserted, 5));
        test_splitable_methods_valid(PositionRun::new(Deleted(1), 5));

        assert!(PositionRun::new(Deleted(1), 1)
            .can_append(&PositionRun::new(Deleted(1), 2)));
        assert!(!PositionRun::new(Deleted(1), 1)
            .can_append(&PositionRun::new(Deleted(999), 2)));
    }

    fn assert_doc_patches_match(doc: &ListCRDT, range: Range<Order>, expect: &[OpItem]) {
        let forward = doc.patch_iter_in_range(range.clone());
        assert_eq!(forward.collect::<Vec<_>>(), expect);

        let backward = doc.patch_iter_in_range(range.clone()).rev();
        let mut actual = backward.collect::<Vec<_>>();
        actual.reverse();
        assert_eq!(actual, expect);
    }

    #[test]
    fn walk_simple_doc() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there");
        doc.local_delete(0, 2, 6);

        assert_doc_patches_match(&doc, 0..doc.get_next_order(), &[
            OpItem { range: 0..8, content: Ins },
            OpItem { range: 8..14, content: Del(2) }
        ]);
    }
}