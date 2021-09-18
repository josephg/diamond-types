use std::ops::Range;

use crate::list::{ListCRDT, Order};
use crate::rangeextra::OrderRange;
use crate::rle::{RleSpanHelpers, RleVec, KVPair};
use std::cell::Cell;
use crate::order::OrderSpan;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum OpContent {
    Del(Order),
    Ins
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct OpItem {
    pub range: Range<Order>,
    pub content: OpContent,
}

impl Default for OpContent {
    // Arbitrary choice, but useful to have.
    fn default() -> Self { OpContent::Ins }
}

/// This is a simple iterator which iterates through the modifications made to a document, in time
/// (Order) order across a single contiguous span of time.
#[derive(Debug)]
pub(crate) struct ListPatchIter<'a, const FWD: bool> {
    deletes: &'a RleVec<KVPair<OrderSpan>>,
    range: Range<Order>,
    del_idx: Cell<usize>,
}

impl<'a, const FWD: bool> ListPatchIter<'a, FWD> {
    fn new(deletes: &'a RleVec<KVPair<OrderSpan>>, range: Range<Order>) -> Self {
        let del_idx = if FWD {
            if range.start == 0 { 0 }
            else {
                deletes.find_index(range.start).unwrap_or_else(|idx| idx)
            }
        } else {
            deletes
                .find_index(range.end)
                .unwrap_or_else(|idx| idx.wrapping_sub(1))
        };

        Self { deletes, range, del_idx: Cell::new(del_idx) }
    }
}

impl<'a> ListPatchIter<'a, true> {
    fn peek(&self) -> Option<OpItem> {
        if self.range.start < self.range.end {
            match self.deletes.search_scanning_sparse(self.range.start, &mut unsafe { *self.del_idx.as_ptr() }) {
                Ok(d) => {
                    // Its a delete.
                    debug_assert!(d.0 <= self.range.start && self.range.start < d.end());

                    let offset = self.range.start - d.0;
                    let target = d.1.order + offset;

                    let end = u32::min(self.range.end, d.end());
                    Some(OpItem {
                        range: self.range.start..end,
                        content: OpContent::Del(target)
                    })
                },
                Err(next_del) => {
                    // Its an insert.
                    let end = u32::min(self.range.end, next_del);
                    Some(OpItem {
                        range: self.range.start..end,
                        content: OpContent::Ins
                    })
                }
            }
        } else { None }
    }
}

impl<'a> ListPatchIter<'a, false> {
    fn peek(&self) -> Option<OpItem> {
        if self.range.start < self.range.end {
            let last_order = self.range.last_order();
            match self.deletes.search_scanning_backwards_sparse(last_order, &mut unsafe { *self.del_idx.as_ptr() }) {
                Ok(d) => {
                    // Its a delete.
                    debug_assert!(d.0 <= last_order && last_order < d.end());
                    let start = u32::max(self.range.start, d.0);
                    debug_assert!(start < self.range.end);
                    let offset = start - d.0;
                    let target = d.1.order + offset;

                    Some(OpItem {
                        range: start..self.range.end,
                        content: OpContent::Del(target)
                    })
                },
                Err(last_del) => {
                    // Its an insert.
                    let start = u32::max(self.range.start, last_del);

                    Some(OpItem {
                        range: start..self.range.end,
                        content: OpContent::Ins
                    })
                }
            }
        } else { None }
    }
}

impl<'a> Iterator for ListPatchIter<'a, true> {
    type Item = OpItem;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.peek()?;
        self.range.start = item.range.end;
        Some(item)
    }
}

impl<'a> Iterator for ListPatchIter<'a, false> {
    type Item = OpItem;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.peek()?;
        self.range.end = item.range.start;
        Some(item)
    }
}


impl ListCRDT {
    pub(crate) fn patch_iter(&self) -> ListPatchIter<true> {
        ListPatchIter::new(&self.deletes, 0..self.get_next_order())
    }

    pub(crate) fn patch_iter_in_range(&self, range: Range<Order>) -> ListPatchIter<true> {
        ListPatchIter::new(&self.deletes, range)
    }

    pub(crate) fn patch_iter_rev(&self) -> ListPatchIter<false> {
        ListPatchIter::new(&self.deletes, 0..self.get_next_order())
    }

    pub(crate) fn patch_iter_in_range_rev(&self, range: Range<Order>) -> ListPatchIter<false> {
        ListPatchIter::new(&self.deletes, range)
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use OpContent::*;

    fn assert_doc_patches_match(doc: &ListCRDT, range: Range<Order>, expect: &[OpItem]) {
        let forward = doc.patch_iter_in_range(range.clone());
        assert_eq!(forward.collect::<Vec<_>>(), expect);

        let backward = doc.patch_iter_in_range_rev(range.clone());
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