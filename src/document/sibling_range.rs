use crate::splitable_span::SplitableSpan;
use crate::range_tree::EntryTraits;
use crate::document::{Order, ROOT_ORDER};

/// The sibling tree optimizes lookups to find insert positions after concurrent writes.
///
/// When concurrent writes happen, we need to not only scan through the parent node's children. We
/// also need to sometimes "fall off the end" and find the next location in the document after the
/// last sibling, and all of the last sibling's descendants.
///
/// There's two other ways we could solve this:
///
/// - Linearly scan forward in the document from the last sibling. This is O(n log n) with the
///   number of concurrent items; but since n is usually small this will be reasonably fast in
///   practice. Unfortunately the way the marker tree is set up, this is super awkward.
///
/// - Treat inserts as a tree and scan up until we find a parent with more children. This sounds
///   good, but in practice we'll often have O(n) parents (eg if you copy+paste 1mb of text, the
///   last character has a depth of 1 million.)
///
/// Both of these approaches have linear time in some circumstances which will show up if you're
/// using this library like git, and have large concurrent changes in patch sets. Also either way we
/// need to store sibling information in a tree or map of some sort *anyway*. This approach gives us
/// log(n) time in all cases, and it should be about as large in memory as the sibling set anyway.
///
/// It increases code size though, because we'll end up with another monomorphized copy of
/// range_tree. That will make the created JS bundle bigger by like 30kb or something - but its
/// probably fine.

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct SiblingRange {
    pub len: usize,
    /// Order of the next item that is a right sibling of my parent, or the next item that is after
    /// all our siblings and subtrees. When using this you need to look at the named item's parent
    /// and compare it to your own.
    pub next_sibling: Order,
}

impl Default for SiblingRange {
    fn default() -> Self {
        Self {
            len: 0,
            next_sibling: ROOT_ORDER
        }
    }
}

impl SplitableSpan for SiblingRange {
    fn len(&self) -> usize { self.len }

    fn truncate(&mut self, at: usize) -> Self {
        let other = SiblingRange {
            len: self.len - at,
            next_sibling: self.next_sibling
        };

        self.len = at;
        other
    }

    fn can_append(&self, other: &Self) -> bool {
        other.next_sibling == self.next_sibling
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        self.len += other.len;
    }
}

impl EntryTraits for SiblingRange {
    type Item = Order;

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let left = SiblingRange {
            len: at,
            next_sibling: self.next_sibling
        };
        self.len -= at;
        left
    }

    fn contains(&self, loc: Self::Item) -> Option<usize> {
        // self.next_sibling == Order
        unimplemented!("Can't search by sibling_range")
    }

    fn is_valid(&self) -> bool {
        self.len > 0
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.next_sibling
    }
}