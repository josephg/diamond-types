use crate::yjs::Order;
use crate::range_tree::{EntryTraits, EntryWithContent, CRDTItem};
use crate::splitable_span::SplitableSpan;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub struct YjsSpan {
    pub order: Order,

    /**
     * The origin_left is only for the first item in the span. Each subsequent item has an
     * origin_left of order+offset
     */
    pub origin_left: Order,

    /**
     * Each item in the span has the same origin_right.
     */
    pub origin_right: Order,

    pub len: i32, // negative if deleted.
}

impl SplitableSpan for YjsSpan {
    fn len(&self) -> usize { self.len.abs() as usize }

    fn truncate(&mut self, at: usize) -> Self {
        debug_assert!(at > 0);
        let at_signed = at as i32 * self.len.signum();
        let other = YjsSpan {
            order: self.order + at as Order,
            origin_left: self.order + at as u32 - 1,
            origin_right: self.origin_right,
            len: self.len - at_signed
        };

        self.len = at_signed;
        other
    }

    fn can_append(&self, other: &Self) -> bool {
        let len = self.len.abs() as u32;
        (self.len > 0) == (other.len > 0)
            && other.order == self.order + len
            && other.origin_left == other.order - 1
            && other.origin_right == self.origin_right
    }

    fn append(&mut self, other: Self) {
        self.len += other.len
    }

    fn prepend(&mut self, other: Self) {
        self.order = other.order;
        self.len += other.len;
    }
}

impl EntryTraits for YjsSpan {
    type Item = Order;

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        debug_assert!(at > 0);
        let at_signed = at as i32 * self.len.signum();

        let other = YjsSpan {
            order: self.order,
            origin_left: self.origin_left,
            origin_right: self.origin_right,
            len: at_signed
        };

        self.order += at as Order;
        self.origin_left = self.order - 1;
        self.len -= at_signed;
        // origin_right stays the same.

        other
    }

    fn contains(&self, loc: Self::Item) -> Option<usize> {
        if (loc >= self.order) && (loc < self.order + self.len.abs() as u32) {
            Some((loc - self.order) as usize)
        } else {
            None
        }
    }

    fn is_valid(&self) -> bool {
        self.order != Order::MAX && self.len != 0
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.order + offset as Order
    }
}

impl EntryWithContent for YjsSpan {
    fn content_len(&self) -> usize {
        self.len.max(0) as usize
    }
}

impl CRDTItem for YjsSpan {
    fn is_insert(&self) -> bool {
        self.len > 0
    }

    fn mark_deleted(&mut self) {
        debug_assert!(self.len > 0);
        self.len = -self.len
    }
}