use crate::splitable_span::SplitableSpan;
use crate::range_tree::{EntryTraits, CRDTItem, EntryWithContent};

/// An OrderMarker defines a span of item orders, with a base and length.
/// If the length is negative, the span has been deleted in the document.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct OrderSpan {
    pub order: u32,
    // TODO: Make this u32 instead of i32.
    pub len: i32,
}

impl Default for OrderSpan {
    fn default() -> Self {
        OrderSpan {
            // Super invalid.
            order: u32::MAX,
            len: 0,
            // parent: usize
        }
    }
}

impl SplitableSpan for OrderSpan {
    fn len(&self) -> usize {
        self.len.abs() as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let at_signed = at as i32 * self.len.signum();

        let other = OrderSpan {
            order: self.order + at as u32,
            len: self.len - at_signed
        };

        self.len = at_signed;
        other
    }

    fn can_append(&self, other: &Self) -> bool {
        (self.len > 0) == (other.len > 0)
            && other.order == self.order + self.len.abs() as u32
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        self.order = other.order;
        self.len += other.len;
    }
}

impl EntryTraits for OrderSpan {
    type Item = usize; // Order.

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let other = OrderSpan {
            order: self.order,
            len: at as i32 * self.len.signum()
        };
        self.order += at as u32;
        self.len += if self.len < 0 { at as i32 } else { -(at as i32) };
        other
    }


    fn contains(&self, loc: Self::Item) -> Option<usize> {
        // debug_assert!(loc < self.len());
        let loc = loc as u32;
        if (loc >= self.order) && (loc < self.order + self.len.abs() as u32) {
            Some((loc - self.order) as usize)
        } else {
            None
        }
    }

    fn is_valid(&self) -> bool {
        self.order != u32::MAX && self.len != 0
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.order as usize + offset
    }
}

impl EntryWithContent for OrderSpan {
    fn content_len(&self) -> usize {
        self.len.max(0) as usize
    }
}

impl CRDTItem for OrderSpan {
    fn is_activated(&self) -> bool {
        debug_assert!(self.len != 0);
        self.len > 0
    }

    fn mark_deactivated(&mut self) {
        debug_assert!(self.len > 0);
        self.len = -self.len;
    }
}