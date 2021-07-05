use crate::splitable_span::SplitableSpan;
use crate::range_tree::{EntryTraits, CRDTItem, EntryWithContent, AbsolutelyPositioned};

/// An OrderMarker defines a span of item orders, with a base and length.
/// If the length is negative, the span has been deleted in the document.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct OrderMarker {
    // TODO: Not sure what the right sizes of these two should be.
    pub order: u32,
    pub len: i32, // i16?
    // pub parent: usize,
}

impl Default for OrderMarker {
    fn default() -> Self {
        OrderMarker {
            // Super invalid.
            order: u32::MAX,
            len: 0,
            // parent: usize
        }
    }
}

impl SplitableSpan for OrderMarker {
    fn len(&self) -> usize {
        self.len.abs() as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let at_signed = at as i32 * self.len.signum();

        let other = OrderMarker {
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

impl EntryTraits for OrderMarker {
    type Item = usize; // Order.

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let other = OrderMarker {
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

impl EntryWithContent for OrderMarker {
    fn content_len(&self) -> usize {
        self.len.max(0) as usize
    }
}

impl CRDTItem for OrderMarker {
    fn is_insert(&self) -> bool {
        debug_assert!(self.len != 0);
        self.len > 0
    }

    fn mark_deleted(&mut self) {
        debug_assert!(self.len > 0);
        self.len = -self.len;
    }
}

// Really just for tests. Note this might be dangerous - the order in each item here might not name
// its position when used!
#[cfg(test)]
impl AbsolutelyPositioned for OrderMarker {
    fn pos(&self) -> u32 { self.order as u32 }
}