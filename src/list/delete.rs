use crate::list::Order;
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;

// TODO: Consider just reusing OrderMarker.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct DeleteEntry {
    pub order: Order,
    // So this is fancy. If the length is negative, we're counting down from order.
    pub len: u32,
}

impl SplitableSpan for DeleteEntry {
    fn len(&self) -> usize {
        // self.len.abs() as usize
        self.len as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder_len = self.len - at as u32;
        self.len = at as u32;
        Self {
            order: self.order + remainder_len,
            len: remainder_len,
        }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.order + self.len == other.order
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        self.order = other.order;
        self.len += other.len;
    }
}
