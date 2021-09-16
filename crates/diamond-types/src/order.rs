use rle::Searchable;
use rle::SplitableSpan;

use crate::rle::{RleKey, RleKeyed};

/// An OrderMarker defines a span of item orders, with a base and length.
/// If the length is negative, the span has been deleted in the document.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct OrderSpan {
    pub order: u32,
    pub len: u32,
}

impl OrderSpan {
    pub fn new(order: u32, len: u32) -> OrderSpan {
        OrderSpan { order, len }
    }

    pub fn consume_start(&mut self, amt: u32) {
        self.order += amt;
        self.len -= amt;
    }

    pub fn end(&self) -> u32 {
        self.order + self.len
    }

    pub fn last(&self) -> u32 { self.order + self.len - 1 }
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
        self.len as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let at = at as u32;

        let other = OrderSpan {
            order: self.order + at,
            len: self.len - at
        };

        self.len = at;
        other
    }

    #[inline]
    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let at = at as u32;
        let other = OrderSpan {
            order: self.order,
            len: at
        };
        self.order += at;
        self.len -= at;
        other
    }

    // #[inline]
    fn can_append(&self, other: &Self) -> bool {
        other.order == self.order + self.len
    }

    // #[inline]
    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        self.order = other.order;
        self.len += other.len;
    }
}

impl Searchable for OrderSpan {
    type Item = usize; // Order.

    fn contains(&self, loc: Self::Item) -> Option<usize> {
        // debug_assert!(loc < self.len());
        let loc = loc as u32;
        if (loc >= self.order) && (loc < self.order + self.len) {
            Some((loc - self.order) as usize)
        } else {
            None
        }
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.order as usize + offset
    }
}

// impl EntryWithContent for OrderSpan {
//     fn content_len(&self) -> usize {
//         self.len as usize
//     }
// }

// impl CRDTItem for OrderSpan {
//     fn is_activated(&self) -> bool {
//         debug_assert!(self.len != 0);
//         self.len > 0
//     }
//
//     fn mark_deactivated(&mut self) {
//         debug_assert!(self.len > 0);
//         self.len = -self.len;
//     }
// }

// This is used for vector clocks. Note if you want order spans keyed by something else, use
// KVPair<OrderSpan> instead.
impl RleKeyed for OrderSpan {
    fn get_rle_key(&self) -> RleKey {
        self.order
    }
}