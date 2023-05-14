use rle::{HasLength, MergableSpan, Searchable, SplitableSpan, SplitableSpanHelpers};

use crate::rle::{RleKey, RleKeyed};
use std::ops::Range;
use crate::list::LV;
use crate::rangeextra::OrderRange;

/// An OrderMarker defines a span of item orders, with a base and length.
/// If the length is negative, the span has been deleted in the document.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct TimeSpan {
    pub start: u32,
    pub len: u32,
}

impl TimeSpan {
    pub fn new(start: u32, len: u32) -> TimeSpan {
        TimeSpan { start, len }
    }

    pub fn consume_start(&mut self, amt: u32) {
        self.start += amt;
        self.len -= amt;
    }

    pub fn end(&self) -> u32 {
        self.start + self.len
    }

    pub fn last(&self) -> u32 { self.start + self.len - 1 }

    pub fn is_empty(&self) -> bool { self.len == 0 }
}

impl Default for TimeSpan {
    fn default() -> Self {
        TimeSpan {
            // Super invalid.
            start: u32::MAX,
            len: 0,
            // parent: usize
        }
    }
}

impl From<Range<LV>> for TimeSpan {
    fn from(range: Range<LV>) -> Self {
        Self {
            start: range.start,
            len: range.order_len(),
        }
    }
}

impl HasLength for TimeSpan {
    fn len(&self) -> usize {
        self.len as usize
    }
}
impl SplitableSpanHelpers for TimeSpan {
    fn truncate_h(&mut self, at: usize) -> Self {
        let at = at as u32;

        let other = TimeSpan {
            start: self.start + at,
            len: self.len - at
        };

        self.len = at;
        other
    }

    #[inline]
    fn truncate_keeping_right_h(&mut self, at: usize) -> Self {
        let at = at as u32;
        let other = TimeSpan {
            start: self.start,
            len: at
        };
        self.start += at;
        self.len -= at;
        other
    }
}
impl MergableSpan for TimeSpan {
    // #[inline]
    fn can_append(&self, other: &Self) -> bool {
        other.start == self.start + self.len
    }

    // #[inline]
    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        self.start = other.start;
        self.len += other.len;
    }
}

impl Searchable for TimeSpan {
    type Item = usize; // Order.

    fn get_offset(&self, loc: Self::Item) -> Option<usize> {
        // debug_assert!(loc < self.len());
        let loc = loc as u32;
        if (loc >= self.start) && (loc < self.start + self.len) {
            Some((loc - self.start) as usize)
        } else {
            None
        }
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.start as usize + offset
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
impl RleKeyed for TimeSpan {
    fn get_rle_key(&self) -> RleKey {
        self.start
    }
}