use rle::{HasLength, MergableSpan, Searchable, SplitableSpan};

use crate::rle::RleKeyed;
use std::ops::Range;

/// A time span defines a ... well, span in time. This is equivalent to Range<u64>, but it
/// implements Copy (which Range does not) and it has some locally useful methods.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct TimeSpan {
    pub start: usize,
    pub end: usize
}

impl TimeSpan {
    #[inline]
    pub fn new(start: usize, end: usize) -> TimeSpan {
        TimeSpan { start, end }
    }

    #[inline]
    pub fn new_from_len(start: usize, len: usize) -> TimeSpan {
        TimeSpan { start, end: start + len }
    }

    pub fn consume_start(&mut self, amt: usize) {
        self.start += amt;
    }

    pub fn end(&self) -> usize {
        self.end
    }

    pub fn last(&self) -> usize { self.end - 1 }

    pub fn contains(&self, item: usize) -> bool {
        self.start <= item && item < self.end
    }
}

impl From<Range<usize>> for TimeSpan {
    fn from(range: Range<usize>) -> Self {
        TimeSpan {
            start: range.start,
            end: range.end,
        }
    }
}

impl HasLength for TimeSpan {
    fn len(&self) -> usize {
        self.end - self.start
    }
}

impl SplitableSpan for TimeSpan {
    fn truncate(&mut self, at: usize) -> Self {
        let other = TimeSpan {
            start: at,
            end: self.end,
        };

        self.end = at;
        other
    }

    #[inline]
    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let other = TimeSpan {
            start: self.start,
            end: at,
        };
        self.end = at;
        other
    }
}

impl MergableSpan for TimeSpan {
    // #[inline]
    fn can_append(&self, other: &Self) -> bool {
        other.start == self.end
    }

    // #[inline]
    fn append(&mut self, other: Self) {
        self.end = other.end;
    }

    fn prepend(&mut self, other: Self) {
        self.start = other.start;
    }
}

impl Searchable for TimeSpan {
    type Item = usize; // Time

    fn get_offset(&self, loc: Self::Item) -> Option<usize> {
        // debug_assert!(loc < self.len());
        if loc >= self.start && loc < self.end {
            Some(loc - self.start)
        } else {
            None
        }
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.start + offset
    }
}

// impl EntryWithContent for OrderSpan {
//     fn content_len(&self) -> usize {
//         self.len as usize
//     }
// }

// This is used for vector clocks. Note if you want order spans keyed by something else, use
// KVPair<OrderSpan> instead.
impl RleKeyed for TimeSpan {
    fn get_rle_key(&self) -> usize {
        self.start
    }
}