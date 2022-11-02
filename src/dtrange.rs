use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::{Debug, DebugStruct, Formatter};
use rle::{HasLength, MergableSpan, Searchable, SplitableSpanHelpers};

use crate::rle::RleKeyed;
use std::ops::{Range, RangeBounds};
use crate::LV;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use crate::serde::DTRangeTuple;

/// This is an internal replacement for Range<usize>. The main use for this is that std::Range
/// doesn't implement Copy (urgh), and we need that for lots of types. But ultimately, this is just
/// a start and end pair. DTRange can be converted to and from std::Range with .from() and .into().
/// It also has some locally useful methods.
#[derive(Copy, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(from = "DTRangeTuple", into = "DTRangeTuple"))]
pub struct DTRange {
    pub start: usize,
    pub end: usize
}

impl DTRange {
    #[inline]
    pub fn new(start: usize, end: usize) -> DTRange {
        DTRange { start, end }
    }

    #[inline]
    pub fn new_from_len(start: usize, len: usize) -> DTRange {
        DTRange { start, end: start + len }
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

    pub fn is_empty(&self) -> bool {
        debug_assert!(self.start <= self.end);
        self.start == self.end
    }

    pub fn intersect(&self, other: &Self) -> Option<DTRange> {
        let result = DTRange {
            start: self.start.max(other.start),
            end: self.end.min(other.end),
        };
        if result.start <= result.end { Some(result) }
        else { None }
    }

    pub fn partial_cmp_time(&self, time: LV) -> Ordering {
        if time < self.start { Ordering::Less }
        else if time >= self.end { Ordering::Greater }
        else { Ordering::Equal }
    }

    pub fn iter(&self) -> impl Iterator<Item=usize> {
        Range::<usize>::from(self)
    }

    pub fn clear(&mut self) {
        self.start = self.end;
    }
}

impl From<usize> for DTRange {
    fn from(start: usize) -> Self {
        DTRange { start, end: start + 1 }
    }
}

impl From<Range<usize>> for DTRange {
    fn from(range: Range<usize>) -> Self {
        DTRange {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<DTRange> for Range<usize> {
    fn from(span: DTRange) -> Self {
        span.start..span.end
    }
}
impl From<&DTRange> for Range<usize> {
    fn from(span: &DTRange) -> Self {
        span.start..span.end
    }
}

impl RangeBounds<usize> for DTRange {
    fn start_bound(&self) -> Bound<&usize> {
        Bound::Included(&self.start)
    }
    fn end_bound(&self) -> Bound<&usize> {
        Bound::Excluded(&self.end)
    }
    fn contains<U>(&self, item: &U) -> bool where usize: PartialOrd<U>, U: ?Sized + PartialOrd<usize> {
        item >= &self.start && item < &self.end
    }
}

impl HasLength for DTRange {
    fn len(&self) -> usize {
        self.end - self.start
    }
}

impl SplitableSpanHelpers for DTRange {
    fn truncate_h(&mut self, at: usize) -> Self {
        let split = self.start + at;
        let other = DTRange {
            start: split,
            end: self.end,
        };

        self.end = split;
        other
    }

    #[inline]
    fn truncate_keeping_right_h(&mut self, at: usize) -> Self {
        let split = self.start + at;
        let other = DTRange {
            start: self.start,
            end: split,
        };
        self.start = split;
        other
    }
}

impl MergableSpan for DTRange {
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

impl Searchable for DTRange {
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
impl RleKeyed for DTRange {
    fn rle_key(&self) -> usize {
        self.start
    }
}

pub(crate) const UNDERWATER_START: usize = usize::MAX / 4;

pub(crate) fn is_underwater(time: LV) -> bool {
    time >= UNDERWATER_START
}

// #[derive(Debug)]
// struct RootTime;

// impl Debug for RootTime {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         f.write_str("ROOT")
//     }
// }

pub(crate) fn debug_time_raw<F: FnOnce(&dyn Debug) -> R, R>(val: LV, f: F) -> R {
    // const LAST_TIME: usize = usize::MAX;
    match val {
        // usize::MAX => {
        //     f(&RootTime)
        // },
        start @ (UNDERWATER_START..) => {
            f(&Underwater(start - UNDERWATER_START))
        },
        start => {
            f(&start)
        }
    }
}

pub(crate) fn debug_time(fmt: &mut DebugStruct, name: &str, val: LV) {
    debug_time_raw(val, |v| { fmt.field(name, v); });
}

// #[derive(Debug)]
struct Underwater(usize);

impl Debug for Underwater {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Underwater({})", self.0))
    }
}

impl Debug for DTRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "T ")?;
        debug_time_raw(self.start, |v| v.fmt(f) )?;
        write!(f, "..")?;
        debug_time_raw(self.end, |v| v.fmt(f) )?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use rle::test_splitable_methods_valid;
    use crate::dtrange::DTRange;

    #[test]
    fn splitable_timespan() {
        test_splitable_methods_valid(DTRange::new(10, 20));
    }
}