use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::{Debug, DebugStruct, Formatter};
use std::ops::RangeBounds;
use rle::{HasLength, HasRleKey, MergableSpan, Searchable, SplitableSpanHelpers};
// use std::range::Range;
use std::ops::Range as LegacyRange;

use crate::LV;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use crate::serde_helpers::DTRangeTuple;

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

// pub struct Foo(Range<usize>);

// This can't be an impl because of the orphan rule.
pub fn range_partial_cmp_lv(r: DTRange, lv: LV) -> Ordering {
    if lv < r.start { Ordering::Less }
    else if lv >= r.end { Ordering::Greater }
    else { Ordering::Equal }
}

pub fn range_take_first(r: &mut DTRange) -> Option<usize> {
    if r.is_empty() {
        None
    } else {
        let next = r.start;
        r.start += 1;
        Some(next)
    }
}

impl DTRange {
    pub fn last(&self) -> usize {
        debug_assert!(self.end > self.start); // last is invalid for empty spans.
        self.end - 1
    }

    pub fn contains(&self, item: &usize) -> bool {
        self.start <= *item && *item < self.end
    }

    pub fn is_empty(&self) -> bool {
        // debug_assert!(self.start <= self.end);
        self.start == self.end
    }

    pub fn iter(&self) -> impl Iterator<Item=usize> {
        LegacyRange::<usize>::from(self)
    }
}

impl From<usize> for DTRange {
    fn from(start: usize) -> Self {
        DTRange { start, end: start + 1 }
    }
}

impl From<LegacyRange<usize>> for DTRange {
    fn from(range: LegacyRange<usize>) -> Self {
        DTRange {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<&LegacyRange<usize>> for DTRange {
    fn from(range: &LegacyRange<usize>) -> Self {
        DTRange {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<DTRange> for LegacyRange<usize> {
    fn from(span: DTRange) -> Self {
        span.start..span.end
    }
}
impl From<&DTRange> for LegacyRange<usize> {
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
    #[inline]
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
    type Item = usize; // LV

    fn get_offset(&self, loc: Self::Item) -> Option<usize> {
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

// This is used for vector clocks. Note if you want order spans keyed by something else, use
// KVPair<OrderSpan> instead.
impl HasRleKey for DTRange {
    fn rle_key(&self) -> usize {
        self.start
    }
}

pub(crate) const UNDERWATER_START: usize = usize::MAX / 4;

pub(crate) fn debug_lv_raw<F: FnOnce(&dyn Debug) -> R, R>(val: LV, f: F) -> R {
    if val >= UNDERWATER_START { f(&Underwater(val - UNDERWATER_START)) }
    else { f(&val) }
}

pub(crate) fn debug_lv(fmt: &mut DebugStruct, name: &str, val: LV) {
    debug_lv_raw(val, |v| { fmt.field(name, v); });
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
        if self.start > self.end {
            write!(f, "INVALID: {}..{}", self.start, self.end)?;
        } else if self.is_empty() {
            write!(f, "(EMPTY)")?;
        } else {
            write!(f, "v ")?;
            debug_lv_raw(self.start, |v| v.fmt(f))?;
            if self.end != self.start + 1 {
                // write!(f, "-")?;
                // debug_time_raw(self.end - 1, |v| v.fmt(f))?;
                write!(f, "..")?;
                debug_lv_raw(self.end, |v| v.fmt(f))?;
            }
        }
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use rle::test_splitable_methods_valid;
    use crate::dtrange::DTRange;

    #[test]
    fn splitable_timespan() {
        test_splitable_methods_valid(DTRange::from(10..20));
    }
}