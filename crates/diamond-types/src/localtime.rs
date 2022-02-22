use std::cmp::Ordering;
use std::fmt::{Debug, DebugStruct, Formatter};
use rle::{HasLength, MergableSpan, Searchable, SplitableSpan};

use crate::rle::RleKeyed;
use std::ops::Range;
use crate::list::Time;
use crate::ROOT_TIME;
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};


/// A time span defines a ... well, span in time. This is equivalent to Range<u64>, but it
/// implements Copy (which Range does not) and it has some locally useful methods.
#[derive(Copy, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
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

    pub fn is_empty(&self) -> bool {
        debug_assert!(self.start <= self.end);
        self.start == self.end
    }

    pub fn intersect(&self, other: &Self) -> Option<TimeSpan> {
        let result = TimeSpan {
            start: self.start.max(other.start),
            end: self.end.min(other.end),
        };
        if result.start <= result.end { Some(result) }
        else { None }
    }

    pub fn partial_cmp_time(&self, time: Time) -> Ordering {
        if time < self.start { Ordering::Less }
        else if time >= self.end { Ordering::Greater }
        else { Ordering::Equal }
    }

    pub fn iter(&self) -> impl Iterator<Item=usize> {
        Range::<usize>::from(self)
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

impl From<TimeSpan> for Range<usize> {
    fn from(span: TimeSpan) -> Self {
        span.start..span.end
    }
}
impl From<&TimeSpan> for Range<usize> {
    fn from(span: &TimeSpan) -> Self {
        span.start..span.end
    }
}

impl HasLength for TimeSpan {
    fn len(&self) -> usize {
        self.end - self.start
    }
}

impl SplitableSpan for TimeSpan {
    fn truncate(&mut self, at: usize) -> Self {
        let split = self.start + at;
        let other = TimeSpan {
            start: split,
            end: self.end,
        };

        self.end = split;
        other
    }

    #[inline]
    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let split = self.start + at;
        let other = TimeSpan {
            start: self.start,
            end: split,
        };
        self.start = split;
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
    fn rle_key(&self) -> usize {
        self.start
    }
}

pub(crate) const UNDERWATER_START: usize = usize::MAX / 4;

pub(crate) fn is_underwater(time: Time) -> bool {
    time >= UNDERWATER_START
}

// #[derive(Debug)]
struct RootTime;

impl Debug for RootTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ROOT")
    }
}

pub(crate) fn debug_time_raw<F: FnOnce(&dyn Debug) -> R, R>(val: Time, f: F) -> R {
    const LAST_TIME: usize = ROOT_TIME - 1;
    match val {
        ROOT_TIME => {
            f(&RootTime)
        },
        start @ (UNDERWATER_START..=LAST_TIME) => {
            f(&Underwater(start - UNDERWATER_START))
        },
        start => {
            f(&start)
        }
    }
}

pub(crate) fn debug_time(fmt: &mut DebugStruct, name: &str, val: Time) {
    debug_time_raw(val, |v| { fmt.field(name, v); });
}

// #[derive(Debug)]
struct Underwater(usize);

impl Debug for Underwater {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Underwater({})", self.0))
    }
}

impl Debug for TimeSpan {
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
    use crate::localtime::TimeSpan;

    #[test]
    fn splitable_timespan() {
        test_splitable_methods_valid(TimeSpan::new(10, 20));
    }
}