use std::ops::Range;
use crate::{HasLength, MergableSpan, SplitableSpanHelpers};

/// A splitablespan which contains a single element repeated N times. This is used in some examples.
#[derive(Copy, Clone, Hash, Debug, PartialEq, Eq, Default)]
pub struct RleRun<T: Clone + Eq> {
    pub val: T,
    pub len: usize,
}

impl<T: Clone + Eq> RleRun<T> {
    pub fn new(val: T, len: usize) -> Self {
        Self { val, len }
    }

    pub fn single(val: T) -> Self {
        Self { val, len: 1 }
    }
}

impl<T: Clone + Eq> HasLength for RleRun<T> {
    fn len(&self) -> usize { self.len }
}

impl<T: Clone + Eq> SplitableSpanHelpers for RleRun<T> {
    fn truncate_h(&mut self, at: usize) -> Self {
        let remainder = self.len - at;
        self.len = at;
        Self { val: self.val.clone(), len: remainder }
    }
}

impl<T: Clone + Eq> MergableSpan for RleRun<T> {
    fn can_append(&self, other: &Self) -> bool {
        self.val == other.val || self.len == 0
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
        self.val = other.val; // Needed when we use default() - which gives it a length of 0.
    }
}

/// Distinct RLE run. Each distinct run expresses some value between each (start, end) pair.
#[derive(Copy, Clone, Hash, Debug, PartialEq, Eq, Default)]
pub struct RleDRun<T> {
    pub start: usize,
    pub end: usize,
    pub val: T,
}

impl<T: Clone> RleDRun<T> {
    pub fn new(range: Range<usize>, val: T) -> Self {
        Self {
            start: range.start,
            end: range.end,
            val,
        }
    }
}

impl<T: Clone> HasLength for RleDRun<T> {
    fn len(&self) -> usize { self.end - self.start }
}

impl<T: Clone> SplitableSpanHelpers for RleDRun<T> {
    fn truncate_h(&mut self, at: usize) -> Self {
        let split_point = self.start + at;
        debug_assert!(split_point < self.end);
        let remainder = Self { start: split_point, end: self.end, val: self.val.clone() };
        self.end = split_point;
        remainder
    }
}

impl<T: Clone + Eq> MergableSpan for RleDRun<T> {
    fn can_append(&self, other: &Self) -> bool {
        self.end == other.start && self.val == other.val
    }

    fn append(&mut self, other: Self) {
        self.end = other.end;
    }
}


// impl<T: Copy + std::fmt::Debug> Searchable for RleDRun<T> {
//     type Item = T;
//
//     fn get_offset(&self, _loc: Self::Item) -> Option<usize> {
//         unimplemented!()
//     }
//
//     fn at_offset(&self, offset: usize) -> Self::Item {
//         Some(
//     }
// }
