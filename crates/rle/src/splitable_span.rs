use std::ops::{Range, Add, Sub};

/// An entry is expected to contain multiple items.
///
/// A SplitableSpan is a range entry. That is, an entry which contains a compact run of many
/// entries internally.
pub trait SplitableSpan: Clone {
    /// The number of child items in the entry. This is indexed with the size used in truncate.
    fn len(&self) -> usize;

    /// Split the entry, returning the part of the entry which was jettisoned. After truncating at
    /// `pos`, self.len() == `pos` and the returned value contains the rest of the items.
    ///
    /// ```ignore
    /// let initial_len = entry.len();
    /// let rest = entry.truncate(truncate_at);
    /// assert!(initial_len == truncate_at + rest.len());
    /// ```
    ///
    /// `at` parameter must strictly obey *0 < at < entry.len()*
    fn truncate(&mut self, at: usize) -> Self;

    /// The inverse of truncate. This method mutably truncates an item, keeping all content from
    /// at..item.len() and returning the item range from 0..at.
    #[inline(always)]
    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let mut other = self.clone();
        *self = other.truncate(at);
        other
    }

    /// See if the other item can be appended to self. `can_append` will always be called
    /// immediately before `append`.
    fn can_append(&self, other: &Self) -> bool;

    /// Merge the passed item into self. Essentially, self = self + other.
    ///
    /// The other item *must* be a valid target for merging
    /// (as per can_append(), above).
    fn append(&mut self, other: Self);

    /// Append an item at the start of this item. self = other + self.
    ///
    /// This item must be a valid append target for other. That is, `other.can_append(self)` must
    /// be true for this method to be called.
    #[inline(always)]
    fn prepend(&mut self, mut other: Self) {
        other.append(self.clone());
        *self = other;
    }
}

/// A SplitableSpan wrapper for any single item.
#[derive(Copy, Clone, Hash, Debug, PartialEq, Eq, Default)]
pub struct Single<T>(pub T);

/// A splitablespan in reverse. This is useful for making lists in descending order.
#[derive(Copy, Clone, Hash, Debug, PartialEq, Eq, Default)]
pub struct ReverseSpan<S: SplitableSpan + Clone>(pub S);

impl<T: Clone> SplitableSpan for Single<T> {
    fn len(&self) -> usize { 1 }

    fn truncate(&mut self, _at: usize) -> Self { panic!("Cannot truncate single sized item"); }
    fn can_append(&self, _other: &Self) -> bool { false }
    fn append(&mut self, _other: Self) { panic!("Cannot append to single sized item"); }
}

impl<S: SplitableSpan + Clone> SplitableSpan for ReverseSpan<S> {
    fn len(&self) -> usize { self.0.len() }

    fn truncate(&mut self, at: usize) -> Self {
        ReverseSpan(self.0.truncate_keeping_right(at))
    }
    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        ReverseSpan(self.0.truncate(at))
    }

    fn can_append(&self, other: &Self) -> bool { other.0.can_append(&self.0) }
    fn append(&mut self, other: Self) { self.0.prepend(other.0); }
    fn prepend(&mut self, other: Self) { self.0.append(other.0); }
}

/// A splitablespan which contains a single element repeated N times.
#[derive(Copy, Clone, Hash, Debug, PartialEq, Eq, Default)]
pub struct RleRun<T: Clone + Eq> {
    pub val: T,
    pub len: usize,
}

impl<T: Clone + Eq> RleRun<T> {
    pub fn new(val: T, len: usize) -> Self {
        Self { val, len }
    }
}

impl<T: Clone + Eq> SplitableSpan for RleRun<T> {
    fn len(&self) -> usize { self.len }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder = self.len - at;
        self.len = at;
        Self { val: self.val.clone(), len: remainder }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.val == other.val
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }
}

// This will implement SplitableSpan for u8, u16, u32, u64, u128, usize
impl<T: Add<Output=T> + Sub + From<usize> + Copy + Eq> SplitableSpan for Range<T> where usize: From<<T as Sub>::Output> {
    fn len(&self) -> usize {
        (self.end - self.start).into()
    }

    fn truncate(&mut self, at: usize) -> Self {
        let old_end = self.end;
        self.end = self.start + at.into();
        Self { start: self.end, end: old_end }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.end == other.start
    }

    fn append(&mut self, other: Self) {
        self.end = other.end;
    }
}

/// Simple test helper to verify an implementation of SplitableSpan is valid and meets expected
/// constraints.
///
/// Use this to test splitablespan implementations in tests.
// #[cfg(test)]
pub fn test_splitable_methods_valid<E: SplitableSpan + std::fmt::Debug + Clone + Eq>(entry: E) {
    assert!(entry.len() >= 2, "Call this with a larger entry");
    for i in 1..entry.len() {
        // Split here and make sure we get the expected results.
        let mut start = entry.clone();
        let end = start.truncate(i);

        assert_eq!(start.len(), i);
        assert_eq!(end.len(), entry.len() - i);

        assert!(start.can_append(&end));

        let mut merge_append = start.clone();
        merge_append.append(end.clone());
        assert_eq!(merge_append, entry);

        let mut merge_prepend = end.clone();
        merge_prepend.prepend(start.clone());
        assert_eq!(merge_prepend, entry);

        // Split using truncate_keeping_right. We should get the same behaviour.
        let mut end2 = entry.clone();
        let start2 = end2.truncate_keeping_right(i);
        assert_eq!(end2, end);
        assert_eq!(start2, start);
    }
}

#[cfg(test)]
mod test {
    use crate::*;

    #[test]
    fn test_rle_run() {
        assert!(!RleRun { val: 10, len: 5 }.can_append(&RleRun { val: 20, len: 5 }));
        assert!(RleRun { val: 10, len: 5 }.can_append(&RleRun { val: 10, len: 15 }));

        test_splitable_methods_valid(RleRun { val: 12, len: 5 });
    }

    #[test]
    fn splitable_range() {
        test_splitable_methods_valid(0..10);
    }
}