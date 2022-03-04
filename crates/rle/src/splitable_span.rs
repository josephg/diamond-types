use std::ops::{Deref, DerefMut, Range};

pub trait HasLength {
    /// The number of child items in the entry. This is indexed with the size used in truncate.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait SplitableSpanCtx: Clone {
    type Ctx: ?Sized;

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
    fn truncate_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self;

    /// The inverse of truncate. This method mutably truncates an item, keeping all content from
    /// at..item.len() and returning the item range from 0..at.
    #[inline(always)]
    fn truncate_keeping_right_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        let mut other = self.clone();
        *self = other.truncate_ctx(at, ctx);
        other
    }

    fn split_ctx(mut self, at: usize, ctx: &Self::Ctx) -> (Self, Self) {
        let remainder = self.truncate_ctx(at, ctx);
        (self, remainder)
    }
}

pub trait SplitableSpan: Clone {
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

    fn split(mut self, at: usize) -> (Self, Self) {
        let remainder = self.truncate(at);
        (self, remainder)
    }
}

impl<T: SplitableSpan> SplitableSpanCtx for T {
    type Ctx = ();

    fn truncate_ctx(&mut self, at: usize, _ctx: &()) -> Self {
        self.truncate(at)
    }

    fn truncate_keeping_right_ctx(&mut self, at: usize, _ctx: &()) -> Self {
        self.truncate_keeping_right(at)
    }

    fn split_ctx(self, at: usize, _ctx: &()) -> (Self, Self) {
        self.split(at)
    }
}

// This is a bit of a hack. This wrapper trait lets us use regular splitablespan methods in lots of
// situations.
#[derive(Debug)]
pub struct WithCtx<'a, T: SplitableSpanCtx + Clone>(pub T, pub &'a T::Ctx);

impl<'a, T: SplitableSpanCtx + Clone> Deref for WithCtx<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, T: SplitableSpanCtx + Clone> DerefMut for WithCtx<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a, T: SplitableSpanCtx + Clone> WithCtx<'a, T> {
    pub fn new(t: T, ctx: &'a T::Ctx) -> Self {
        Self(t, ctx)
    }

    pub fn to_inner(self) -> T {
        self.0
    }
}

impl<'a, T: SplitableSpanCtx + Clone> Clone for WithCtx<'a, T> {
    fn clone(&self) -> Self {
        WithCtx(self.0.clone(), self.1)
    }
}

impl<'a, T: SplitableSpanCtx> SplitableSpan for WithCtx<'a, T> {
    fn truncate(&mut self, at: usize) -> Self {
        WithCtx(self.0.truncate_ctx(at, self.1), self.1)
    }

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        WithCtx(self.0.truncate_keeping_right_ctx(at, self.1), self.1)
    }
}

impl<'a, T: SplitableSpanCtx + HasLength> HasLength for WithCtx<'a, T> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

// struct WithoutCtx<T: SplitableSpanCtx<Ctx=()>>(T);


// impl<T: SplitableSpanCtx<Ctx=()>> SplitableSpan for T {
//     fn truncate(&mut self, at: usize) -> Self {
//         self.truncate_ctx(at, &())
//     }
//
//     fn truncate_keeping_right(&mut self, at: usize) -> Self {
//         self.truncate_keeping_right_ctx(at, &())
//     }
//
//     fn split(self, at: usize) -> (Self, Self) {
//         self.split_ctx(at, &())
//     }
// }

pub trait TrimCtx: SplitableSpanCtx + HasLength {
    /// Trim self to at most `at` items. Remainder (if any) is returned.
    fn trim_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Option<Self> {
        if at >= self.len() {
            None
        } else {
            Some(self.truncate_ctx(at, ctx))
        }
    }
}

impl<T: SplitableSpanCtx + HasLength> TrimCtx for T {}

pub trait Trim: SplitableSpan + HasLength {
    fn trim(&mut self, at: usize) -> Option<Self> {
        self.trim_ctx(at, &())
    }
}

impl<T: SplitableSpan + HasLength> Trim for T {}



pub trait MergableSpan: Clone {
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

/// An entry is expected to contain multiple items.
///
/// A SplitableSpan is a range entry. That is, an entry which contains a compact run of many
/// entries internally.
pub trait SplitAndJoinSpan: HasLength + SplitableSpan + MergableSpan {}
impl<T: HasLength + SplitableSpan + MergableSpan> SplitAndJoinSpan for T {}

pub trait SplitAndJoinSpanCtx: HasLength + SplitableSpanCtx + MergableSpan {}
impl<T: HasLength + SplitableSpanCtx + MergableSpan> SplitAndJoinSpanCtx for T {}

/// A SplitableSpan wrapper for any single item.
#[derive(Copy, Clone, Hash, Debug, PartialEq, Eq, Default)]
pub struct Single<T>(pub T);

/// A splitablespan in reverse. This is useful for making lists in descending order.
#[derive(Copy, Clone, Hash, Debug, PartialEq, Eq, Default)]
pub struct ReverseSpan<S>(pub S);

impl<T> HasLength for Single<T> {
    fn len(&self) -> usize { 1 }
}
impl<T: Clone> SplitableSpan for Single<T> {
    // This is a valid impl because truncate can never be called for single items.
    fn truncate(&mut self, _at: usize) -> Self { panic!("Cannot truncate single sized item"); }
}
impl<T: Clone> MergableSpan for Single<T> {
    fn can_append(&self, _other: &Self) -> bool { false }
    fn append(&mut self, _other: Self) { panic!("Cannot append to single sized item"); }
}

impl<S: HasLength> HasLength for ReverseSpan<S> {
    fn len(&self) -> usize { self.0.len() }
}
impl<S: SplitableSpan> SplitableSpan for ReverseSpan<S> {
    fn truncate(&mut self, at: usize) -> Self {
        ReverseSpan(self.0.truncate_keeping_right(at))
    }
    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        ReverseSpan(self.0.truncate(at))
    }
}
impl<S: MergableSpan> MergableSpan for ReverseSpan<S> {
    fn can_append(&self, other: &Self) -> bool { other.0.can_append(&self.0) }
    fn append(&mut self, other: Self) { self.0.prepend(other.0); }
    fn prepend(&mut self, other: Self) { self.0.append(other.0); }
}

impl<A, B> MergableSpan for (A, B) where A: MergableSpan, B: MergableSpan {
    fn can_append(&self, other: &Self) -> bool {
        self.0.can_append(&other.0) && self.1.can_append(&other.1)
    }

    fn append(&mut self, other: Self) {
        self.0.append(other.0);
        self.1.append(other.1);
    }
}

impl<A, B> HasLength for (A, B) where A: HasLength {
    fn len(&self) -> usize {
        // debug_assert_eq!(self.0.len(), self.1.len());
        self.0.len()
    }
}

impl<A, B> SplitableSpan for (A, B) where A: SplitableSpan, B: SplitableSpan {
    fn truncate(&mut self, at: usize) -> Self {
        (self.0.truncate(at), self.1.truncate(at))
    }
}

// impl<A, B> SplitableSpanCtx for (A, B) where A: SplitableSpanCtx, B: SplitableSpanCtx, A::Ctx: Sized, B::Ctx: Sized {
//     type Ctx = (A::Ctx, B::Ctx);
//
//     fn truncate_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
//         (self.0.truncate_ctx(at, &ctx.0), self.1.truncate_ctx(at, &ctx.1))
//     }
// }


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
}

impl<T: Clone + Eq> HasLength for RleRun<T> {
    fn len(&self) -> usize { self.len }
}
impl<T: Clone + Eq> SplitableSpan for RleRun<T> {
    fn truncate(&mut self, at: usize) -> Self {
        let remainder = self.len - at;
        self.len = at;
        Self { val: self.val.clone(), len: remainder }
    }
}
impl<T: Clone + Eq> MergableSpan for RleRun<T> {
    fn can_append(&self, other: &Self) -> bool {
        self.val == other.val
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }
}

// impl<T, E> SplitableSpan for Result<T, E> where T: SplitableSpan + Clone, E: Clone {
//     fn truncate(&mut self, at: usize) -> Self {
//         match self {
//             Ok(v) => Result::Ok(v.truncate(at)),
//             Err(e) => Result::Err(e.clone())
//         }
//     }
// }

impl<T, E> SplitableSpanCtx for Result<T, E> where T: SplitableSpanCtx + Clone, E: Clone {
    type Ctx = T::Ctx;

    fn truncate_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        match self {
            Ok(v) => Result::Ok(v.truncate_ctx(at, ctx)),
            Err(e) => Result::Err(e.clone())
        }
    }
}

impl<T, E> HasLength for Result<T, E> where T: HasLength, Result<T, E>: Clone {
    fn len(&self) -> usize {
        match self {
            Ok(val) => val.len(),
            Err(_) => 1
        }
    }
}

impl<V> SplitableSpan for Option<V> where V: SplitableSpan {
    fn truncate(&mut self, at: usize) -> Self {
        self.as_mut().map(|v| v.truncate(at))
        // match self {
        //     None => None,
        //     Some(v) => Some(v.truncate(at))
        // }
    }
}

// This will implement SplitableSpan for u8, u16, u32, u64, u128, usize
// impl<T: Add<Output=T> + Sub + Copy + Eq> SplitableSpan for Range<T>
// where usize: From<<T as Sub>::Output>, T: From<usize>
// {
//     fn len(&self) -> usize {
//         (self.end - self.start).into()
//     }
//
//     fn truncate(&mut self, at: usize) -> Self {
//         let old_end = self.end;
//         self.end = self.start + at.into();
//         Self { start: self.end, end: old_end }
//     }
//
//     fn can_append(&self, other: &Self) -> bool {
//         self.end == other.start
//     }
//
//     fn append(&mut self, other: Self) {
//         self.end = other.end;
//     }
// }
impl HasLength for Range<u32> {
    fn len(&self) -> usize { (self.end - self.start) as _ }
}
impl SplitableSpan for Range<u32> {
    // This is a valid impl because truncate can never be called for single items.
    fn truncate(&mut self, at: usize) -> Self {
        let old_end = self.end;
        self.end = self.start + at as u32;
        Self { start: self.end, end: old_end }
    }
}
impl MergableSpan for Range<u32> {
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
pub fn test_splitable_methods_valid<E: SplitAndJoinSpan + std::fmt::Debug + Clone + Eq>(entry: E) {
    test_splitable_methods_valid_ctx(entry, &());
}

pub fn test_splitable_methods_valid_ctx<E: SplitAndJoinSpanCtx + std::fmt::Debug + Clone + Eq>(entry: E, ctx: &E::Ctx) {
    assert!(entry.len() >= 2, "Call this with a larger entry");
    // dbg!(&entry);

    for i in 1..entry.len() {
        // Split here and make sure we get the expected results.
        let mut start = entry.clone();
        let end = start.truncate_ctx(i, ctx);
        // dbg!(&start, &end);

        assert_eq!(start.len(), i);
        assert_eq!(end.len(), entry.len() - i);

        // dbg!(&start, &end);
        assert!(start.can_append(&end));

        let mut merge_append = start.clone();

        // dbg!(&start, &end);
        merge_append.append(end.clone());
        // dbg!(&merge_append);
        assert_eq!(merge_append, entry);

        let mut merge_prepend = end.clone();
        merge_prepend.prepend(start.clone());
        assert_eq!(merge_prepend, entry);

        // Split using truncate_keeping_right. We should get the same behaviour.
        let mut end2 = entry.clone();
        let start2 = end2.truncate_keeping_right_ctx(i, ctx);
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
        test_splitable_methods_valid(0u32..10u32);
    }
}