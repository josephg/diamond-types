use std::fmt::{Debug, Formatter};

use rle::{HasRleKey, HasLength, MergableSpan, Searchable, SplitableSpan, SplitableSpanCtx};
pub use rle_vec::RleVec;
use crate::dtrange::{debug_time_raw, DTRange};

pub mod rle_vec;

pub trait RleSpanHelpers: HasRleKey + HasLength {
    fn end(&self) -> usize {
        self.rle_key() + self.len()
    }

    fn last(&self) -> usize {
        self.end() - 1
    }

    fn span(&self) -> DTRange {
        let start = self.rle_key();
        DTRange { start, end: start + self.len() }
    }
}

impl<V: HasRleKey + HasLength> RleSpanHelpers for V {}

pub trait RleKeyedAndSplitable: HasRleKey + SplitableSpanCtx {
    #[inline(always)]
    fn truncate_from_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        self.truncate_ctx(at - self.rle_key(), ctx)
    }

    #[inline(always)]
    fn truncate_keeping_right_from_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        self.truncate_keeping_right_ctx(at - self.rle_key(), ctx)
    }

    #[inline(always)]
    fn truncate_from(&mut self, at: usize) -> Self where Self: SplitableSpan {
        self.truncate(at - self.rle_key())
    }

    #[inline(always)]
    fn truncate_keeping_right_from(&mut self, at: usize) -> Self where Self: SplitableSpan {
        self.truncate_keeping_right(at - self.rle_key())
    }
}

impl<V: HasRleKey + SplitableSpanCtx> RleKeyedAndSplitable for V {}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct KVPair<V>(pub usize, pub V);

impl<V: Debug> Debug for KVPair<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_tuple("KVPair");
        debug_time_raw(self.0, |v| { s.field(v); });
        s.field(&self.1);
        s.finish()
    }
}


impl<V> HasRleKey for KVPair<V> {
    fn rle_key(&self) -> usize {
        self.0
    }
}

impl<V: HasLength> HasLength for KVPair<V> {
    fn len(&self) -> usize { self.1.len() }
}

impl<V: HasLength> KVPair<V> {
    pub fn range(&self) -> DTRange {
        DTRange::new(self.0, self.0 + self.len())
    }
}

impl<V: SplitableSpanCtx> SplitableSpanCtx for KVPair<V> {
    type Ctx = V::Ctx;

    #[inline]
    fn truncate_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        // debug_assert!(at > 0);
        // debug_assert!(at <= self.len());

        let remainder = self.1.truncate_ctx(at, ctx);
        KVPair(self.0 + at, remainder)
    }

    #[inline]
    fn truncate_keeping_right_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        // debug_assert!(at <= self.len());
        let old_key = self.0;
        self.0 += at;
        let trimmed = self.1.truncate_keeping_right_ctx(at, ctx);
        KVPair(old_key, trimmed)
    }
}

impl<V: MergableSpan + HasLength> MergableSpan for KVPair<V> {
    fn can_append(&self, other: &Self) -> bool {
        other.0 == self.end() && self.1.can_append(&other.1)
    }

    fn append(&mut self, other: Self) {
        self.1.append(other.1);
    }

    fn prepend(&mut self, other: Self) {
        self.1.prepend(other.1);
        self.0 = other.0;
    }
}

impl<V: Searchable> Searchable for KVPair<V> {
    type Item = V::Item;

    fn get_offset(&self, loc: Self::Item) -> Option<usize> { self.1.get_offset(loc) }
    fn at_offset(&self, offset: usize) -> Self::Item { self.1.at_offset(offset) }
}

impl<V: Default> Default for KVPair<V> {
    fn default() -> Self {
        KVPair(0, V::default())
    }
}

#[allow(unused)]
pub fn try_trim<V>(mut x: V, target_span: DTRange) -> Option<V>
    where V: HasRleKey + HasLength + SplitableSpan
{
    let x_span = x.span();
    if x_span.start < target_span.start {
        if x_span.end <= target_span.start { return None; }
        x.truncate_keeping_right(target_span.start - x_span.start);
    }

    if x_span.end > target_span.end {
        if x_span.start >= target_span.end { return None; }
        x.truncate(target_span.end - x_span.start);
    }

    Some(x)
}

#[allow(unused)]
pub fn trim<V>(val: V, span: DTRange) -> V
    where V: HasRleKey + HasLength + SplitableSpan
{
    try_trim(val, span).unwrap()
}

// pub fn intersect<A, B>(mut a: A, mut b: B) -> Option<(A, B)>
//     where A: HasKey + HasLength + SplitableSpan,
//           B: HasKey + HasLength + SplitableSpan
// {
//     let a_span = a.span();
//     let b_span = b.span();
//
//     if a.start <= b.start {
//         if a.end <= b.start { return None; }
//         a.truncate_keeping_right(b.start - a.start);
//     } else { // b.start < a.start
//         if b.end <= a.start { return None; }
//         b.truncate_keeping_right(a.start - b.start);
//     }
//
//     // And trim the end too.
//
//
//     Some((a, b))
// }


// #[cfg(test)]
// mod test {
//     use rle::test_splitable_methods_valid;
//
//     use crate::order::OrderSpan;
//     use crate::rle::KVPair;
//
//     #[test]
//     fn kvpair_valid() {
//         test_splitable_methods_valid(KVPair(10, OrderSpan {
//             order: 10,
//             len: 5
//         }));
//     }
// }