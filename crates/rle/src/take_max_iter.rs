//! This file implements an iterator which can take up to n items at a time from a splitablespan
//! iterator.

use crate::{HasLength, SplitableSpanCtx};

#[derive(Debug, Clone, Default)]
pub struct Rem<T: SplitableSpanCtx + HasLength>(Option<T>);

impl<T: SplitableSpanCtx + HasLength> Rem<T> {
    pub fn new() -> Self {
        Self(None)
    }

    fn prime<F: FnOnce() -> Option<T>>(&mut self, f: F) {
        if self.0.is_none() {
            self.0 = f();
        }
    }

    fn take_max_primed(&mut self, max_size: usize, ctx: &T::Ctx) -> T {
        let mut r = self.0.take().unwrap();

        if r.len() > max_size {
            self.0 = Some(r.truncate_ctx(max_size, ctx));
        }

        r
    }

    pub fn take_max_opt<F: FnOnce() -> Option<T>>(&mut self, max_size: usize, f: F, ctx: &T::Ctx) -> Option<T> {
        let mut chunk = if let Some(r) = self.0.take() {
            r
        } else {
            f()?
        };

        if chunk.len() > max_size {
            let new_remainder = chunk.truncate_ctx(max_size, ctx);
            self.0 = Some(new_remainder);
        }

        Some(chunk)
    }

    pub fn take_max_result<E, F: FnOnce() -> Result<T, E>>(&mut self, max_size: usize, f: F, ctx: &T::Ctx) -> Result<T, E> {
        let mut chunk = if let Some(r) = self.0.take() {
            r
        } else {
            f()?
        };

        if chunk.len() > max_size {
            let new_remainder = chunk.truncate_ctx(max_size, ctx);
            self.0 = Some(new_remainder);
        }

        Ok(chunk)
    }
}

#[derive(Clone, Debug)]
pub struct TakeMaxIter<Iter, Item>
    where Iter: Iterator<Item = Item>, Item: SplitableSpanCtx + HasLength
{
    iter: Iter,
    remainder: Rem<Item>
}

impl<Iter, Item> TakeMaxIter<Iter, Item>
    where Iter: Iterator<Item = Item>, Item: SplitableSpanCtx + HasLength
{
    pub fn new(iter: Iter) -> Self {
        Self {
            iter,
            remainder: Rem::new(),
        }
    }

    #[inline]
    pub fn next_ctx(&mut self, max_size: usize, ctx: &Item::Ctx) -> Option<Item> {
        self.remainder.take_max_opt(max_size, || self.iter.next(), ctx)
    }

    /// Peek at the next item to be returned. Note this takes a &mut self because it may consume
    /// from the underlying iterator.
    pub fn peek(&mut self) -> Option<&Item> {
        self.remainder.prime(|| self.iter.next());
        self.remainder.0.as_ref()
    }

    // <Iter, Item> TakeMaxIter<Iter, Item>
    // where Iter: Iterator<Item = Item>, Item: SplitableSpan + HasLength
    pub fn zip_next<Iter2, Item2>(a: &mut Self, b: &mut TakeMaxIter<Iter2, Item2>, max_size: usize, ctx1: &Item::Ctx, ctx2: &Item2::Ctx) -> Option<(Item, Item2)>
        where Iter2: Iterator<Item = Item2>, Item2: SplitableSpanCtx + HasLength
    {
        a.remainder.prime(|| a.iter.next());
        b.remainder.prime(|| b.iter.next());

        match (a.remainder.0.as_mut(), b.remainder.0.as_mut()) {
            // TODO: This isn't very good error reporting. What should we do in this case?
            (_, None) => None,
            (None, _) => None,
            (Some(aa), Some(bb)) => {
                let len1 = aa.len();
                let len2 = bb.len();
                let take_here = max_size.min(len1).min(len2);

                Some((
                    a.remainder.take_max_primed(take_here, ctx1),
                    b.remainder.take_max_primed(take_here, ctx2)
                ))
            }
        }
    }
}

impl<Iter, Item> TakeMaxIter<Iter, Item>
    where Iter: Iterator<Item = Item>, Item: SplitableSpanCtx<Ctx=()> + HasLength
{
    #[inline]
    pub fn next(&mut self, max_size: usize) -> Option<Item> {
        self.remainder.take_max_opt(max_size, || self.iter.next(), &())
    }
}


// impl<Iter, Item> TakeMaxIter<Iter, Item, ()>
//     where Iter: Iterator<Item = Item, Ctx=()>, Item: SplitableSpan + HasLength
// {
//     #[inline]
//     pub fn next(&mut self, max_size: usize) -> Option<Item> {
//         self.next_ctx((), max_size)
//     }
// }

pub trait TakeMaxFns<I>
    where Self: Iterator<Item = I> + Sized, I: SplitableSpanCtx + HasLength
{
    fn take_max(self) -> TakeMaxIter<Self, I> {
        TakeMaxIter::new(self)
    }
}

impl<Iter, Item> TakeMaxFns<Item> for Iter
    where Iter: Iterator<Item = Item>, Item: SplitableSpanCtx + HasLength {}

#[cfg(test)]
mod tests {
    use crate::RleRun;
    use crate::take_max_iter::TakeMaxFns;

    #[test]
    fn check_max_take_works() {
        let items = vec![RleRun { val: 5, len: 1 }, RleRun { val: 15, len: 7 }];

        let mut out = Vec::new();
        let mut iter = items.into_iter().take_max();
        while let Some(v) = iter.next(3, &()) {
            out.push(v);
        }

        assert_eq!(&out, &[
            RleRun { val: 5, len: 1 },
            RleRun { val: 15, len: 3 },
            RleRun { val: 15, len: 3 },
            RleRun { val: 15, len: 1 },
        ]);
    }
}