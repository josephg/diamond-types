//! This file implements an iterator which can take up to n items at a time from a splitablespan
//! iterator.

use std::marker::PhantomData;
use crate::{HasLength, SplitableSpan};
use crate::iter_ctx::IteratorWithCtx;

#[derive(Clone, Debug)]
pub struct TakeMaxIter<'a, Iter, Item, Ctx>
    where Iter: IteratorWithCtx<'a, Item = Item, Ctx=Ctx>, Item: SplitableSpan + HasLength, Ctx: 'a
{
    iter: Iter,
    remainder: Option<Item>,
    _phantom: PhantomData<&'a Ctx> // Gross.
}

impl<'a, Iter, Item, Ctx> TakeMaxIter<'a, Iter, Item, Ctx>
    where Iter: IteratorWithCtx<'a, Item = Item, Ctx=Ctx>, Item: SplitableSpan + HasLength
{
    pub fn new(iter: Iter) -> Self {
        Self {
            iter,
            remainder: None,
            _phantom: Default::default()
        }
    }

    #[inline]
    pub fn next_with_ctx(&mut self, ctx: Ctx, max_size: usize) -> Option<Item> {
        let mut chunk = if let Some(r) = self.remainder.take() {
            r
        } else {
            if let Some(r) = self.iter.next_ctx(ctx) {
                r
            } else {
                return None;
            }
        };

        if chunk.len() > max_size {
            let new_remainder = chunk.truncate(max_size);
            self.remainder = Some(new_remainder);
        }

        Some(chunk)
    }
}

impl<'a, Iter, Item> TakeMaxIter<'a, Iter, Item, ()>
    where Iter: IteratorWithCtx<'a, Item = Item, Ctx=()>, Item: SplitableSpan + HasLength
{
    #[inline]
    pub fn next(&mut self, max_size: usize) -> Option<Item> {
        self.next_with_ctx((), max_size)
    }
}

pub trait TakeMaxFns<'a, I, Ctx>
    where Self: IteratorWithCtx<'a, Item = I, Ctx = Ctx> + Sized, I: SplitableSpan + HasLength
{
    fn take_max(self) -> TakeMaxIter<'a, Self, I, Ctx> {
        TakeMaxIter::new(self)
    }
}

impl<'a, Iter, Item, Ctx> TakeMaxFns<'a, Item, Ctx> for Iter
    where Iter: IteratorWithCtx<'a, Item = Item, Ctx = Ctx>, Item: SplitableSpan + HasLength {}

#[cfg(test)]
mod tests {
    use crate::RleRun;
    use crate::take_max_iter::TakeMaxFns;

    #[test]
    fn check_max_take_works() {
        let items = vec![RleRun { val: 5, len: 1 }, RleRun { val: 15, len: 7 }];

        let mut out = Vec::new();
        let mut iter = items.into_iter().take_max();
        while let Some(v) = iter.next(3) {
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