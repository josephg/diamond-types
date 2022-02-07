//! This file implements an iterator which can take up to n items at a time from a splitablespan
//! iterator.

use crate::{HasLength, SplitableSpan};

#[derive(Clone, Debug)]
pub struct TakeMaxIter<Iter, Item>
    where Iter: Iterator<Item = Item>, Item: SplitableSpan + HasLength
{
    iter: Iter,
    remainder: Option<Item>
}

impl<Iter, Item> TakeMaxIter<Iter, Item>
    where Iter: Iterator<Item = Item>, Item: SplitableSpan + HasLength
{
    pub fn new(iter: Iter) -> Self {
        Self {
            iter,
            remainder: None
        }
    }

    #[inline]
    pub fn next(&mut self, max_size: usize) -> Option<Item> {
        let mut chunk = if let Some(r) = self.remainder.take() {
            r
        } else {
            if let Some(r) = self.iter.next() {
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

pub trait TakeMaxFns<I>
    where Self: Iterator<Item = I> + Sized, I: SplitableSpan + HasLength
{
    fn take_max(self) -> TakeMaxIter<Self, I> {
        TakeMaxIter::new(self)
    }
}

impl<Iter, Item> TakeMaxFns<Item> for Iter
    where Iter: Iterator<Item = Item>, Item: SplitableSpan + HasLength {}

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