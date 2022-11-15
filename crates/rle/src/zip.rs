use std::cmp::Ordering;
use std::mem::take;
use crate::{HasLength, SplitableSpan};

// Also used by intersect.
#[derive(Clone, Debug)]
pub(crate) enum Remainder<A, B> {
    Nothing,
    SomeA(A),
    SomeB(B),
}

impl<A, B> Default for Remainder<A, B> {
    fn default() -> Self { Remainder::Nothing }
}

impl<A, B> Remainder<A, B> {
    pub(crate) fn take_from_iter<AI, BI>(&mut self, ai: &mut AI, bi: &mut BI) -> Option<(A, B)>
        where AI: Iterator<Item=A>, BI: Iterator<Item=B>
    {
        Some(match take(self) {
            Remainder::Nothing => {
                // Fetch from both.
                let a = ai.next()?;
                let b = bi.next()?;
                (a, b)
            }
            Remainder::SomeA(a) => {
                let b = bi.next()?;
                (a, b)
            }
            Remainder::SomeB(b) => {
                let a = ai.next()?;
                (a, b)
            }
        })
    }
}

/// A RleZip is a zip iterator over 2 SplitableSpan iterators. Each item it yields contains the
/// longest readable span from each of A and B.
///
/// The iterator ends at the min of A and B.
#[derive(Clone, Debug)]
pub struct RleZip<A, B>
    where A: Iterator, B: Iterator,
          A::Item: SplitableSpan + HasLength, B::Item: SplitableSpan + HasLength,
{
    rem: Remainder<A::Item, B::Item>,
    a: A,
    b: B,
}

impl<A, B> Iterator for RleZip<A, B>
    where A: Iterator, B: Iterator,
          A::Item: SplitableSpan + HasLength, B::Item: SplitableSpan + HasLength
{
    type Item = (A::Item, B::Item);

    fn next(&mut self) -> Option<Self::Item> {
        let (mut a, mut b) = self.rem.take_from_iter(&mut self.a, &mut self.b)?;

        let a_len = a.len();
        let b_len = b.len();

        self.rem = match a_len.cmp(&b_len) {
            Ordering::Equal => {
                // Take all of both.
                Remainder::Nothing
            }
            Ordering::Less => {
                // a < b.
                let b_rem = b.truncate(a_len);
                Remainder::SomeB(b_rem)
            }
            Ordering::Greater => {
                // a > b.
                let a_rem = a.truncate(b_len);
                Remainder::SomeA(a_rem)
            }
        };

        Some((a, b))
    }
}

pub fn rle_zip<A, B>(a: A, b: B) -> RleZip<A, B>
    where A: Iterator, B: Iterator,
          A::Item: SplitableSpan + HasLength, B::Item: SplitableSpan + HasLength
{
    RleZip {
        rem: Remainder::Nothing,
        a,
        b
    }
}

pub fn rle_zip3<A, B, C>(a: A, b: B, c: C) -> impl Iterator<Item=(A::Item, B::Item, C::Item)>
    where A: Iterator, B: Iterator, C: Iterator,
          A::Item: SplitableSpan + HasLength,
          B::Item: SplitableSpan + HasLength,
          C::Item: SplitableSpan + HasLength,
{
    rle_zip(rle_zip(a, b), c).map(|((a, b), c)| (a, b, c))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::RleRun;

    fn check_zip(a: &[RleRun<u32>], b: &[RleRun<u32>], expect: &[(RleRun<u32>, RleRun<u32>)]) {
        assert_eq!(rle_zip(a.iter().copied(), b.iter().copied())
                       .collect::<Vec<_>>(), expect);

        // And check that if we swap the parameter order we get the same thing.
        assert_eq!(rle_zip(b.iter().copied(), a.iter().copied())
                       .map(|(a, b)| (b, a))
                       .collect::<Vec<_>>(), expect);
    }

    #[test]
    fn smoke() {
        let one = vec![
            RleRun { val: 1, len: 1 },
            RleRun { val: 2, len: 4 }
        ];
        let two = vec![
            RleRun { val: 11, len: 4 },
            RleRun { val: 12, len: 1 }
        ];

        let expected = vec![
            (RleRun { val: 1, len: 1 }, RleRun { val: 11, len: 1}),
            (RleRun { val: 2, len: 3 }, RleRun { val: 11, len: 3}),
            (RleRun { val: 2, len: 1 }, RleRun { val: 12, len: 1}),
        ];

        check_zip(&one, &two, &expected);
    }

    #[test]
    fn one_is_longer() {
        let one = vec![
            RleRun { val: 1, len: 100 },
        ];
        let two = vec![
            RleRun { val: 11, len: 10 },
        ];

        let expected = vec![
            (RleRun { val: 1, len: 10 }, RleRun { val: 11, len: 10}),
        ];

        check_zip(&one, &two, &expected);
    }
}