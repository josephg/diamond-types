use crate::{HasRleKey, HasLength, SplitableSpan};
use crate::zip::Remainder;

/// An intersection iterator returns the sub-items which are contained in both A and B. 
#[derive(Debug, Clone)]
pub struct RleIntersect<A, B, const FWD: bool = true> where A: Iterator, B: Iterator
{
    rem: Remainder<A::Item, B::Item>,
    a: A,
    b: B,
}

impl<A, B> RleIntersect<A, B, true> where A: Iterator, B: Iterator {
    pub fn new_fwd(a: A, b: B) -> Self {
        Self {
            rem: Default::default(),
            a, b
        }
    }
}
impl<A, B> RleIntersect<A, B, false> where A: Iterator, B: Iterator {
    pub fn new_rev(a: A, b: B) -> Self {
        Self {
            rem: Default::default(),
            a, b
        }
    }
}

impl<A, B> Iterator for RleIntersect<A, B, true> where A: Iterator, B: Iterator,
    A::Item: SplitableSpan + HasLength + HasRleKey,
    B::Item: SplitableSpan + HasLength + HasRleKey
{
    type Item = (A::Item, B::Item);

    fn next(&mut self) -> Option<Self::Item> {
        // If b is None here, we'll discard the a item, but the iterator will only produce None
        // from here anyway so its not a big deal.
        let (mut a, mut b) = self.rem.take_from_iter(&mut self.a, &mut self.b)?;

        loop {
            let a_key = a.rle_key();
            let b_key = b.rle_key();

            if a_key >= b_key + b.len() {
                // This could be further optimized, but its not a big deal here.
                b = self.b.next()?;
                continue;
            }
            if b_key >= a_key + a.len() {
                a = self.a.next()?;
                continue;
            }

            // Ok, they have some intersection.
            if a_key > b_key {
                b.truncate_keeping_right(a_key - b_key);
            } else if b_key > a_key {
                a.truncate_keeping_right(b_key - a_key);
            }

            if b.len() > a.len() {
                let rem = b.truncate(a.len());
                self.rem = Remainder::SomeB(rem);
            } else if a.len() > b.len() {
                let rem = a.truncate(b.len());
                self.rem = Remainder::SomeA(rem);
            } // Else the remainder should be nothing, but that should happen anyway.

            return Some((a, b));
        }
    }
}

// For backwards items.
impl<A, B> Iterator for RleIntersect<A, B, false> where A: Iterator, B: Iterator,
    A::Item: SplitableSpan + HasLength + HasRleKey,
    B::Item: SplitableSpan + HasLength + HasRleKey
{
    type Item = (A::Item, B::Item);

    fn next(&mut self) -> Option<Self::Item> {
        // If b is None here, we'll discard the a item, but the iterator will only produce None
        // from here anyway so its not a big deal.
        let (mut a, mut b) = self.rem.take_from_iter(&mut self.a, &mut self.b)?;

        loop {
            let a_key = a.rle_key();
            let b_key = b.rle_key();

            if a_key >= b_key + b.len() {
                a = self.a.next()?;
                continue;
            }
            if b_key >= a_key + a.len() {
                b = self.b.next()?;
                continue;
            }

            // Ok, they have some intersection.
            if a_key > b_key {
                let rem = b.truncate_keeping_right(a_key - b_key);
                self.rem = Remainder::SomeB(rem);
            } else if b_key > a_key {
                let rem = a.truncate_keeping_right(b_key - a_key);
                self.rem = Remainder::SomeA(rem);
            }

            if b.len() > a.len() {
                b.truncate(a.len());
            } else if a.len() > b.len() {
                a.truncate(b.len());
            }

            return Some((a, b));
        }
    }
}

pub fn rle_intersect<A, B>(a: A, b: B) -> RleIntersect<A, B>
    where A: Iterator, B: Iterator,
          A::Item: SplitableSpan + HasLength + HasRleKey,
          B::Item: SplitableSpan + HasLength + HasRleKey
{
    RleIntersect::new_fwd(a, b)
}

pub fn rle_intersect_first<A, B>(a: A, b: B) -> impl Iterator<Item = A::Item>
    where A: Iterator, B: Iterator,
          A::Item: SplitableSpan + HasLength + HasRleKey,
          B::Item: SplitableSpan + HasLength + HasRleKey
{
    RleIntersect::new_fwd(a, b).map(|(a, _)| a)
}

pub fn rle_intersect_rev<A, B>(a: A, b: B) -> RleIntersect<A, B, false>
    where A: Iterator, B: Iterator,
          A::Item: SplitableSpan + HasLength + HasRleKey,
          B::Item: SplitableSpan + HasLength + HasRleKey
{
    RleIntersect::new_rev(a, b)
}

pub fn rle_intersect_rev_first<A, B>(a: A, b: B) -> impl Iterator<Item = A::Item>
    where A: Iterator, B: Iterator,
          A::Item: SplitableSpan + HasLength + HasRleKey,
          B::Item: SplitableSpan + HasLength + HasRleKey
{
    RleIntersect::new_rev(a, b).map(|(a, _)| a)
}

#[cfg(test)]
mod test {
    use std::ops::Range;
    use crate::intersect::*;

    fn dup(a: &[Range<u32>]) -> Vec<(Range<u32>, Range<u32>)> {
        a.iter().map(|r| (r.clone(), r.clone())).collect::<Vec<_>>()
    }

    fn test_intersect(a: &[Range<u32>], b: &[Range<u32>], expect: &[Range<u32>]) {
        let result1: Vec<_> = rle_intersect(a.iter().cloned(), b.iter().cloned()).collect();
        // Swapped
        let result2: Vec<_> = rle_intersect(b.iter().cloned(), a.iter().cloned()).collect();

        // The result is repeated here because we get an entry from both a and b.
        let expect_dup = dup(expect);
        assert_eq!(result1, expect_dup);
        assert_eq!(result2, expect_dup);

        // Also an item crossed with itself should produce itself.
        let cloned_a: Vec<_> = rle_intersect(a.iter().cloned(), a.iter().cloned()).collect();
        assert_eq!(cloned_a, dup(a));
        let cloned_b: Vec<_> = rle_intersect(b.iter().cloned(), b.iter().cloned()).collect();
        assert_eq!(cloned_b, dup(b));

        let mut result_rev1: Vec<_> = rle_intersect_rev(a.iter().rev().cloned(), b.iter().rev().cloned())
            .collect();
        result_rev1.reverse();
        assert_eq!(result_rev1, expect_dup);

        let mut result_rev2: Vec<_> = rle_intersect_rev(b.iter().rev().cloned(), a.iter().rev().cloned())
            .collect();
        result_rev2.reverse();
        assert_eq!(result_rev2, expect_dup);
    }

    #[test]
    fn intersect_smoke() {
        test_intersect(&[0..5, 10..20], &[3..15], &[3..5, 10..15]);
        test_intersect(&[0..5, 10..20], &[10..20], &[10..20]);
        test_intersect(&[0..5], &[10..20], &[]);
        test_intersect(&[0..5], &[5..10], &[]);
        test_intersect(&[0..20], &[5..10], &[5..10]);
    }

    #[test]
    fn intersect_with_empty() {
        test_intersect(&[], &[0..100], &[]);
        test_intersect(&[], &[], &[]);
    }
}