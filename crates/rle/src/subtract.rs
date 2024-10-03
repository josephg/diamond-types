use std::cmp::Ordering;
use crate::{HasRleKey, HasLength, SplitableSpan};
use crate::zip::Remainder;

/// A subtraction iterator subtracts A - B and yields all the item in A which *do not* appear in B.
#[derive(Debug, Clone)]
pub struct RleSubtract<A, B> where A: Iterator, B: Iterator
{
    rem: Remainder<A::Item, B::Item>,
    a: A,
    b: B,
}

fn trim_heads<A, B>(mut a: A, mut b: B) -> Remainder<A, B>
where A: SplitableSpan + HasLength + HasRleKey,
      B: SplitableSpan + HasLength + HasRleKey
{
    debug_assert_eq!(a.rle_key(), b.rle_key());

    match a.len().cmp(&b.len()) {
        Ordering::Less => {
            // A is shorter than B.
            b.truncate_keeping_right(a.len());
            Remainder::SomeB(b)
        }
        Ordering::Greater => {
            // B is shorter than A.
            a.truncate_keeping_right(b.len());
            Remainder::SomeA(a)
        }
        Ordering::Equal => Remainder::Nothing,
    }
}

impl<A, B> Iterator for RleSubtract<A, B>
    where A: Iterator, B: Iterator,
          A::Item: SplitableSpan + HasLength + HasRleKey,
          B::Item: SplitableSpan + HasLength + HasRleKey
{
    type Item = A::Item;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            let (a, b) = self.rem.take_from_either(&mut self.a, &mut self.b);

            // If b is exhausted, we just return all remaining items in a.
            let Some(mut b) = b else { return a; };

            // If a is exhausted, we're done. It doesn't matter what's in b.
            let Some(mut a) = a else { return None; };

            loop {
                // So now we have an a and a b.
                let a_key = a.rle_key();
                let b_key = b.rle_key();

                if a_key >= b_key + b.len() {
                    // Skip this b.
                    if let Some(bb) = self.b.next() {
                        // Use this b and continue.
                        b = bb;
                        continue;
                    } else {
                        // b iterator exhausted. Yield remaining a values.
                        return Some(a);
                    }
                }

                if b_key >= a_key + a.len() {
                    // Save b for later and yield a.
                    self.rem = Remainder::SomeB(b);
                    return Some(a);
                }

                // The keys overlap in some way.
                if a_key < b_key {
                    // Take and return the first part of a.
                    let a_rem = a.truncate(b_key - a_key);
                    self.rem = trim_heads(a_rem, b);
                    return Some(a);
                } else if a_key > b_key {
                    b.truncate_keeping_right(a_key - b_key);
                }

                // This probably won't generate optimal code, but eh.
                self.rem = trim_heads(a, b);
                continue 'outer;
            }
        }
    }
}

pub fn rle_subtract<A, B>(a: A, b: B) -> RleSubtract<A, B>
    where A: Iterator, B: Iterator,
          A::Item: SplitableSpan + HasLength + HasRleKey,
          B::Item: SplitableSpan + HasLength + HasRleKey
{
    RleSubtract { rem: Remainder::Nothing, a, b }
}

#[cfg(test)]
mod test {
    use std::ops::Range;
    use super::*;

    fn test_subtract(a: &[Range<u32>], b: &[Range<u32>], expect_ab: &[Range<u32>], expect_ba: &[Range<u32>]) {
        let r1: Vec<_> = rle_subtract(a.iter().cloned(), b.iter().cloned())
            .collect();
        assert_eq!(&r1, expect_ab);

        let r2: Vec<_> = rle_subtract(b.iter().cloned(), a.iter().cloned())
            .collect();
        assert_eq!(&r2, expect_ba);

        // An item minus itself should be nothing.
        let a_minus_a: Vec<_> = rle_subtract(a.iter().cloned(), a.iter().cloned())
            .collect();
        assert_eq!(a_minus_a, vec![]);

        let b_minus_b: Vec<_> = rle_subtract(b.iter().cloned(), b.iter().cloned())
            .collect();
        assert_eq!(b_minus_b, vec![]);
    }

    #[test]
    fn subtract_smoke() {
        test_subtract(&[], &[], &[], &[]);
        test_subtract(&[0..5], &[], &[0..5], &[]);
        test_subtract(&[0..5], &[0..10], &[], &[5..10]);
        test_subtract(&[0..30], &[10..20], &[0..10, 20..30], &[]);
        test_subtract(&[0..10, 20..30], &[5..25], &[0..5, 25..30], &[10..20]);
        test_subtract(&[0..10, 20..30], &[5..10, 20..25], &[0..5, 25..30], &[]);
    }
}