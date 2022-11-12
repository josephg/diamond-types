use crate::MergableSpan;

/// This is an iterator composer which wraps any iterator over a SplitableSpan to become an
/// iterator over those same items in run-length order.

#[derive(Debug, Clone)]
pub struct MergeIter<I: Iterator, const FWD: bool = true> {
    next: Option<I::Item>,
    iter: I
}

pub fn merge_items<I: Iterator>(iter: I) -> MergeIter<I, true> {
    MergeIter::new(iter)
}
pub fn merge_items_rev<I: Iterator>(iter: I) -> MergeIter<I, false> {
    MergeIter::new(iter)
}

impl<I: Iterator, const FWD: bool> MergeIter<I, FWD> {
    pub fn new(iter: I) -> Self {
        Self {
            next: None,
            iter
        }
    }
}

impl<I, X, const FWD: bool> Iterator for MergeIter<I, FWD>
where
    I: Iterator<Item = X>,
    X: MergableSpan
{
    type Item = X;

    fn next(&mut self) -> Option<Self::Item> {
        let mut this_val = match self.next.take() {
            Some(val) => val,
            None => {
                self.iter.next()?
            }
        };

        for val in &mut self.iter {
            if FWD && this_val.can_append(&val) {
                this_val.append(val);
            } else if !FWD && val.can_append(&this_val) {
                this_val.prepend(val);
            } else {
                self.next = Some(val);
                break;
            }
        }

        Some(this_val)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.iter.size_hint();
        (lower.min(1), upper)
    }
}

pub trait MergeableIterator<X: MergableSpan>: Iterator<Item = X> where Self: Sized {
    fn merge_spans(self) -> MergeIter<Self, true>;
    fn merge_spans_rev(self) -> MergeIter<Self, false>;
}

impl<X, I> MergeableIterator<X> for I
where I: Iterator<Item=X>, X: MergableSpan, Self: Sized
{
    fn merge_spans(self) -> MergeIter<Self> {
        MergeIter::new(self)
    }
    fn merge_spans_rev(self) -> MergeIter<Self, false> {
        MergeIter::new(self)
    }
}

#[cfg(test)]
mod test {
    use std::ops::Range;
    use super::merge_items;
    use crate::{merge_items_rev, RleRun};

    #[test]
    fn test_merge_iter() {
        let empty: Vec<RleRun<u32>> = vec![];
        assert_eq!(merge_items(empty.into_iter()).collect::<Vec<_>>(), vec![]);

        let one = vec![RleRun { val: 5, len: 1 }];
        assert_eq!(merge_items(one.into_iter()).collect::<Vec<_>>(), vec![RleRun { val: 5, len: 1 }]);

        let two_split = vec![2..3, 5..10];
        assert_eq!(merge_items(two_split.iter().cloned()).collect::<Vec<_>>(), two_split);

        let two_merged = vec![2..5, 5..10];
        assert_eq!(merge_items(two_merged.iter().cloned()).collect::<Vec<_>>(), vec![2..10]);
    }

    #[test]
    fn test_merge_iter_rev() {
        // TODO: This is a bit of a crap test because it doesn't actually
        let empty: Vec<Range<u32>> = vec![];
        assert_eq!(merge_items_rev(empty.into_iter()).collect::<Vec<_>>(), vec![]);

        let one = vec![5..6];
        assert_eq!(merge_items_rev(one.into_iter()).collect::<Vec<_>>(), vec![5..6]);

        let two_split = vec![5..10, 2..3];
        assert_eq!(merge_items_rev(two_split.iter().cloned()).collect::<Vec<_>>(), two_split);

        let two_merged = vec![5..10, 2..5];
        assert_eq!(merge_items_rev(two_merged.iter().cloned()).collect::<Vec<_>>(), vec![2..10]);
    }
}