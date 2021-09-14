use super::splitable_span::SplitableSpan;

/// This is an iterator composer which wraps any iterator over a SplitableSpan to become an
/// iterator over those same items in run-length order.

#[derive(Debug, Clone)]
pub struct MergeIter<I: Iterator> {
    next: Option<I::Item>,
    iter: I
}

pub fn merge_items<I: Iterator>(iter: I) -> MergeIter<I> {
    MergeIter::new(iter)
}

impl<I: Iterator> MergeIter<I> {
    pub fn new(iter: I) -> Self {
        Self {
            next: None,
            iter
        }
    }
}

impl<I, X> Iterator for MergeIter<I>
where
    I: Iterator<Item = X>,
    X: SplitableSpan
{
    type Item = X;

    fn next(&mut self) -> Option<Self::Item> {
        let mut this_val = match self.next.take() {
            Some(val) => val,
            None => {
                match self.iter.next() {
                    Some(val) => val,
                    None => { return None; }
                }
            }
        };

        while let Some(val) = self.iter.next() {
            if this_val.can_append(&val) {
                this_val.append(val);
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

pub trait MergeableIterator<X: SplitableSpan>: Iterator<Item = X> where Self: Sized {
    fn merge_spans(self) -> MergeIter<Self>;
}

impl<X, I> MergeableIterator<X> for I
where I: Iterator<Item=X>, X: SplitableSpan, Self: Sized
{
    fn merge_spans(self) -> MergeIter<Self> {
        MergeIter::new(self)
    }
}

#[cfg(test)]
mod test {
    use super::merge_items;
    use crate::splitable_span::SplitableSpan;

    /// Simple example where entries are runs of positive or negative items. This is used for testing
    /// and for the encoder.
    impl SplitableSpan for i32 {
        fn len(&self) -> usize {
            self.abs() as usize
        }

        fn truncate(&mut self, at: usize) -> Self {
            let at = at as i32;
            // dbg!(at, *self);
            debug_assert!(at > 0 && at < self.abs());
            debug_assert_ne!(*self, 0);

            let abs = self.abs();
            let sign = self.signum();
            *self = at * sign;

            (abs - at) * sign
        }

        fn can_append(&self, other: &Self) -> bool {
            (*self >= 0) == (*other >= 0)
        }

        fn append(&mut self, other: Self) {
            debug_assert!(self.can_append(&other));
            *self += other;
        }

        fn prepend(&mut self, other: Self) {
            self.append(other);
        }
    }

    #[test]
    fn test_merge_iter() {
        let empty: Vec<i32> = vec![];
        assert_eq!(merge_items(empty.into_iter()).collect::<Vec<_>>(), vec![]);

        let one: Vec<i32> = vec![5];
        assert_eq!(merge_items(one.into_iter()).collect::<Vec<_>>(), vec![5]);

        let two_split: Vec<i32> = vec![5, -10];
        assert_eq!(merge_items(two_split.iter().copied()).collect::<Vec<_>>(), two_split);

        let two_merged_1: Vec<i32> = vec![5, 15];
        assert_eq!(merge_items(two_merged_1.iter().copied()).collect::<Vec<_>>(), vec![20]);
        let two_merged_2: Vec<i32> = vec![-5, -15];
        assert_eq!(merge_items(two_merged_2.iter().copied()).collect::<Vec<_>>(), vec![-20]);
    }
}