use crate::SplitableSpan;

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
    use crate::RleRun;

    #[test]
    fn test_merge_iter() {
        let empty: Vec<RleRun<u32>> = vec![];
        assert_eq!(merge_items(empty.into_iter()).collect::<Vec<_>>(), vec![]);

        let one = vec![RleRun { val: 5, len: 1 }];
        assert_eq!(merge_items(one.into_iter()).collect::<Vec<_>>(), vec![RleRun { val: 5, len: 1 }]);

        let two_split = vec![RleRun { val: 5, len: 1 }, RleRun { val: 15, len: 1 }];
        assert_eq!(merge_items(two_split.iter().copied()).collect::<Vec<_>>(), two_split);

        let two_merged = vec![RleRun { val: 5, len: 1 }, RleRun { val: 5, len: 2 }];
        assert_eq!(merge_items(two_merged.iter().copied()).collect::<Vec<_>>(), vec![RleRun { val: 5, len: 3 }]);
    }
}