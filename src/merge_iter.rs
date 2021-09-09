use crate::splitable_span::SplitableSpan;

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
    use crate::order::OrderSpan;
    use crate::merge_iter::merge_items;

    #[test]
    fn test_merge_iter() {
        let empty: Vec<OrderSpan> = vec![];
        assert_eq!(merge_items(empty.into_iter()).collect::<Vec<_>>(), vec![]);

        let one: Vec<OrderSpan> = vec![OrderSpan { order: 5, len: 10 }];
        assert_eq!(merge_items(one.into_iter()).collect::<Vec<_>>(), vec![OrderSpan { order: 5, len: 10 }]);

        let two_split: Vec<OrderSpan> = vec![
            OrderSpan { order: 5, len: 10 },
            OrderSpan { order: 105, len: 10 },
        ];
        assert_eq!(merge_items(two_split.iter().copied()).collect::<Vec<_>>(), two_split);

        let two_merged: Vec<OrderSpan> = vec![
            OrderSpan { order: 5, len: 10 },
            OrderSpan { order: 15, len: 10 },
        ];
        assert_eq!(merge_items(two_merged.iter().copied()).collect::<Vec<_>>(), vec![
            OrderSpan { order: 5, len: 20 },
        ]);
    }
}