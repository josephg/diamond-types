use smallvec::SmallVec;

use crate::splitable_span::SplitableSpan;

pub trait AppendRLE<T: SplitableSpan> {
    fn push_rle(&mut self, item: T);
    fn push_reversed_rle(&mut self, item: T);
}

// Apparently the cleanest way to do this DRY is using macros.
impl<T: SplitableSpan> AppendRLE<T> for Vec<T> {
    fn push_rle(&mut self, item: T) {
        if let Some(v) = self.last_mut() {
            if v.can_append(&item) {
                v.append(item);
                return;
            }
        }

        self.push(item);
    }

    fn push_reversed_rle(&mut self, item: T) {
        if let Some(v) = self.last_mut() {
            if item.can_append(v) {
                v.prepend(item);
                return;
            }
        }

        self.push(item);
    }
}

impl<A: smallvec::Array> AppendRLE<A::Item> for SmallVec<A> where A::Item: SplitableSpan {
    fn push_rle(&mut self, item: A::Item) {
        debug_assert!(item.len() > 0);

        if let Some(v) = self.last_mut() {
            if v.can_append(&item) {
                v.append(item);
                return;
            }
        }

        self.push(item);
    }

    fn push_reversed_rle(&mut self, item: A::Item) {
        debug_assert!(item.len() > 0);

        if let Some(v) = self.last_mut() {
            if item.can_append(v) {
                v.prepend(item);
                return;
            }
        }

        self.push(item);
    }
}
