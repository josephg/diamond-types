
pub type RleKey = u32;

mod simple_rle;

pub use simple_rle::Rle;
use rle::splitable_span::SplitableSpan;
use std::fmt::Debug;
use smallvec::SmallVec;
use crate::entry::Searchable;

pub trait RleKeyed {
    fn get_rle_key(&self) -> RleKey;
}

pub trait RleSpanHelpers: RleKeyed + SplitableSpan {
    fn end(&self) -> u32 {
        self.get_rle_key() + self.len() as u32
    }
}

impl<V: RleKeyed + SplitableSpan> RleSpanHelpers for V {}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct KVPair<V>(pub RleKey, pub V);

impl<V> RleKeyed for KVPair<V> {
    fn get_rle_key(&self) -> u32 {
        self.0
    }
}

impl<V: SplitableSpan> SplitableSpan for KVPair<V> {
    fn len(&self) -> usize { self.1.len() }

    fn truncate(&mut self, at: usize) -> Self {
        debug_assert!(at > 0);
        debug_assert!(at < self.1.len());

        let remainder = self.1.truncate(at);
        KVPair(self.0 + at as u32, remainder)
    }

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let old_key = self.0;
        self.0 += at as u32;
        let trimmed = self.1.truncate_keeping_right(at);
        KVPair(old_key, trimmed)
    }

    fn can_append(&self, other: &Self) -> bool {
        other.0 == self.end() && self.1.can_append(&other.1)
    }

    fn append(&mut self, other: Self) {
        self.1.append(other.1);
    }

    fn prepend(&mut self, other: Self) {
        self.1.prepend(other.1);
        self.0 = other.0;
    }
}

impl<V: Searchable> Searchable for KVPair<V> {
    type Item = V::Item;

    fn contains(&self, loc: Self::Item) -> Option<usize> { self.1.contains(loc) }
    fn at_offset(&self, offset: usize) -> Self::Item { self.1.at_offset(offset) }
}

impl<V: Default> Default for KVPair<V> {
    fn default() -> Self {
        KVPair(0, V::default())
    }
}


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

#[cfg(test)]
mod test {
    use rle::splitable_span::test_splitable_methods_valid;
    use crate::rle::KVPair;
    use crate::order::OrderSpan;

    #[test]
    fn kvpair_valid() {
        test_splitable_methods_valid(KVPair(10, OrderSpan {
            order: 10,
            len: 5
        }));
    }
}