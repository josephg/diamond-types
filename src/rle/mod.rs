
pub type RleKey = u32;

mod simple_rle;

pub use simple_rle::Rle;
use crate::splitable_span::SplitableSpan;
use crate::range_tree::EntryTraits;
use std::fmt::Debug;
use smallvec::SmallVec;

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

// impl<V: SplitableSpan> KVPair<V> {
//     pub fn end(&self) -> u32 {
//         self.0 + self.1.len() as u32
//     }
// }

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

impl<V: EntryTraits> EntryTraits for KVPair<V> {
    type Item = V::Item;

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let old_key = self.0;
        self.0 += at as u32;
        let trimmed = self.1.truncate_keeping_right(at);
        KVPair(old_key, trimmed)
    }

    fn contains(&self, loc: Self::Item) -> Option<usize> { self.1.contains(loc) }
    fn is_valid(&self) -> bool { self.1.is_valid() }
    fn at_offset(&self, offset: usize) -> Self::Item { self.1.at_offset(offset) }
}

impl<V: Default> Default for KVPair<V> {
    fn default() -> Self {
        KVPair(0, V::default())
    }
}


pub trait AppendRLE<T: SplitableSpan> {
    fn append_rle(&mut self, item: T);
    fn append_reversed_rle(&mut self, item: T);
}

// Apparently the cleanest way to do this DRY is using macros.
impl<T: SplitableSpan> AppendRLE<T> for Vec<T> {
    fn append_rle(&mut self, item: T) {
        if let Some(v) = self.last_mut() {
            if v.can_append(&item) {
                v.append(item);
                return;
            }
        }

        self.push(item);
    }

    fn append_reversed_rle(&mut self, item: T) {
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
    fn append_rle(&mut self, item: A::Item) {
        if let Some(v) = self.last_mut() {
            if v.can_append(&item) {
                v.append(item);
                return;
            }
        }

        self.push(item);
    }

    fn append_reversed_rle(&mut self, item: A::Item) {
        if let Some(v) = self.last_mut() {
            if item.can_append(v) {
                v.prepend(item);
                return;
            }
        }

        self.push(item);
    }
}