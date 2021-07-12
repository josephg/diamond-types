
pub type RleKey = u32;

mod simple_rle;
// mod mutable_rle;

pub use simple_rle::Rle;
use crate::splitable_span::SplitableSpan;
use crate::range_tree::EntryTraits;
use std::fmt::Debug;
// pub use mutable_rle::MutRle;

pub trait RleKeyed {
    fn get_rle_key(&self) -> RleKey;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RlePair<V>(pub RleKey, pub V);

impl<V> RleKeyed for RlePair<V> {
    fn get_rle_key(&self) -> u32 {
        self.0
    }
}

impl<V: SplitableSpan> SplitableSpan for RlePair<V> {
    fn len(&self) -> usize { self.1.len() }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder = self.1.truncate(at);
        RlePair(self.0 + at as u32, remainder)
    }

    fn can_append(&self, other: &Self) -> bool {
        other.0 == self.0 + self.1.len() as u32 && self.1.can_append(&other.1)
    }

    fn append(&mut self, other: Self) {
        self.1.append(other.1);
    }

    fn prepend(&mut self, other: Self) {
        self.1.prepend(other.1);
        self.0 = other.0;
    }
}

impl<V: EntryTraits> EntryTraits for RlePair<V> {
    type Item = V::Item;

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let old_key = self.0;
        self.0 += at as u32;
        let trimmed = self.1.truncate_keeping_right(at);
        RlePair(old_key, trimmed)
    }

    fn contains(&self, loc: Self::Item) -> Option<usize> { self.1.contains(loc) }
    fn is_valid(&self) -> bool { self.1.is_valid() }
    fn at_offset(&self, offset: usize) -> Self::Item { self.1.at_offset(offset) }
}

impl<V: Default> Default for RlePair<V> {
    fn default() -> Self {
        RlePair(0, V::default())
    }
}