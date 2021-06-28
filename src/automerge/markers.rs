use crate::splitable_span::SplitableSpan;
use std::ptr::NonNull;
use crate::range_tree::{NodeLeaf, EntryTraits, TreeIndex};
use std::ops::Index;
use std::fmt::Debug;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct MarkerEntry<E: EntryTraits, I: TreeIndex<E>> {
    // The order / seq is implicit from the location in the list.
    pub len: u32,
    pub ptr: NonNull<NodeLeaf<E, I>>
}

impl<E: EntryTraits, I: TreeIndex<E>> SplitableSpan for MarkerEntry<E, I> {
    // type Item = NonNull<NodeLeaf>;

    fn len(&self) -> usize {
        self.len as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder_len = self.len - at as u32;
        self.len = at as u32;
        MarkerEntry {
            len: remainder_len,
            ptr: self.ptr
        }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) { self.len += other.len; }
}

impl<E: EntryTraits, I: TreeIndex<E>> Index<usize> for MarkerEntry<E, I> {
    type Output = NonNull<NodeLeaf<E, I>>;

    fn index(&self, _index: usize) -> &Self::Output {
        &self.ptr
    }
}



impl<E: EntryTraits, I: TreeIndex<E>> Default for MarkerEntry<E, I> {
    fn default() -> Self {
        MarkerEntry {ptr: NonNull::dangling(), len: 0}
    }
}

impl<E: EntryTraits, I: TreeIndex<E>> EntryTraits for MarkerEntry<E, I> {
    type Item = NonNull<NodeLeaf<E, I>>;

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let left = Self {
            len: at as _,
            ptr: self.ptr
        };
        self.len -= at as u32;
        left
    }

    fn contains(&self, _loc: Self::Item) -> Option<usize> {
        panic!("Should never be used")
        // if self.ptr == loc { Some(0) } else { None }
    }

    fn is_valid(&self) -> bool {
        // TODO: Replace this with a real nullptr.
        self.ptr != NonNull::dangling()
    }

    fn at_offset(&self, _offset: usize) -> Self::Item {
        self.ptr
    }
}