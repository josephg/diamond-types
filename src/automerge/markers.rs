use crate::splitable_span::SplitableSpan;
use std::ptr::NonNull;
use crate::range_tree::{NodeLeaf, EntryTraits, FullIndex, TreeIndex};
use std::ops::Index;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct MarkerEntry<E: EntryTraits, I: TreeIndex<E>> {
    // The order / seq is implicit from the location inthe list.
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
        return MarkerEntry {
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
