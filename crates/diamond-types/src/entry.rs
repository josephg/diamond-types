use std::fmt::Debug;

use rle::splitable_span::SplitableSpan;

// TODO: Consider renaming this "RangeEntry" or something.
pub trait EntryTraits: SplitableSpan + Copy + Debug + Default {}
impl<T: SplitableSpan + Copy + Debug + Default> EntryTraits for T {}

pub trait Searchable {
    type Item: Copy + Debug;

    // This is strictly unnecessary given truncate(), but it makes some code cleaner.
    // fn truncate_keeping_right(&mut self, at: usize) -> Self;

    /// Checks if the entry contains the specified item. If it does, returns the offset into the
    /// item.
    fn contains(&self, loc: Self::Item) -> Option<usize>;

    // I'd use Index for this but the index trait returns a reference.
    // fn at_offset(&self, offset: usize) -> Self::Item;
    fn at_offset(&self, offset: usize) -> Self::Item;
}

pub trait ContentLength {
    /// User specific content length. Used by content_tree for character counts.
    fn content_len(&self) -> usize;
}

// impl<T: EntryTraits + Searchable> IndexGet<usize> for T {
//     type Output = T::Item;
//
//     fn index_get(&self, index: usize) -> Self::Output {
//         self.at_offset(index)
//     }
// }

/// This trait marks items as being able to toggle on and off. The motivation for this is CRDT
/// items which want to stay in a list even after they've been deleted.
pub trait Toggleable {
    fn is_activated(&self) -> bool;
    fn is_deactivated(&self) -> bool {
        !self.is_activated()
    }
    fn mark_activated(&mut self);
    fn mark_deactivated(&mut self);
}
