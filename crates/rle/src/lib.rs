use std::fmt::Debug;

pub use append_rle::AppendRle;
pub use splitable_span::*;
pub use merge_iter::*;

mod splitable_span;
mod merge_iter;
mod append_rle;
pub mod zip;
pub mod take_max_iter;
// pub mod iter_ctx;

pub trait Searchable {
    type Item: Copy + Debug;

    // This is strictly unnecessary given truncate(), but it makes some code cleaner.
    // fn truncate_keeping_right(&mut self, at: usize) -> Self;

    /// Checks if the entry contains the specified item. If it does, returns the offset into the
    /// item.
    fn get_offset(&self, loc: Self::Item) -> Option<usize>;

    // I'd use Index for this but the index trait returns a reference.
    // fn at_offset(&self, offset: usize) -> Self::Item;
    fn at_offset(&self, offset: usize) -> Self::Item;
}
