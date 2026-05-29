use std::fmt::Debug;

pub use append_rle::AppendRle;
pub use splitable_span::*;
pub use merge_iter::*;
use std::range::Range;
use std::ops::Range as LegacyRange;

mod splitable_span;
mod merge_iter;
mod append_rle;
pub mod zip;
pub mod take_max_iter;
pub mod intersect;
pub mod rlerun;
// mod gapbuffer;
// pub mod iter_ctx;

pub use rlerun::{RleRun, RleDRun};

pub trait Searchable {
    type Item: Copy + Debug;

    /// Checks if the entry contains the specified item. If it does, returns the offset into the
    /// item.
    fn get_offset(&self, loc: Self::Item) -> Option<usize>;

    // I'd use std Index for this but the index trait returns a reference.
    fn at_offset(&self, offset: usize) -> Self::Item;
}

pub trait HasRleKey {
    fn rle_key(&self) -> usize;
}

impl<T> HasRleKey for &T where T: HasRleKey {
    fn rle_key(&self) -> usize {
        (*self).rle_key()
    }
}

impl HasRleKey for Range<usize> {
    fn rle_key(&self) -> usize {
        self.start
    }
}

impl HasRleKey for Range<u32> {
    fn rle_key(&self) -> usize {
        self.start as _
    }
}

impl HasRleKey for LegacyRange<usize> {
    fn rle_key(&self) -> usize {
        self.start
    }
}

impl HasRleKey for LegacyRange<u32> {
    fn rle_key(&self) -> usize {
        self.start as _
    }
}

impl Searchable for Range<usize> {
    type Item = usize; // LV

    fn get_offset(&self, loc: Self::Item) -> Option<usize> {
        if loc >= self.start && loc < self.end {
            Some(loc - self.start)
        } else {
            None
        }
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.start + offset
    }
}

// This is sort of useful sometimes but ?? its a bit weird.
// impl HasRleKey for usize {
//     fn rle_key(&self) -> usize { *self }
// }