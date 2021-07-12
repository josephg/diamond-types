use crate::range_tree::{EntryTraits, EntryWithContent};
use crate::common::ItemCount;
use std::fmt::Debug;
use std::ops::{AddAssign, SubAssign};

/// The index describes which fields we're tracking, and can query. Indexes let us convert
/// cursors to positions and vice versa.

pub trait TreeIndex<E: EntryTraits> where Self: Debug + Copy + Clone + PartialEq + Eq {
    type IndexUpdate: Debug + Default + PartialEq + Eq;
    type IndexOffset: Copy + Clone + Default + Debug + AddAssign + SubAssign + PartialEq + Eq + Sized;

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E);
    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E);

    fn update_offset_by_marker(offset: &mut Self::IndexOffset, by: &Self::IndexUpdate);

    fn increment_offset(offset: &mut Self::IndexOffset, by: &E);

    // This is actually unnecessary - it would be more correct to call truncate().content_len()
    // or whatever. TODO: Check if this actually makes any performance difference.
    fn increment_offset_partial(offset: &mut Self::IndexOffset, by: &E, at: usize) {
        let mut e = *by;
        if e.len() < at { e.truncate(at); }
        Self::increment_offset(offset, &e);
        // *offset += by.content_len().min(at) as u32;
    }

    fn can_count_items() -> bool { false }
    fn count_items(idx: Self::IndexOffset) -> usize { panic!("Index cannot count items") }
}

/// Content index - which just indexes based on the resulting size. Deletes are not counted.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContentIndex;

impl<E: EntryTraits + EntryWithContent> TreeIndex<E> for ContentIndex {
    type IndexUpdate = isize;
    type IndexOffset = ItemCount;

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker += entry.content_len() as isize;
    }

    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker -= entry.content_len() as isize;
        // dbg!(&marker, entry);
    }

    fn update_offset_by_marker(offset: &mut Self::IndexOffset, by: &Self::IndexUpdate) {
        // :( I wish there were a better way to do this.
        *offset = offset.wrapping_add(*by as u32);
    }

    fn increment_offset(offset: &mut Self::IndexOffset, by: &E) {
        *offset += by.content_len() as u32;
    }

    // This is unnecessary.
    fn increment_offset_partial(offset: &mut Self::IndexOffset, by: &E, at: usize) {
        *offset += by.content_len().min(at) as u32;
    }
}

/// Index based on the raw position of an element
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RawPositionIndex;

impl<E: EntryTraits> TreeIndex<E> for RawPositionIndex {
    type IndexUpdate = isize;
    type IndexOffset = ItemCount;

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker += entry.len() as isize;
    }

    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker -= entry.len() as isize;
        // dbg!(&marker, entry);
    }

    fn update_offset_by_marker(offset: &mut Self::IndexOffset, by: &Self::IndexUpdate) {
        // :( I wish there were a better way to do this.
        *offset = offset.wrapping_add(*by as u32);
    }

    fn increment_offset(offset: &mut Self::IndexOffset, by: &E) {
        *offset += by.len() as u32;
    }

    // This is unnecessary.
    fn increment_offset_partial(offset: &mut Self::IndexOffset, by: &E, at: usize) {
        *offset += by.len().min(at) as u32;
    }

    fn can_count_items() -> bool { true }
    fn count_items(idx: Self::IndexOffset) -> usize { idx as usize }
}

/// Index based on both resulting size and raw insert position
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FullIndex;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct FullMarker {
    pub len: i32,
    pub content: i32
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct FullOffset {
    pub len: ItemCount, // Number of items ever inserted
    pub content: ItemCount, // Number of items not currently deleted
}

impl AddAssign for FullOffset {
    fn add_assign(&mut self, rhs: Self) {
        self.len += rhs.len;
        self.content += rhs.content;
    }
}

impl SubAssign for FullOffset {
    fn sub_assign(&mut self, rhs: Self) {
        self.len -= rhs.len;
        self.content -= rhs.content;
    }
}

impl<E: EntryTraits + EntryWithContent> TreeIndex<E> for FullIndex {
    type IndexUpdate = FullMarker;
    type IndexOffset = FullOffset;

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        marker.len += entry.len() as i32;
        marker.content += entry.content_len() as i32;
    }

    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        marker.len -= entry.len() as i32;
        marker.content -= entry.content_len() as i32;
    }

    fn update_offset_by_marker(offset: &mut Self::IndexOffset, by: &Self::IndexUpdate) {
        offset.len = offset.len.wrapping_add(by.len as u32);
        offset.content = offset.content.wrapping_add(by.content as u32);
    }

    fn increment_offset(offset: &mut Self::IndexOffset, entry: &E) {
        offset.len += entry.len() as u32;
        offset.content += entry.content_len() as u32;
    }

    fn increment_offset_partial(offset: &mut Self::IndexOffset, by: &E, at: usize) {
        offset.len += at as u32;
        offset.content += by.content_len().min(at) as u32;
    }
}