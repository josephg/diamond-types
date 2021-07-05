use crate::range_tree::{EntryTraits, EntryWithContent, AbsolutelyPositioned};
use crate::common::ItemCount;
use std::fmt::Debug;
use std::ops::{AddAssign, SubAssign};

/// The index describes which fields we're tracking, and can query. Indexes let us convert
/// cursors to positions and vice versa.

pub trait TreeIndex<E: EntryTraits> where Self: Debug + Copy + Clone + PartialEq + Eq {
    type IndexUpdate: Copy + Clone + Debug + PartialEq + Eq;
    type IndexEntry: Copy + Clone + Default + Debug + AddAssign + SubAssign + PartialEq + Eq + Sized;

    fn new_update() -> Self::IndexUpdate;
    fn update_is_needed(update: &Self::IndexUpdate) -> bool;
    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E);
    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E);

    fn update_index_by(offset: &mut Self::IndexEntry, by: &Self::IndexUpdate);

    fn increment_entry_by_size(offset: &mut Self::IndexEntry, by: &E);

    // This is actually unnecessary - it would be more correct to call truncate().content_len()
    // or whatever. TODO: Check if this actually makes any performance difference.
    fn increment_offset_partial(offset: &mut Self::IndexEntry, by: &E, at: usize) {
        let mut e = *by;
        if e.len() < at { e.truncate(at); }
        Self::increment_entry_by_size(offset, &e);
        // *offset += by.content_len().min(at) as u32;
    }

    fn set_pivot(marker: &mut Self::IndexUpdate, entry: &E);

    fn needs_span_update() -> bool;
}

// Marker traits.
// trait IndexWithSpans {}
// trait IndexWithPivots {}

/// Index like a traditional b-tree, using pivots naming the position at the start of each node
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AbsPositionIndex;
// impl IndexWithPivots for AbsPositionIndex {}

impl<E: EntryTraits + AbsolutelyPositioned> TreeIndex<E> for AbsPositionIndex {
    type IndexUpdate = u32; // u32::MAX to indicate no update needed. Option would be cleaner though.
    type IndexEntry = u32;

    fn new_update() -> Self::IndexUpdate { u32::MAX }
    fn update_is_needed(update: &Self::IndexUpdate) -> bool { *update != u32::MAX }

    fn increment_marker(_marker: &mut Self::IndexUpdate, _entry: &E) {}
    fn decrement_marker(_marker: &mut Self::IndexUpdate, _entry: &E) {}
    fn update_index_by(index: &mut Self::IndexEntry, update: &Self::IndexUpdate) {
        // debug_assert_ne!(*update, u32::MAX);
        if *update != u32::MAX {
            *index = *update;
        }
    }
    fn increment_entry_by_size(_offset: &mut Self::IndexEntry, _by: &E) {}

    fn set_pivot(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker = entry.pos();
    }

    // fn needs_pivot_update() -> bool { true }
    fn needs_span_update() -> bool { false }
}

/// Content index - which just indexes based on the resulting size. Deletes are not counted.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContentIndex;
// impl IndexWithSpans for ContentIndex {}

impl<E: EntryTraits + EntryWithContent> TreeIndex<E> for ContentIndex {
    type IndexUpdate = isize;
    type IndexEntry = ItemCount;

    fn new_update() -> Self::IndexUpdate { 0 }

    fn update_is_needed(update: &Self::IndexUpdate) -> bool { *update != 0 }

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker += entry.content_len() as isize;
    }

    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker -= entry.content_len() as isize;
        // dbg!(&marker, entry);
    }

    fn update_index_by(offset: &mut Self::IndexEntry, by: &Self::IndexUpdate) {
        // :( I wish there were a better way to do this.
        *offset = offset.wrapping_add(*by as u32);
    }

    fn increment_entry_by_size(offset: &mut Self::IndexEntry, by: &E) {
        *offset += by.content_len() as u32;
    }

    // This is unnecessary.
    fn increment_offset_partial(offset: &mut Self::IndexEntry, by: &E, at: usize) {
        *offset += by.content_len().min(at) as u32;
    }

    fn set_pivot(_marker: &mut Self::IndexUpdate, _entry: &E) {}

    // fn needs_pivot_update() -> bool { false }
    fn needs_span_update() -> bool { true }
}

// /// Index based on the raw position of an element
// #[derive(Debug, Copy, Clone, Eq, PartialEq)]
// pub struct RawPositionIndex;
//
// impl<E: EntryTraits> TreeIndex<E> for RawPositionIndex {
//     type FlushMarker = isize;
//     type IndexEntry = ItemCount;
//
//     fn increment_marker(marker: &mut Self::FlushMarker, entry: &E) {
//         *marker += entry.len() as isize;
//     }
//
//     fn decrement_marker(marker: &mut Self::FlushMarker, entry: &E) {
//         *marker -= entry.len() as isize;
//         // dbg!(&marker, entry);
//     }
//
//     fn update_size_by_marker(offset: &mut Self::IndexEntry, by: &Self::FlushMarker) {
//         // :( I wish there were a better way to do this.
//         *offset = offset.wrapping_add(*by as u32);
//     }
//
//     fn increment_entry_by_size(offset: &mut Self::IndexEntry, by: &E) {
//         *offset += by.len() as u32;
//     }
//
//     // This is unnecessary.
//     fn increment_offset_partial(offset: &mut Self::IndexEntry, by: &E, at: usize) {
//         *offset += by.len().min(at) as u32;
//     }
// }
//
// /// Index based on both resulting size and raw insert position
// #[derive(Debug, Copy, Clone, Eq, PartialEq)]
// pub struct FullIndex;
//
// #[derive(Default, Debug, PartialEq, Eq)]
// pub struct FullMarker {
//     pub len: i32,
//     pub content: i32
// }
//
// #[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
// pub struct FullOffset {
//     pub len: ItemCount, // Number of items ever inserted
//     pub content: ItemCount, // Number of items not currently deleted
// }
//
// impl AddAssign for FullOffset {
//     fn add_assign(&mut self, rhs: Self) {
//         self.len += rhs.len;
//         self.content += rhs.content;
//     }
// }
//
// impl SubAssign for FullOffset {
//     fn sub_assign(&mut self, rhs: Self) {
//         self.len -= rhs.len;
//         self.content -= rhs.content;
//     }
// }
//
// impl<E: EntryTraits + EntryWithContent> TreeIndex<E> for FullIndex {
//     type FlushMarker = FullMarker;
//     type IndexEntry = FullOffset;
//
//     fn increment_marker(marker: &mut Self::FlushMarker, entry: &E) {
//         marker.len += entry.len() as i32;
//         marker.content += entry.content_len() as i32;
//     }
//
//     fn decrement_marker(marker: &mut Self::FlushMarker, entry: &E) {
//         marker.len -= entry.len() as i32;
//         marker.content -= entry.content_len() as i32;
//     }
//
//     fn update_size_by_marker(offset: &mut Self::IndexEntry, by: &Self::FlushMarker) {
//         offset.len = offset.len.wrapping_add(by.len as u32);
//         offset.content = offset.content.wrapping_add(by.content as u32);
//     }
//
//     fn increment_entry_by_size(offset: &mut Self::IndexEntry, entry: &E) {
//         offset.len += entry.len() as u32;
//         offset.content += entry.content_len() as u32;
//     }
//
//     fn increment_offset_partial(offset: &mut Self::IndexEntry, by: &E, at: usize) {
//         offset.len += at as u32;
//         offset.content += by.content_len().min(at) as u32;
//     }
// }