use std::fmt::Debug;
use std::ops::{AddAssign, SubAssign};

use crate::ContentTraits;

/// The index describes which fields we're tracking, and can query. Indexes let us convert
/// cursors to positions and vice versa.

pub trait TreeIndex<E: ContentTraits> where Self: Debug + Copy + Clone + PartialEq + Eq {
    type IndexUpdate: Debug + Default + PartialEq + Eq;
    type IndexValue: Copy + Clone + Default + Debug + AddAssign + SubAssign + PartialEq + Eq + Sized;

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E);
    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E);

    // TODO: Unused. Consider removing.
    fn decrement_marker_by_val(marker: &mut Self::IndexUpdate, val: &Self::IndexValue);

    fn update_offset_by_marker(offset: &mut Self::IndexValue, by: &Self::IndexUpdate);

    fn increment_offset(offset: &mut Self::IndexValue, by: &E);

    const CAN_COUNT_ITEMS: bool = false;
    // TODO: Unused. Consider removing.
    fn count_items(_idx: Self::IndexValue) -> usize { panic!("Index cannot count items") }
}

pub trait FindContent<E: ContentTraits + ContentLength>: TreeIndex<E> {
    fn index_to_content(offset: Self::IndexValue) -> usize;
}

pub trait FindOffset<E: ContentTraits>: TreeIndex<E> {
    fn index_to_offset(offset: Self::IndexValue) -> usize;
}


/// Content index - which just indexes based on the resulting size. Deletes are not counted.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContentIndex;

impl<E: ContentTraits + ContentLength> TreeIndex<E> for ContentIndex {
    type IndexUpdate = isize;
    type IndexValue = u32; // TODO: Move this to a template parameter.

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker += entry.content_len() as isize;
    }

    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker -= entry.content_len() as isize;
        // dbg!(&marker, entry);
    }

    fn decrement_marker_by_val(marker: &mut Self::IndexUpdate, val: &Self::IndexValue) {
        *marker -= *val as isize;
    }

    fn update_offset_by_marker(offset: &mut Self::IndexValue, by: &Self::IndexUpdate) {
        // :( I wish there were a better way to do this.
        *offset = offset.wrapping_add(*by as u32);
    }

    fn increment_offset(offset: &mut Self::IndexValue, by: &E) {
        *offset += by.content_len() as u32;
    }
}

impl<E: ContentTraits + ContentLength> FindContent<E> for ContentIndex {
    fn index_to_content(offset: Self::IndexValue) -> usize { offset as usize }
    // fn entry_to_num(entry: &E) -> usize { entry.content_len() }
}

/// Index based on the raw size of an element.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RawPositionIndex;

impl<E: ContentTraits> TreeIndex<E> for RawPositionIndex {
    type IndexUpdate = isize;
    type IndexValue = u32; // TODO: Move this to a template parameter.

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker += entry.len() as isize;
    }

    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        *marker -= entry.len() as isize;
        // dbg!(&marker, entry);
    }

    fn decrement_marker_by_val(marker: &mut Self::IndexUpdate, val: &Self::IndexValue) {
        *marker -= *val as isize;
    }

    fn update_offset_by_marker(offset: &mut Self::IndexValue, by: &Self::IndexUpdate) {
        // :( I wish there were a better way to do this.
        *offset = offset.wrapping_add(*by as u32);
    }

    fn increment_offset(offset: &mut Self::IndexValue, by: &E) {
        *offset += by.len() as u32;
    }

    const CAN_COUNT_ITEMS: bool = true;
    fn count_items(idx: Self::IndexValue) -> usize { idx as usize }
}

impl<E: ContentTraits> FindOffset<E> for RawPositionIndex {
    fn index_to_offset(offset: Self::IndexValue) -> usize { offset as usize }
}


/// Index based on both resulting size and raw insert position
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FullIndex;

// Not sure why tuples of integers don't have AddAssign and SubAssign.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct Pair<V: Copy + Clone + Default + AddAssign + SubAssign + PartialEq + Eq>(pub V, pub V);

impl<V: Copy + Clone + Default + AddAssign + SubAssign + PartialEq + Eq> AddAssign for Pair<V> {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
        self.1 += rhs.1;
    }
}
impl<V: Copy + Clone + Default + AddAssign + SubAssign + PartialEq + Eq> SubAssign for Pair<V> {
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
        self.1 -= rhs.1;
    }
}

impl<E: ContentTraits + ContentLength> TreeIndex<E> for FullIndex {
    // In pair, len = 0, content = 1.
    type IndexUpdate = Pair<i32>;
    type IndexValue = Pair<u32>;

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        marker.0 += entry.len() as i32;
        marker.1 += entry.content_len() as i32;
    }

    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E) {
        marker.0 -= entry.len() as i32;
        marker.1 -= entry.content_len() as i32;
    }

    fn decrement_marker_by_val(marker: &mut Self::IndexUpdate, val: &Self::IndexValue) {
        marker.0 -= val.0 as i32;
        marker.1 -= val.1 as i32;
    }

    fn update_offset_by_marker(offset: &mut Self::IndexValue, by: &Self::IndexUpdate) {
        offset.0 = offset.0.wrapping_add(by.0 as u32);
        offset.1 = offset.1.wrapping_add(by.1 as u32);
    }

    fn increment_offset(offset: &mut Self::IndexValue, entry: &E) {
        offset.0 += entry.len() as u32;
        offset.1 += entry.content_len() as u32;
    }

    const CAN_COUNT_ITEMS: bool = true;
    fn count_items(idx: Self::IndexValue) -> usize { idx.0 as usize }
}

impl<E: ContentTraits + ContentLength> FindContent<E> for FullIndex {
    fn index_to_content(offset: Self::IndexValue) -> usize {
        offset.1 as usize
    }
}

impl<E: ContentTraits + ContentLength> FindOffset<E> for FullIndex {
    fn index_to_offset(offset: Self::IndexValue) -> usize {
        offset.0 as usize
    }
}

pub trait ContentLength {
    /// User specific content length. Used by content-tree for character counts.
    fn content_len(&self) -> usize;
    fn content_len_at_offset(&self, offset: usize) -> usize;
}

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
