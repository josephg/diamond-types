use std::fmt::Debug;
use std::ops::{AddAssign, SubAssign};

use crate::ContentTraits;

/// The index describes which fields we're tracking, and can query. Indexes let us convert
/// cursors to positions and vice versa.

pub trait TreeMetrics<E: ContentTraits> where Self: Debug + Copy + Clone + PartialEq + Eq {
    type Update: Debug + Default + PartialEq + Eq;
    type Value: Copy + Clone + Default + Debug + AddAssign + SubAssign + PartialEq + Eq + Sized;

    fn increment_marker(marker: &mut Self::Update, entry: &E);
    fn decrement_marker(marker: &mut Self::Update, entry: &E);

    // TODO: Unused. Consider removing.
    fn decrement_marker_by_val(marker: &mut Self::Update, val: &Self::Value);

    fn update_offset_by_marker(offset: &mut Self::Value, by: &Self::Update);

    fn increment_offset(offset: &mut Self::Value, by: &E);

    const CAN_COUNT_ITEMS: bool = false;
    // TODO: Unused. Consider removing.
    fn count_items(_idx: Self::Value) -> usize { panic!("Index cannot count items") }
}

pub trait FindContent<E: ContentTraits + ContentLength>: TreeMetrics<E> {
    fn index_to_content(offset: Self::Value) -> usize;
}

pub trait FindOffset<E: ContentTraits>: TreeMetrics<E> {
    fn index_to_offset(offset: Self::Value) -> usize;
}


/// Content index - which just indexes based on the resulting size. Deletes are not counted.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContentMetrics;

impl<E: ContentTraits + ContentLength> TreeMetrics<E> for ContentMetrics {
    type Update = isize;
    type Value = usize; // TODO: Move this to a template parameter.

    fn increment_marker(marker: &mut Self::Update, entry: &E) {
        *marker += entry.content_len() as isize;
    }

    fn decrement_marker(marker: &mut Self::Update, entry: &E) {
        *marker -= entry.content_len() as isize;
        // dbg!(&marker, entry);
    }

    fn decrement_marker_by_val(marker: &mut Self::Update, val: &Self::Value) {
        *marker -= *val as isize;
    }

    fn update_offset_by_marker(offset: &mut Self::Value, by: &Self::Update) {
        // :( I wish there were a better way to do this.
        *offset = offset.wrapping_add(*by as Self::Value);
    }

    fn increment_offset(offset: &mut Self::Value, by: &E) {
        *offset += by.content_len() as Self::Value;
    }
}

impl<E: ContentTraits + ContentLength> FindContent<E> for ContentMetrics {
    fn index_to_content(offset: Self::Value) -> usize { offset as usize }
    // fn entry_to_num(entry: &E) -> usize { entry.content_len() }
}

/// Index based on the raw size of an element.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RawPositionMetrics;

impl<E: ContentTraits> TreeMetrics<E> for RawPositionMetrics {
    type Update = isize;
    type Value = u32; // TODO: Move this to a template parameter.

    fn increment_marker(marker: &mut Self::Update, entry: &E) {
        *marker += entry.len() as isize;
    }

    fn decrement_marker(marker: &mut Self::Update, entry: &E) {
        *marker -= entry.len() as isize;
        // dbg!(&marker, entry);
    }

    fn decrement_marker_by_val(marker: &mut Self::Update, val: &Self::Value) {
        *marker -= *val as isize;
    }

    fn update_offset_by_marker(offset: &mut Self::Value, by: &Self::Update) {
        // :( I wish there were a better way to do this.
        *offset = offset.wrapping_add(*by as u32);
    }

    fn increment_offset(offset: &mut Self::Value, by: &E) {
        *offset += by.len() as u32;
    }

    const CAN_COUNT_ITEMS: bool = true;
    fn count_items(idx: Self::Value) -> usize { idx as usize }
}

impl<E: ContentTraits> FindOffset<E> for RawPositionMetrics {
    fn index_to_offset(offset: Self::Value) -> usize { offset as usize }
}


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

/// Index based on both resulting size and raw insert position.
///
/// Item 0 is the raw offset position, and item 1 is the content position.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FullMetricsU32;

impl<E: ContentTraits + ContentLength> TreeMetrics<E> for FullMetricsU32 {
    // In pair, len = 0, content = 1.
    type Update = Pair<i32>;
    type Value = Pair<u32>;

    fn increment_marker(marker: &mut Self::Update, entry: &E) {
        marker.0 += entry.len() as i32;
        marker.1 += entry.content_len() as i32;
    }

    fn decrement_marker(marker: &mut Self::Update, entry: &E) {
        marker.0 -= entry.len() as i32;
        marker.1 -= entry.content_len() as i32;
    }

    fn decrement_marker_by_val(marker: &mut Self::Update, val: &Self::Value) {
        marker.0 -= val.0 as i32;
        marker.1 -= val.1 as i32;
    }

    fn update_offset_by_marker(offset: &mut Self::Value, by: &Self::Update) {
        offset.0 = offset.0.wrapping_add(by.0 as u32);
        offset.1 = offset.1.wrapping_add(by.1 as u32);
    }

    fn increment_offset(offset: &mut Self::Value, entry: &E) {
        offset.0 += entry.len() as u32;
        offset.1 += entry.content_len() as u32;
    }

    const CAN_COUNT_ITEMS: bool = true;
    fn count_items(idx: Self::Value) -> usize { idx.0 as usize }
}

impl<E: ContentTraits + ContentLength> FindContent<E> for FullMetricsU32 {
    fn index_to_content(offset: Self::Value) -> usize {
        offset.1 as usize
    }
}

impl<E: ContentTraits + ContentLength> FindOffset<E> for FullMetricsU32 {
    fn index_to_offset(offset: Self::Value) -> usize {
        offset.0 as usize
    }
}

/// Index based on both resulting size and raw insert position.
///
/// Item 0 is the raw offset position, and item 1 is the content position.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FullMetricsUsize;

impl<E: ContentTraits + ContentLength> TreeMetrics<E> for FullMetricsUsize {
    // In pair, len = 0, content = 1.
    type Update = Pair<isize>;
    type Value = Pair<usize>;

    fn increment_marker(marker: &mut Self::Update, entry: &E) {
        marker.0 += entry.len() as isize;
        marker.1 += entry.content_len() as isize;
    }

    fn decrement_marker(marker: &mut Self::Update, entry: &E) {
        marker.0 -= entry.len() as isize;
        marker.1 -= entry.content_len() as isize;
    }

    fn decrement_marker_by_val(marker: &mut Self::Update, val: &Self::Value) {
        marker.0 -= val.0 as isize;
        marker.1 -= val.1 as isize;
    }

    fn update_offset_by_marker(offset: &mut Self::Value, by: &Self::Update) {
        offset.0 = offset.0.wrapping_add(by.0 as usize);
        offset.1 = offset.1.wrapping_add(by.1 as usize);
    }

    fn increment_offset(offset: &mut Self::Value, entry: &E) {
        offset.0 += entry.len() as usize;
        offset.1 += entry.content_len() as usize;
    }

    const CAN_COUNT_ITEMS: bool = true;
    fn count_items(idx: Self::Value) -> usize { idx.0 }
}

impl<E: ContentTraits + ContentLength> FindContent<E> for FullMetricsUsize {
    fn index_to_content(offset: Self::Value) -> usize {
        offset.1 as usize
    }
}

impl<E: ContentTraits + ContentLength> FindOffset<E> for FullMetricsUsize {
    fn index_to_offset(offset: Self::Value) -> usize {
        offset.0 as usize
    }
}



pub trait ContentLength {
    /// User specific content length. Used by content-tree for character counts.
    fn content_len(&self) -> usize;
    fn content_len_at_offset(&self, offset: usize) -> usize;
    fn offset_len_at_content(&self, content: usize) -> usize { content }
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
