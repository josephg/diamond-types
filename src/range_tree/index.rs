use crate::range_tree::{EntryTraits, EntryWithContent};
use crate::common::ItemCount;
use std::fmt::Debug;
use std::ops::{AddAssign, SubAssign};

/// The index describes which fields we're tracking, and can query. Indexes let us convert
/// cursors to positions and vice versa.

pub trait TreeIndex<E: EntryTraits> where Self: Debug + Copy + Clone + PartialEq + Eq {
    type IndexUpdate: Debug + Default + PartialEq + Eq;
    type IndexValue: Copy + Clone + Default + Debug + AddAssign + SubAssign + PartialEq + Eq + Sized;

    fn increment_marker(marker: &mut Self::IndexUpdate, entry: &E);
    fn decrement_marker(marker: &mut Self::IndexUpdate, entry: &E);

    fn decrement_marker_by_val(marker: &mut Self::IndexUpdate, val: &Self::IndexValue);

    fn update_offset_by_marker(offset: &mut Self::IndexValue, by: &Self::IndexUpdate);

    fn increment_offset(offset: &mut Self::IndexValue, by: &E);

    // This is actually unnecessary - it would be more correct to call truncate().content_len()
    // or whatever. TODO: Check if this actually makes any performance difference.
    fn increment_offset_partial(offset: &mut Self::IndexValue, by: &E, at: usize) {
        let mut e = *by;
        if at < e.len() { e.truncate(at); }
        Self::increment_offset(offset, &e);
        // *offset += by.content_len().min(at) as u32;
    }

    const CAN_COUNT_ITEMS: bool = false;
    fn count_items(_idx: Self::IndexValue) -> usize { panic!("Index cannot count items") }
}

/// Content index - which just indexes based on the resulting size. Deletes are not counted.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContentIndex;

impl<E: EntryTraits + EntryWithContent> TreeIndex<E> for ContentIndex {
    type IndexUpdate = isize;
    type IndexValue = ItemCount;

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

    // This is unnecessary.
    fn increment_offset_partial(offset: &mut Self::IndexValue, by: &E, at: usize) {
        *offset += by.content_len().min(at) as u32;
    }
}

/// Index based on the raw size of an element.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct RawPositionIndex;

impl<E: EntryTraits> TreeIndex<E> for RawPositionIndex {
    type IndexUpdate = isize;
    type IndexValue = ItemCount;

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

    // This is unnecessary.
    fn increment_offset_partial(offset: &mut Self::IndexValue, by: &E, at: usize) {
        *offset += by.len().min(at) as u32;
    }

    const CAN_COUNT_ITEMS: bool = true;
    fn count_items(idx: Self::IndexValue) -> usize { idx as usize }
}

/// Index based on both resulting size and raw insert position
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FullIndex;

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

impl<E: EntryTraits + EntryWithContent> TreeIndex<E> for FullIndex {
    // In pair, len = 0, content = 1.
    type IndexUpdate = Pair<i32>;
    type IndexValue = Pair<ItemCount>;

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

    fn increment_offset_partial(offset: &mut Self::IndexValue, by: &E, at: usize) {
        offset.0 += at as u32;
        offset.1 += by.content_len().min(at) as u32;
    }
}