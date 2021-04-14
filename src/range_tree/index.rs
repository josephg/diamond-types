use crate::range_tree::{EntryTraits};
use crate::common::ItemCount;
use std::fmt::Debug;
use std::ops::{Add, AddAssign, SubAssign};

/// The index describes which fields we're tracking, and can query. Indexes let us convert
/// cursors to positions and vice versa.

pub trait TreeIndex<E: EntryTraits> where Self: Debug + Copy + Clone + PartialEq + Eq {
    type FlushMarker: Debug + Default + PartialEq + Eq;
    type IndexOffset: Copy + Clone + Default + Debug + AddAssign + SubAssign + PartialEq + Eq + Sized;

    fn increment_marker(marker: &mut Self::FlushMarker, entry: &E);
    fn decrement_marker(marker: &mut Self::FlushMarker, entry: &E);

    fn update_offset_by_marker(offset: &mut Self::IndexOffset, by: &Self::FlushMarker);

    fn increment_offset(offset: &mut Self::IndexOffset, by: &E);

    // This is actually unnecessary - it would be more correct to call truncate().content_len()
    // or whatever. TODO: Check if this actually makes any performance difference.
    fn increment_offset_partial(offset: &mut Self::IndexOffset, by: &E, at: usize) {
        let mut e = *by;
        if e.len() < at { e.truncate(at); }
        Self::increment_offset(offset, &e);
        // *offset += by.content_len().min(at) as u32;
    }
}


// ***

/// Helper struct to track pending size changes in the document which need to be propagated
// #[derive(Debug, Default, Eq, PartialEq)]
// pub struct ContentFlushMarker(pub isize);
//
// impl Drop for ContentFlushMarker {
//     fn drop(&mut self) {
//         if self.0 != 0 {
//             if !std::thread::panicking() {
//                 panic!("Flush marker dropped without being flushed");
//             }
//         }
//     }
// }

// impl ContentFlushMarker {
//     // TODO: This should take a Pin<> or be unsafe or something. This is unsound because we could
//     // move node.
//     pub(super) fn flush<E: EntryTraits, I: TreeIndex<E>>(&mut self, node: &mut NodeLeaf<E, I>) {
//         // println!("Flush marker flushing {}", self.0);
//         node.update_parent_count(self.0 as i32);
//         self.0 = 0;
//     }
// }

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContentIndex;

impl<E: EntryTraits> TreeIndex<E> for ContentIndex {
    type FlushMarker = isize;
    type IndexOffset = ItemCount;

    fn increment_marker(marker: &mut Self::FlushMarker, entry: &E) {
        *marker += entry.content_len() as isize;
    }

    fn decrement_marker(marker: &mut Self::FlushMarker, entry: &E) {
        *marker -= entry.content_len() as isize;
        // dbg!(&marker, entry);
    }

    fn update_offset_by_marker(offset: &mut Self::IndexOffset, by: &Self::FlushMarker) {
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

impl<E: EntryTraits> TreeIndex<E> for FullIndex {
    type FlushMarker = FullMarker;
    type IndexOffset = FullOffset;

    fn increment_marker(marker: &mut Self::FlushMarker, entry: &E) {
        marker.len += entry.len() as i32;
        marker.content += entry.content_len() as i32;
    }

    fn decrement_marker(marker: &mut Self::FlushMarker, entry: &E) {
        marker.len -= entry.len() as i32;
        marker.content -= entry.content_len() as i32;
    }

    fn update_offset_by_marker(offset: &mut Self::IndexOffset, by: &Self::FlushMarker) {
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