use crate::range_tree::{EntryTraits, NodeLeaf};
use crate::common::ItemCount;
use std::fmt::Debug;

/// The index describes which fields we're tracking, and can query. Indexes let us convert
/// cursors to positions and vice versa.

pub trait TreeIndex<E: EntryTraits> where Self: Debug + Copy + Clone + PartialEq + Eq {
    type FlushMarker: Debug;
    type IndexOffset: Copy + Clone + Default + Debug;

    fn increment_marker(marker: &mut Self::FlushMarker, entry: &E);
    fn decrement_marker(marker: &mut Self::FlushMarker, entry: &E);
    fn update_offset(offset: &mut Self::IndexOffset, by: &Self::FlushMarker);
}


// ***

/// Helper struct to track pending size changes in the document which need to be propagated
#[derive(Debug)]
pub struct ContentFlushMarker(pub isize);

impl Drop for ContentFlushMarker {
    fn drop(&mut self) {
        if self.0 != 0 {
            if !std::thread::panicking() {
                panic!("Flush marker dropped without being flushed");
            }
        }
    }
}

impl ContentFlushMarker {
    // TODO: This should take a Pin<> or be unsafe or something. This is unsound because we could
    // move node.
    pub(super) fn flush<E: EntryTraits, I: TreeIndex<E>>(&mut self, node: &mut NodeLeaf<E, I>) {
        // println!("Flush marker flushing {}", self.0);
        node.update_parent_count(self.0 as i32);
        self.0 = 0;
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContentIndex;

impl<E: EntryTraits> TreeIndex<E> for ContentIndex {
    type FlushMarker = ContentFlushMarker;
    type IndexOffset = ItemCount;

    fn increment_marker(marker: &mut Self::FlushMarker, entry: &E) {
        marker.0 += entry.content_len() as isize;
    }

    fn decrement_marker(marker: &mut Self::FlushMarker, entry: &E) {
        marker.0 -= entry.content_len() as isize;
    }

    fn update_offset(offset: &mut Self::IndexOffset, by: &Self::FlushMarker) {
        // :( I wish there were a better way to do this.
        *offset = offset.wrapping_add(by.0 as u32);
    }
}