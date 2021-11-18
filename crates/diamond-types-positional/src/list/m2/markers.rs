use std::fmt::Debug;
use std::ptr::NonNull;

use rle::{HasLength, MergableSpan, SplitableSpan};

use content_tree::*;
use content_tree::ContentTraits;
use rle::Searchable;
use crate::list::m2::delete::TimeSpanRev;
use crate::list::m2::DocRangeIndex;
use crate::list::m2::yjsspan2::YjsSpan2;

// use crate::common::IndexGet;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct MarkerEntry {
    // TODO: Clean this mess up. Yikes! We only have either ptr or delete_info - replace with an enum.

    // This is cleaner as a separate enum and struct, but doing it that way
    // bumps it from 16 to 24 bytes per entry because of alignment.
    pub len: usize,

    /// This is the pointer to the leaf node containing the inserted item. For deletes, we do not
    /// reuse this to store the pointer because when items are moved, we can't move the pointer too.
    pub ptr: Option<NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>>>,

    // Could inline this. The reverse logic is complex though.
    // Length is duplicated here during deletes for bad reasons.
    // TODO: Clean this up.
    pub delete_info: Option<TimeSpanRev>,
}

impl HasLength for MarkerEntry {
    fn len(&self) -> usize {
        self.len as usize
    }
}

fn edit_mut<F: FnOnce(&mut G) -> R, G, R>(opt: &mut Option<G>, edit_fn: F) -> Option<R> {
    if let Some(val) = opt {
        Some(edit_fn(val))
    } else {
        None
    }
}

impl SplitableSpan for MarkerEntry {
    fn truncate(&mut self, at: usize) -> Self {
        let remainder_len = self.len - at;
        self.len = at;
        MarkerEntry {
            len: remainder_len,
            ptr: self.ptr,
            delete_info: edit_mut(&mut self.delete_info, |d| d.truncate(at)),
        }
    }

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let left = Self {
            len: at,
            ptr: self.ptr,
            delete_info: edit_mut(&mut self.delete_info, |d| d.truncate_keeping_right(at)),
        };
        self.len -= at;
        left
    }
}

impl MergableSpan for MarkerEntry {
    fn can_append(&self, other: &Self) -> bool {
        self.ptr == other.ptr
            && match (self.delete_info, other.delete_info) {
            (None, None) => true,
            (Some(a), Some(b)) => a.can_append(&b),
            _ => false,
        }
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
        match (&mut self.delete_info, other.delete_info) {
            (None, None) => {},
            (Some(a), Some(b)) => a.append(b),
            _ => panic!("Invalid append"),
        }
    }

    fn prepend(&mut self, other: Self) {
        self.len += other.len;
        match (&mut self.delete_info, other.delete_info) {
            (None, None) => {},
            (Some(a), Some(b)) => a.prepend(b),
            _ => panic!("Invalid append"),
        }
    }
}

// impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> IndexGet<usize> for MarkerEntry<YjsSpan2, DocRangeIndex, IE, LE> {
//     type Output = NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, IE, LE>>;
//
//     fn index_get(&self, _index: usize) -> Self::Output {
//         self.ptr
//     }
// }



impl Default for MarkerEntry {
    fn default() -> Self {
        MarkerEntry {ptr: None, len: 0, delete_info: None }
    }
}


impl MarkerEntry {
    pub fn unwrap_ptr(&self) -> NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>> {
        self.ptr.unwrap()
    }
}

impl Searchable for MarkerEntry {
    type Item = Option<NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>>>;

    fn get_offset(&self, _loc: Self::Item) -> Option<usize> {
        panic!("Should never be used")
    }

    fn at_offset(&self, _offset: usize) -> Self::Item {
        self.ptr
    }
}

// #[cfg(test)]
// mod tests {
//     use std::ptr::NonNull;
//     use crate::list::Time;
//
//     #[test]
//     fn test_sizes() {
//         #[derive(Copy, Clone, Eq, PartialEq, Debug)]
//         pub enum MarkerOp {
//             Ins(NonNull<usize>),
//             Del(Time),
//         }
//
//         #[derive(Copy, Clone, Eq, PartialEq, Debug)]
//         pub struct MarkerEntry1 {
//             // The order / seq is implicit from the location in the list.
//             pub len: u32,
//             pub op: MarkerOp
//         }
//
//         dbg!(std::mem::size_of::<MarkerEntry1>());
//
//         #[derive(Copy, Clone, Eq, PartialEq, Debug)]
//         pub enum MarkerEntry2 {
//             Ins(u32, NonNull<usize>),
//             Del(u32, Time, bool),
//         }
//         dbg!(std::mem::size_of::<MarkerEntry2>());
//
//         pub type MarkerEntry3 = (u64, Option<NonNull<usize>>);
//         dbg!(std::mem::size_of::<MarkerEntry3>());
//     }
// }