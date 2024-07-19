//! This module exists as a future planned replacement for the content-tree crate. It has a few
//! advantages:
//!
//! - I have two separate data structures, one for the index and one for content. Content-tree uses
//!   the same b-tree data structure for both
//! - These btree implementations store data in a Vec<Leaf> / Vec<Node> pair rather than using raw
//!   pointers. Surprisingly, this turns out to perform better - because the CPU ends up caching
//!   runs of nodes. It also means this works with no unsafe {} blocks.
//! - There's less abstraction here. Way less abstraction. I went a bit overboard with content-tree
//!   and as a result, its much harder to read. However, the code here has more duplication. Eh.
//! - The resulting wasm size is a little smaller.

mod index_tree;
pub(crate) mod content_tree;
pub(crate) mod recording_index_tree;
// mod content_tree;

use std::iter::Sum;
pub(crate) use index_tree::{IndexContent, IndexTree};

use std::ops::{Add, AddAssign, Index, IndexMut, Range, Sub, SubAssign};
use rle::{HasLength, MergableSpan, SplitableSpan};
use crate::listmerge::yjsspan::CRDTSpan;
// use crate::ost::content_tree::ContentTree;
// Some utility types.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct LeafIdx(pub(crate) usize);

impl Default for LeafIdx {
    fn default() -> Self { Self(usize::MAX) }
}
impl LeafIdx {
    pub fn exists(&self) -> bool { self.0 != usize::MAX }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct NodeIdx(usize);

impl Default for NodeIdx {
    fn default() -> Self { Self(usize::MAX) }
}

impl NodeIdx {
    fn is_root(&self) -> bool { self.0 == usize::MAX }
}

// #[derive(Copy, Clone, Eq, PartialEq, Debug)]
// enum LenType { CURRENT, END }

#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct LenPair {
    pub cur: usize,
    pub end: usize,
}

impl LenPair {
    fn get<const IS_CURRENT: bool>(&self) -> usize {
        if IS_CURRENT { self.cur } else { self.end }
    }

    #[inline]
    fn update_by(&mut self, upd: LenUpdate) {
        self.cur = self.cur.wrapping_add_signed(upd.cur);
        self.end = self.end.wrapping_add_signed(upd.end);
    }

    pub fn new(cur: usize, end: usize) -> Self {
        LenPair { cur, end }
    }
}

impl From<(usize, usize)> for LenPair {
    fn from((cur, end): (usize, usize)) -> Self {
        Self::new(cur, end)
    }
}

impl AddAssign for LenPair {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.cur += rhs.cur;
        self.end += rhs.end;
    }
}

impl SubAssign for LenPair {
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        self.cur -= rhs.cur;
        self.end -= rhs.end;
    }
}

impl Add for LenPair {
    type Output = LenPair;

    fn add(self, rhs: Self) -> Self::Output {
        LenPair {
            cur: self.cur + rhs.cur,
            end: self.end + rhs.end,
        }
    }
}

impl Sub for LenPair {
    type Output = LenPair;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            cur: self.cur - rhs.cur,
            end: self.end - rhs.end,
        }
    }
}

impl Sum for LenPair {
    fn sum<I: Iterator<Item=Self>>(iter: I) -> Self {
        let mut aggregate = Self::default();
        for i in iter { aggregate += i; }
        aggregate
    }
}


impl CRDTSpan {
    fn len_pair(&self) -> LenPair {
        LenPair {
            cur: self.content_len(),
            end: self.end_state_len(),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct LenUpdate {
    pub cur: isize,
    pub end: isize,
}

impl LenUpdate {
    fn inc_by(&mut self, e: &CRDTSpan) {
        self.cur += e.content_len() as isize;
        self.end += e.end_state_len() as isize;
    }

    fn dec_by(&mut self, e: &CRDTSpan) {
        self.cur -= e.content_len() as isize;
        self.end -= e.end_state_len() as isize;
    }

    fn is_empty(&self) -> bool {
        self.cur == 0 && self.end == 0
    }
}

// In debug mode, nodes are kept intentionally small to exercise the node splitting / joining code
// more.
#[cfg(debug_assertions)]
const NODE_CHILDREN: usize = 4;
#[cfg(debug_assertions)]
const LEAF_CHILDREN: usize = 4;

// Figured out with benchmarking.
#[cfg(not(debug_assertions))]
const NODE_CHILDREN: usize = 16;
#[cfg(not(debug_assertions))]
const LEAF_CHILDREN: usize = 32;


// type LeafData = crate::listmerge::markers::Marker;
// #[derive(Debug, Default)]
// struct OrderStatisticTree {
//     content: ContentTree,
//     index: IndexTree<()>,
// }
//
// impl OrderStatisticTree {
//     pub fn new() -> Self {
//         Self {
//             content: ContentTree::new(),
//             index: IndexTree::new(),
//         }
//     }
//
//     // fn insert(&mut self,
//
//     pub fn clear(&mut self) {
//         self.index.clear();
//         self.content.clear();
//     }
//
//     #[allow(unused)]
//     fn dbg_check(&self) {
//         self.content.dbg_check();
//         self.index.dbg_check();
//
//         // Invariants:
//         // - All index markers point to the node which contains the specified item.
//     }
// }

/// Utility method for tree implementations.
///
/// Remove the items in `a[del_range]`, sliding back items later in the array.
#[inline(always)]
fn remove_from_array<T: Sized + Copy, const S: usize>(a: &mut [T; S], del_range: Range<usize>) {
    a.copy_within(del_range.end..S, del_range.start);
}

/// Utility method for tree implementations.
///
/// Remove the items in `a[del_range]`, sliding back items later in the array. The end of the array
/// is filled with the provided default value.
#[inline(always)]
fn remove_from_array_fill<T: Sized + Copy, const S: usize>(a: &mut [T; S], del_range: Range<usize>, default: T) {
    a.copy_within(del_range.end..S, del_range.start);
    a[S - del_range.len()..S].fill(default);
}


