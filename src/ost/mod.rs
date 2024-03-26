mod index_tree;
mod content_tree;

pub(crate) use index_tree::{IndexTree, IndexContent};

use std::ops::{AddAssign, Index, IndexMut, SubAssign};
use ::content_tree::ContentLength;
use rle::{HasLength, MergableSpan, SplitableSpan};
use crate::listmerge::yjsspan::CRDTSpan;
use crate::ost::content_tree::ContentTree;
// Some utility types.

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct LeafIdx(usize);

impl Default for LeafIdx {
    fn default() -> Self { Self(usize::MAX) }
}
impl LeafIdx {
    fn exists(&self) -> bool { self.0 != usize::MAX }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct NodeIdx(usize);

impl Default for NodeIdx {
    fn default() -> Self { Self(usize::MAX) }
}

impl NodeIdx {
    fn is_root(&self) -> bool { self.0 == usize::MAX }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct LenPair {
    pub cur: usize,
    pub end: usize,
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
}

#[cfg(debug_assertions)]
const NODE_CHILDREN: usize = 4;
#[cfg(debug_assertions)]
const LEAF_CHILDREN: usize = 4;

#[cfg(not(debug_assertions))]
const NODE_CHILDREN: usize = 16;
#[cfg(not(debug_assertions))]
const LEAF_SIZE: usize = 32;


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


