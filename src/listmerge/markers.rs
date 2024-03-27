use std::fmt::Debug;
use std::ptr::NonNull;

use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanHelpers};

use content_tree::*;
use rle::Searchable;
use crate::rev_range::RangeRev;
use crate::listmerge::DocRangeIndex;
use crate::listmerge::yjsspan::CRDTSpan;
use crate::list::operation::ListOpKind;
use crate::{DTRange, LV};
use crate::ost::IndexContent;

// TODO: Consider refactoring this to be a single enum. Put len in InsPtr and use .len(). But this
// might make the code way slower.

// #[derive(Copy, Clone, Eq, PartialEq, Debug)]
// pub enum MarkerOld {
//     /// For inserts, we store a pointer to the leaf node containing the inserted item. This is only
//     /// used for inserts so we don't need to modify multiple entries when the inserted item is
//     /// moved.
//     InsPtr(NonNull<NodeLeaf<CRDTSpan, DocRangeIndex>>),
//
//     /// For deletes we name the delete's target. Note this contains redundant information - since
//     /// we already have a length field.
//     DelTarget(RangeRev),
// }
//
// /// So this struct is a little weird. Its designed this way so I can reuse content-tree for two
// /// different structures:
// ///
// /// - When we enable and disable inserts, we need a marker (index) into the b-tree node in the range
// ///   tree containing that entry. This lets us find things in O(log n) time, which improves
// ///   performance for large merges. (Though at a cost of extra bookkeeping overhead for small
// ///   merges).
// /// - For deletes, we need to know the delete's target. Ie, which corresponding insert inserted the
// ///   item which was deleted by this edit.
// ///
// /// The cleanest implementation of this would store a TimeSpan for the ID of this edit instead of
// /// just storing a length field. And we'd use a variant of the content-tree which uses absolutely
// /// positioned items like a normal b-tree with RLE. But I don't have an implementation of that. So
// /// instead we end up with this slightly weird structure.
// #[derive(Copy, Clone, Eq, PartialEq, Debug)]
// pub struct MarkerEntry {
//     pub len: usize,
//     pub inner: MarkerOld,
// }
//
// impl MarkerOld {
//     pub(super) fn tag(&self) -> ListOpKind {
//         match self {
//             MarkerOld::InsPtr(_) => ListOpKind::Ins,
//             MarkerOld::DelTarget(_) => ListOpKind::Del
//         }
//     }
// }

// impl HasLength for MarkerEntry {
//     fn len(&self) -> usize {
//         self.len
//     }
// }
//
// impl SplitableSpanHelpers for MarkerOld {
//     fn truncate_h(&mut self, at: usize) -> Self {
//         match self {
//             InsPtr(_) => *self,
//             MarkerOld::DelTarget(target) => DelTarget(target.truncate(at)),
//         }
//     }
// }
//
// impl SplitableSpanHelpers for MarkerEntry {
//     fn truncate_h(&mut self, at: usize) -> Self {
//         let remainder_len = self.len - at;
//         self.len = at;
//         MarkerEntry {
//             len: remainder_len,
//             inner: self.inner.truncate(at),
//         }
//     }
//
//     fn truncate_keeping_right_h(&mut self, at: usize) -> Self {
//         let left = Self {
//             len: at,
//             inner: self.inner.truncate_keeping_right(at)
//         };
//         self.len -= at;
//         left
//     }
// }
//
// impl MergableSpan for MarkerOld {
//     fn can_append(&self, other: &Self) -> bool {
//         match (self, other) {
//             (InsPtr(ptr1), InsPtr(ptr2)) => {
//                 ptr1 == ptr2
//             }
//             (DelTarget(t1), DelTarget(t2)) => t1.can_append(t2),
//             _ => false,
//         }
//     }
//
//     fn append(&mut self, other: Self) {
//         match (self, other) {
//             (InsPtr(_), InsPtr(_)) => {},
//             (DelTarget(t1), DelTarget(t2)) => t1.append(t2),
//             _ => {
//                 panic!("Internal consistency error: Invalid append");
//             },
//         }
//     }
//
//     fn prepend(&mut self, other: Self) {
//         match (self, other) {
//             (InsPtr(_), InsPtr(_)) => {},
//             (DelTarget(t1), DelTarget(t2)) => t1.prepend(t2),
//             _ => {
//                 panic!("Internal consistency error: Invalid prepend");
//             },
//         }
//     }
// }
//
// impl MergableSpan for MarkerEntry {
//     fn can_append(&self, other: &Self) -> bool {
//         self.inner.can_append(&other.inner)
//     }
//
//     fn append(&mut self, other: Self) {
//         self.len += other.len;
//         self.inner.append(other.inner);
//     }
//
//     fn prepend(&mut self, other: Self) {
//         self.len += other.len;
//         self.inner.prepend(other.inner);
//     }
// }
//
// // impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> IndexGet<usize> for MarkerEntry<YjsSpan2, DocRangeIndex, IE, LE> {
// //     type Output = NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, IE, LE>>;
// //
// //     fn index_get(&self, _index: usize) -> Self::Output {
// //         self.ptr
// //     }
// // }
//
//
//
// impl Default for MarkerEntry {
//     fn default() -> Self {
//         MarkerEntry {
//             len: 0,
//             inner: InsPtr(std::ptr::NonNull::dangling()),
//         }
//     }
// }
//
//
// // impl MarkerEntry {
// //     pub fn unwrap_ptr(&self) -> NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>> {
// //         if let InsPtr(ptr) = self.inner {
// //             ptr
// //         } else {
// //             panic!("Internal consistency error: Cannot unwrap delete");
// //         }
// //     }
// // }
//
// impl Searchable for MarkerEntry {
//     type Item = Option<NonNull<NodeLeaf<CRDTSpan, DocRangeIndex>>>;
//
//     fn get_offset(&self, _loc: Self::Item) -> Option<usize> {
//         panic!("Should never be used")
//     }
//
//     fn at_offset(&self, _offset: usize) -> Self::Item {
//         if let InsPtr(ptr) = self.inner {
//             Some(ptr)
//         } else {
//             None
//         }
//     }
// }


/// Its kind of upsetting that I need this.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DelRange {
    /// This is the first LV that the range targets. If fwd, it is the lowest. Otherwise, it is
    /// the "end" of the range - which is 1 past the deleted range.
    pub target: LV,
    pub fwd: bool,
}

impl DelRange {
    pub fn new(target: LV, fwd: bool) -> Self {
        Self { target, fwd }
    }

    /// Get the relative range from start + offset_start to start + offset_end.
    ///
    /// The returned range is always "forwards".
    pub fn range(&self, offset_start: usize, offset_end: usize) -> DTRange {
        debug_assert!(offset_start <= offset_end);

        if self.fwd {
            // Simple case.
            DTRange {
                start: self.target + offset_start,
                end: self.target + offset_end
            }
        } else {
            DTRange {
                start: self.target - offset_end,
                end: self.target - offset_start
            }
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Marker {
    /// For inserts, we store a pointer to the leaf node containing the inserted item. This is only
    /// used for inserts, so we don't need to modify multiple entries when the inserted item is
    /// moved.
    InsPtr(NonNull<NodeLeaf<CRDTSpan, DocRangeIndex>>),

    /// For deletes we name the delete's target. Note this contains redundant information - since
    /// we already have a length field.
    Del(DelRange),
}

// impl From<MarkerOld> for Marker {
//     fn from(marker: MarkerOld) -> Self {
//         match marker {
//             InsPtr(ptr) => Marker::InsPtr(ptr),
//             DelTarget(RangeRev { span, fwd }) => {
//                 Marker::Del(DelRange {
//                     target: if fwd { span.start } else { span.end },
//                     fwd,
//                 })
//             }
//         }
//     }
// }

impl Default for Marker {
    fn default() -> Self {
        Marker::InsPtr(NonNull::dangling())
    }
}

impl IndexContent for Marker {
    fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool {
        debug_assert!(offset > 0);

        match (self, other) {
            (Marker::InsPtr(p1), Marker::InsPtr(p2)) => p1 == p2,
            (Marker::Del(a), Marker::Del(b)) => {
                // let offs_1 = offset == 1;
                // let other_len_1 = other_len == 1;

                // Can we append forwards?
                if a.fwd && b.fwd && a.target + offset == b.target { return true; }

                // Can we append backwards? This is horrible. If we're going to, first figure out
                // the expected resulting a.target value. IF we can append backward, they will
                // match.
                let a_start = if !a.fwd {
                    Some(a.target)
                } else if offset == 1 {
                    Some(a.target + 1)
                } else { None };

                let b_start = if !b.fwd {
                    Some(b.target + offset)
                } else if other_len == 1 {
                    Some(b.target + 1 + offset)
                } else { None };

                if let (Some(a_start), Some(b_start)) = (a_start, b_start) {
                    if a_start == b_start {
                        a.target = b_start;
                        a.fwd = false;
                        return true;
                    }
                }

                return false;
            },
            _ => false,
        }
    }

    fn at_offset(&self, offset: usize) -> Self {
        match self {
            Marker::InsPtr(_) => *self,
            Marker::Del(DelRange {target: lv, fwd: true}) => Marker::Del(DelRange::new(*lv + offset, true)),
            Marker::Del(DelRange {target: lv, fwd: false}) => Marker::Del(DelRange::new(*lv - offset, false)),
        }
    }

    fn eq(&self, other: &Self, upto_len: usize) -> bool {
        debug_assert!(upto_len >= 1);

        self == other || if let (Marker::Del(a), Marker::Del(b)) = (self, other) {
            // We can only save equality if upto_len == 1, one of them is reversed, and the target is off by 1.
            upto_len == 1 && (
                a.fwd && !b.fwd && a.target + 1 == b.target
            ) || (
                !a.fwd && b.fwd && a.target == b.target + 1
            )
        } else { false }
    }

    // fn append_at(&mut self, offset: usize, other: Self) {
    //     debug_assert!(self.can_append(offset, &other));
    //
    //     match (self, other) {
    //         (Marker2::Del(a), Marker2::Del(b)) => {
    //             a.fwd = b.target >= a.target;
    //         },
    //         _ => {}
    //     }
    // }
}



#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use rle::test_splitable_methods_valid;
    use crate::rev_range::RangeRev;
    use super::*;

    // #[test]
    // fn marker_split_merge() {
    //     test_splitable_methods_valid(MarkerEntry {
    //         len: 10,
    //         inner: InsPtr(NonNull::dangling())
    //     });
    //
    //     test_splitable_methods_valid(MarkerEntry {
    //         len: 10,
    //         inner: DelTarget(RangeRev {
    //             span: (0..10).into(),
    //             fwd: true,
    //         })
    //     });
    //
    //     test_splitable_methods_valid(MarkerEntry {
    //         len: 10,
    //         inner: DelTarget(RangeRev {
    //             span: (0..10).into(),
    //             fwd: false,
    //         })
    //     });
    // }
}