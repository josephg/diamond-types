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

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Its kind of upsetting that I need this.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(into = "MarkerJSON", from = "MarkerJSON"))]
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

/// This is used for replaying data in the IndexTree for micro benchmarking.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
enum MarkerJSON {
    InsPtr(usize),
    Del(DelRange),
}

#[cfg(feature = "serde")]
impl From<Marker> for MarkerJSON {
    fn from(value: Marker) -> Self {
        match value {
            Marker::InsPtr(ptr) => MarkerJSON::InsPtr(ptr.as_ptr() as usize),
            Marker::Del(range) => MarkerJSON::Del(range)
        }
    }
}

// This is wildly unsafe. Only useful / correct for testing data.
#[cfg(feature = "serde")]
impl From<MarkerJSON> for Marker {
    fn from(value: MarkerJSON) -> Self {
        match value {
            MarkerJSON::InsPtr(ptr) => Marker::InsPtr(unsafe { std::mem::transmute(ptr) } ),
            MarkerJSON::Del(range) => Marker::Del(range),
        }
    }
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