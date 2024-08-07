use std::fmt::Debug;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanHelpers};
use rle::Searchable;

use crate::{DTRange, LV};
use crate::ost::{IndexContent, LeafIdx};

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

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Marker {
    /// For inserts, we store a pointer to the leaf node containing the inserted item. This is only
    /// used for inserts, so we don't need to modify multiple entries when the inserted item is
    /// moved.
    InsPtr(LeafIdx),

    /// For deletes we name the delete's target. Note this contains redundant information - since
    /// we already have a length field.
    Del(DelRange),
}

impl Default for Marker {
    fn default() -> Self {
        Self::InsPtr(LeafIdx(usize::MAX))
    }
}

impl IndexContent for Marker {
    fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool {
        debug_assert!(offset > 0);

        match (self, other) {
            (Self::InsPtr(p1), Self::InsPtr(p2)) => p1 == p2,
            (Self::Del(a), Self::Del(b)) => {
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
            Self::InsPtr(_) => *self,
            Self::Del(DelRange {target: lv, fwd: true}) => Self::Del(DelRange::new(*lv + offset, true)),
            Self::Del(DelRange {target: lv, fwd: false}) => Self::Del(DelRange::new(*lv - offset, false)),
        }
    }

    fn eq(&self, other: &Self, upto_len: usize) -> bool {
        debug_assert!(upto_len >= 1);

        self == other || if let (Self::Del(a), Self::Del(b)) = (self, other) {
            // We can only save equality if upto_len == 1, one of them is reversed, and the target is off by 1.
            upto_len == 1 && (
                a.fwd && !b.fwd && a.target + 1 == b.target
            ) || (
                !a.fwd && b.fwd && a.target == b.target + 1
            )
        } else { false }
    }
}

#[cfg(test)]
mod tests {
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