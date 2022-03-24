use std::ops::Range;
use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanHelpers};
use crate::dtrange::DTRange;

#[cfg(feature = "serde")]
use serde_crate::{Deserialize};

/// This is a DTRange which can be either a forwards range (1,2,3) or backwards (3,2,1).
///
/// The inner span is always "forwards" - where span.start <= span.end. But if fwd is false, this
/// span should be iterated in the reverse order.
///
/// Note that time spans are used with some other (more complex) semantics in operations. The
/// implementation of SplitableSpan and MergableSpan here uses (assumes) the span refers to absolute
/// positions. So:
///     (0..10) + (10..20) = (0..20)
/// This is *not true* for example with delete operations, where:
///     (Del 0..10) + (Del 0..10) = (Del 0..20)
#[derive(Copy, Clone, Debug, Eq, Default)] // Default needed for ContentTree.
#[cfg_attr(feature = "serde", derive(Deserialize), serde(crate="serde_crate"))]
pub struct RangeRev {
    /// The inner span.
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub span: DTRange,

    /// If target is `1..4` then we either reference `1,2,3` (rev=false) or `3,2,1` (rev=true).
    /// TODO: Consider swapping this, and making it a fwd variable (default true).
    pub fwd: bool,
}

impl RangeRev {
    // Works, but unused.
    // pub fn offset_at_time(&self, time: Time) -> usize {
    //     if self.reversed {
    //         self.span.end - time - 1
    //     } else {
    //         time - self.span.start
    //     }
    // }

    #[allow(unused)]
    pub fn time_at_offset(&self, offset: usize) -> usize {
        if self.fwd {
            self.span.start + offset
        } else {
            self.span.end - offset - 1
        }
    }

    // pub fn first(&self) -> Time {
    //     if self.rev { self.span.last() } else { self.span.start }
    // }

    /// Get the relative range from start + offset_start to start + offset_end.
    ///
    /// This is useful because reversed ranges are weird.
    pub fn range(&self, offset_start: usize, offset_end: usize) -> DTRange {
        debug_assert!(offset_start <= offset_end);
        debug_assert!(self.span.start + offset_start <= self.span.end);
        debug_assert!(self.span.start + offset_end <= self.span.end);

        if self.fwd {
            // Simple case.
            DTRange {
                start: self.span.start + offset_start,
                end: self.span.start + offset_end
            }
        } else {
            DTRange {
                start: self.span.end - offset_end,
                end: self.span.end - offset_start
            }
        }
    }
}

impl From<DTRange> for RangeRev {
    fn from(target: DTRange) -> Self {
        RangeRev {
            span: target,
            fwd: true,
        }
    }
}
impl From<Range<usize>> for RangeRev {
    fn from(range: Range<usize>) -> Self {
        RangeRev {
            span: range.into(),
            fwd: true,
        }
    }
}

impl PartialEq for RangeRev {
    fn eq(&self, other: &Self) -> bool {
        // Custom eq because if the two deletes have length 1, we don't care about rev.
        self.span == other.span && (self.fwd == other.fwd || self.span.len() <= 1)
    }
}

impl HasLength for RangeRev {
    fn len(&self) -> usize { self.span.len() }
}

impl SplitableSpanHelpers for RangeRev {
    fn truncate_h(&mut self, at: usize) -> Self {
        RangeRev {
            span: if self.fwd {
                self.span.truncate(at)
            } else {
                self.span.truncate_keeping_right(self.len() - at)
            },
            fwd: self.fwd,
        }
    }
}

impl MergableSpan for RangeRev {
    fn can_append(&self, other: &Self) -> bool {
        // Can we append forward?
        let self_len_1 = self.len() == 1;
        let other_len_1 = other.len() == 1;
        if (self_len_1 || self.fwd) && (other_len_1 || other.fwd)
            && other.span.start == self.span.end {
            return true;
        }

        // Can we append backwards?
        if (self_len_1 || !self.fwd) && (other_len_1 || !other.fwd)
            && other.span.end == self.span.start {
            return true;
        }

        false
    }

    fn append(&mut self, other: Self) {
        debug_assert!(self.can_append(&other));
        self.fwd = other.span.start >= self.span.start;

        if self.fwd {
            self.span.end = other.span.end;
        } else {
            self.span.start = other.span.start;
        }
    }
}


// pub(super) fn btree_set<E: SplitableSpan + MergableSpan + HasLength>(map: &mut BTreeMap<usize, E>, key: usize, val: E) {
//     let end = key + val.len();
//     let mut range = map.range_mut((Included(0), Included(end)));
//     range.next_back()
// }

#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;
    use super::*;

    #[test]
    fn split_fwd_rev() {
        let fwd = RangeRev {
            span: (1..4).into(),
            fwd: true
        };
        assert_eq!(fwd.split_h(1), (
            RangeRev {
                span: (1..2).into(),
                fwd: true
            },
            RangeRev {
                span: (2..4).into(),
                fwd: true
            }
        ));

        let rev = RangeRev {
            span: (1..4).into(),
            fwd: false
        };
        assert_eq!(rev.split_h(1), (
            RangeRev {
                span: (3..4).into(),
                fwd: false
            },
            RangeRev {
                span: (1..3).into(),
                fwd: false
            }
        ));
    }

    #[test]
    fn splitable_mergable() {
        test_splitable_methods_valid(RangeRev {
            span: (1..5).into(),
            fwd: true
        });

        test_splitable_methods_valid(RangeRev {
            span: (1..5).into(),
            fwd: false
        });
    }

    #[test]
    fn at_offset() {
        for fwd in [true, false] {
            let span = RangeRev {
                span: (1..5).into(),
                fwd
            };

            for offset in 1..span.len() {
                let (a, b) = span.split_h(offset);
                assert_eq!(span.time_at_offset(offset - 1), a.time_at_offset(offset - 1));
                assert_eq!(span.time_at_offset(offset), b.time_at_offset(0));
                // assert_eq!(span.time_at_offset(offset), a.time_at_offset(0));
                // assert_eq!(span.offset_at_time())
            }
        }
    }
}