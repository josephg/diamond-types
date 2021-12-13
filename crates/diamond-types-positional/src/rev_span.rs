use std::ops::Range;
use rle::{HasLength, MergableSpan, SplitableSpan};
use crate::localtime::TimeSpan;
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};

/// This is a TimeSpan which can be either a forwards range (1,2,3) or backwards (3,2,1), depending
/// on what is most efficient.
///
/// The inner span is always "forwards" - where span.start <= span.end. But if rev is true, this
/// span should be iterated in the reverse order.
///
/// Note that time spans are used with some other (more complex) semantics in operations. The
/// implementation of SplitableSpan and MergableSpan here uses (assumes) the span refers to absolute
/// positions. So:
///     (0..10) + (10..20) = (0..20)
/// This is *not true* for example with delete operations, where:
///     (Del 0..10) + (Del 0..10) = (Del 0..20)
#[derive(Copy, Clone, Debug, Eq, Default)] // Default needed for ContentTree.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct TimeSpanRev {
    /// The inner span.
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub span: TimeSpan,

    /// If target is `1..4` then we either reference `1,2,3` (rev=false) or `3,2,1` (rev=true).
    /// TODO: Consider swapping this, and making it a fwd variable (default true).
    pub fwd: bool,
}

impl TimeSpanRev {
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
    pub fn range(&self, offset_start: usize, offset_end: usize) -> TimeSpan {
        debug_assert!(offset_start <= offset_end);
        debug_assert!(self.span.start + offset_start <= self.span.end);
        debug_assert!(self.span.start + offset_end <= self.span.end);

        if self.fwd {
            // Simple case.
            TimeSpan {
                start: self.span.start + offset_start,
                end: self.span.start + offset_end
            }
        } else {
            TimeSpan {
                start: self.span.end - offset_end,
                end: self.span.end - offset_start
            }
        }
    }
}

impl From<TimeSpan> for TimeSpanRev {
    fn from(target: TimeSpan) -> Self {
        TimeSpanRev {
            span: target,
            fwd: true,
        }
    }
}
impl From<Range<usize>> for TimeSpanRev {
    fn from(range: Range<usize>) -> Self {
        TimeSpanRev {
            span: range.into(),
            fwd: true,
        }
    }
}

impl PartialEq for TimeSpanRev {
    fn eq(&self, other: &Self) -> bool {
        // Custom eq because if the two deletes have length 1, we don't care about rev.
        self.span == other.span && (self.fwd == other.fwd || self.span.len() <= 1)
    }
}

impl HasLength for TimeSpanRev {
    fn len(&self) -> usize { self.span.len() }
}

impl SplitableSpan for TimeSpanRev {
    fn truncate(&mut self, at: usize) -> Self {
        TimeSpanRev {
            span: if self.fwd {
                self.span.truncate(at)
            } else {
                self.span.truncate_keeping_right(self.len() - at)
            },
            fwd: self.fwd,
        }
    }
}

impl MergableSpan for TimeSpanRev {
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
        let fwd = TimeSpanRev {
            span: (1..4).into(),
            fwd: true
        };
        assert_eq!(fwd.split(1), (
            TimeSpanRev {
                span: (1..2).into(),
                fwd: true
            },
            TimeSpanRev {
                span: (2..4).into(),
                fwd: true
            }
        ));

        let rev = TimeSpanRev {
            span: (1..4).into(),
            fwd: false
        };
        assert_eq!(rev.split(1), (
            TimeSpanRev {
                span: (3..4).into(),
                fwd: false
            },
            TimeSpanRev {
                span: (1..3).into(),
                fwd: false
            }
        ));
    }

    #[test]
    fn splitable_mergable() {
        test_splitable_methods_valid(TimeSpanRev {
            span: (1..5).into(),
            fwd: true
        });

        test_splitable_methods_valid(TimeSpanRev {
            span: (1..5).into(),
            fwd: false
        });
    }

    #[test]
    fn at_offset() {
        for fwd in [true, false] {
            let span = TimeSpanRev {
                span: (1..5).into(),
                fwd
            };

            for offset in 1..span.len() {
                let (a, b) = span.split(offset);
                assert_eq!(span.time_at_offset(offset - 1), a.time_at_offset(offset - 1));
                assert_eq!(span.time_at_offset(offset), b.time_at_offset(0));
                // assert_eq!(span.time_at_offset(offset), a.time_at_offset(0));
                // assert_eq!(span.offset_at_time())
            }
        }
    }
}