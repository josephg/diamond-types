use smallvec::smallvec;
use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanCtx};
use crate::{CRDTSpan, DTRange, LocalVersion, Time};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CGEntry {
    pub start: Time,
    pub parents: LocalVersion,
    pub span: CRDTSpan,
}

impl Default for CGEntry {
    fn default() -> Self {
        CGEntry {
            start: 0,
            parents: Default::default(),
            span: CRDTSpan {
                agent: 0,
                seq_range: (0..0).into()
            }
        }
    }
}

impl HasLength for CGEntry {
    fn len(&self) -> usize {
        self.span.len()
    }
}

impl MergableSpan for CGEntry {
    fn can_append(&self, other: &Self) -> bool {
        let end = self.start + self.len();
        (end == other.start)
            && other.parents_are_trivial()
            && self.span.can_append(&other.span)
    }

    fn append(&mut self, other: Self) {
        self.span.append(other.span)
        // Other parents don't matter.
    }
}

impl CGEntry {
    pub fn parents_are_trivial(&self) -> bool {
        self.parents.len() == 1
            && self.parents[0] == self.start - 1
    }

    pub fn time_span(&self) -> DTRange {
        (self.start..self.start + self.len()).into()
    }

    pub fn clear(&mut self) {
        self.span.seq_range.clear()
    }
}

impl SplitableSpanCtx for CGEntry {
    type Ctx = ();

    #[inline]
    fn truncate_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        let other_span = self.span.truncate(at);

        Self {
            start: self.start + at,
            parents: smallvec![self.start + at - 1],
            span: other_span
        }
    }
}