use rle::{HasLength, MergableSpan};
use crate::{CRDTSpan, DTRange, LocalVersion, Time};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CGEntry {
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
