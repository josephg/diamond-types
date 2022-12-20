use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanHelpers};
use crate::{DTRange, Frontier, LV};
use crate::causalgraph::agent_span::AgentSpan;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CGEntry {
    pub start: LV,
    pub parents: Frontier,
    pub span: AgentSpan,
}

impl Default for CGEntry {
    fn default() -> Self {
        CGEntry {
            start: 0,
            parents: Default::default(),
            span: AgentSpan {
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

impl SplitableSpanHelpers for CGEntry {
    #[inline]
    fn truncate_h(&mut self, at: usize) -> Self {
        let other_span = self.span.truncate(at);

        Self {
            start: self.start + at,
            parents: Frontier::new_1(self.start + at - 1),
            span: other_span
        }
    }
}