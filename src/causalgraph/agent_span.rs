// TODO: Consider moving me into agent_assignment/.

use std::ops::Range;
// use content_tree::ContentLength;
use rle::{HasLength, MergableSpan, Searchable, SplitableSpan, SplitableSpanHelpers};
use crate::AgentId;
use crate::dtrange::DTRange;

/// (agent_id, seq) pair. The agent ID is an integer which maps to a local string via causal graph.
pub type AgentVersion = (AgentId, usize);

/// An AgentSpan represents a sequential span of (agent, seq) versions.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AgentSpan {
    pub agent: AgentId,
    pub seq_range: DTRange,
}

impl From<(AgentId, DTRange)> for AgentSpan {
    fn from((agent, seq_range): (AgentId, DTRange)) -> Self {
        AgentSpan { agent, seq_range }
    }
}

impl From<(AgentId, Range<usize>)> for AgentSpan {
    fn from((agent, seq_range): (AgentId, Range<usize>)) -> Self {
        AgentSpan { agent, seq_range: seq_range.into() }
    }
}

impl From<AgentVersion> for AgentSpan {
    fn from((agent, seq): AgentVersion) -> Self {
        AgentSpan { agent, seq_range: seq.into() }
    }
}

impl Searchable for AgentSpan {
    type Item = AgentVersion;

    fn get_offset(&self, (agent, seq): AgentVersion) -> Option<usize> {
        // let r = self.loc.seq .. self.loc.seq + (self.len.abs() as usize);
        // self.loc.agent == loc.agent && entry.get_seq_range().contains(&loc.seq)
        if self.agent == agent {
            self.seq_range.get_offset(seq)
        } else { None }
    }

    fn at_offset(&self, offset: usize) -> AgentVersion {
        assert!(offset < self.len());
        (self.agent, self.seq_range.start + offset)
    }
}

impl HasLength for AgentSpan {
    /// this length refers to the length that we'll use when we call truncate(). So this does count
    /// deletes.
    fn len(&self) -> usize {
        self.seq_range.len()
    }
}
impl SplitableSpanHelpers for AgentSpan {
    fn truncate_h(&mut self, at: usize) -> Self {
        AgentSpan {
            agent: self.agent,
            seq_range: self.seq_range.truncate(at)
        }
    }

    fn truncate_keeping_right_h(&mut self, at: usize) -> Self {
        AgentSpan {
            agent: self.agent,
            seq_range: self.seq_range.truncate_keeping_right(at)
        }
    }
}
impl MergableSpan for AgentSpan {
    fn can_append(&self, other: &Self) -> bool {
        self.agent == other.agent
            && self.seq_range.end == other.seq_range.start
    }

    fn append(&mut self, other: Self) {
        self.seq_range.end = other.seq_range.end;
    }

    fn prepend(&mut self, other: Self) {
        self.seq_range.start = other.seq_range.start;
    }
}
