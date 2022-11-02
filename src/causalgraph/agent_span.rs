use std::cmp::Ordering;
use std::ops::Range;
use content_tree::ContentLength;
use rle::{HasLength, MergableSpan, Searchable, SplitableSpan, SplitableSpanHelpers};
use crate::AgentId;
use crate::dtrange::DTRange;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AgentVersion {
    pub agent: AgentId,
    pub seq: usize,
}

// TODO: Make this crate-private, and make it a tuple.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AgentSpan {
    pub agent: AgentId,
    pub seq_range: DTRange,
}

impl From<(AgentId, usize)> for AgentVersion {
    fn from((agent, seq): (AgentId, usize)) -> Self {
        AgentVersion { agent, seq }
    }
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

// impl Default for CRDTId {
//     fn default() -> Self {
//         CRDTId {
//             agent: CLIENT_INVALID,
//             seq: u64::MAX
//         }
//     }
// }

pub const ROOT_CRDT_ID: usize = usize::MAX;
pub const ROOT_CRDT_ID_GUID: AgentVersion = AgentVersion {
    agent: AgentId::MAX,
    seq: 0
};

impl PartialOrd for AgentVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.agent != other.agent {
            None
        } else {
            Some(self.seq.cmp(&other.seq))
        }
    }
}

// // From https://github.com/rust-lang/rfcs/issues/997:
// pub trait IndexGet<Idx: ?Sized> {
//     type Output;
//     fn index_get(&self, index: Idx) -> Self::Output;
// }


impl Searchable for AgentSpan {
    type Item = AgentVersion;

    fn get_offset(&self, loc: AgentVersion) -> Option<usize> {
        // let r = self.loc.seq .. self.loc.seq + (self.len.abs() as usize);
        // self.loc.agent == loc.agent && entry.get_seq_range().contains(&loc.seq)
        if self.agent == loc.agent {
            self.seq_range.get_offset(loc.seq)
        } else { None }
    }

    fn at_offset(&self, offset: usize) -> AgentVersion {
        assert!(offset < self.len());
        AgentVersion {
            agent: self.agent,
            seq: self.seq_range.start + offset
        }
    }
}

impl ContentLength for AgentSpan {
    fn content_len(&self) -> usize {
        self.seq_range.len()
    }

    fn content_len_at_offset(&self, offset: usize) -> usize {
        offset
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

impl From<AgentVersion> for AgentSpan {
    fn from(guid: AgentVersion) -> Self {
        Self {
            agent: guid.agent,
            seq_range: guid.seq.into()
        }
    }
}