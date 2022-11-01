use std::cmp::Ordering;
use content_tree::ContentLength;
use rle::{HasLength, MergableSpan, Searchable, SplitableSpan, SplitableSpanHelpers};
use crate::{AgentId, ROOT_AGENT};
use crate::dtrange::DTRange;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CRDTGuid {
    pub agent: AgentId,
    pub seq: usize,
}
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CRDTSpan {
    pub agent: AgentId,
    pub seq_range: DTRange,
}

// impl Default for CRDTId {
//     fn default() -> Self {
//         CRDTId {
//             agent: CLIENT_INVALID,
//             seq: u64::MAX
//         }
//     }
// }

pub const CRDT_DOC_ROOT: CRDTGuid = CRDTGuid {
    agent: ROOT_AGENT,
    seq: 0
};

impl PartialOrd for CRDTGuid {
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


impl Searchable for CRDTSpan {
    type Item = CRDTGuid;

    fn get_offset(&self, loc: CRDTGuid) -> Option<usize> {
        // let r = self.loc.seq .. self.loc.seq + (self.len.abs() as usize);
        // self.loc.agent == loc.agent && entry.get_seq_range().contains(&loc.seq)
        if self.agent == loc.agent {
            self.seq_range.get_offset(loc.seq)
        } else { None }
    }

    fn at_offset(&self, offset: usize) -> CRDTGuid {
        assert!(offset < self.len());
        CRDTGuid {
            agent: self.agent,
            seq: self.seq_range.start + offset
        }
    }
}

impl ContentLength for CRDTSpan {
    fn content_len(&self) -> usize {
        self.seq_range.len()
    }

    fn content_len_at_offset(&self, offset: usize) -> usize {
        offset
    }
}

impl HasLength for CRDTSpan {
    /// this length refers to the length that we'll use when we call truncate(). So this does count
    /// deletes.
    fn len(&self) -> usize {
        self.seq_range.len()
    }
}
impl SplitableSpanHelpers for CRDTSpan {
    fn truncate_h(&mut self, at: usize) -> Self {
        CRDTSpan {
            agent: self.agent,
            seq_range: self.seq_range.truncate(at)
        }
    }

    fn truncate_keeping_right_h(&mut self, at: usize) -> Self {
        CRDTSpan {
            agent: self.agent,
            seq_range: self.seq_range.truncate_keeping_right(at)
        }
    }
}
impl MergableSpan for CRDTSpan {
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

impl From<CRDTGuid> for CRDTSpan {
    fn from(guid: CRDTGuid) -> Self {
        Self {
            agent: guid.agent,
            seq_range: guid.seq.into()
        }
    }
}