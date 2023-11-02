use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};

pub mod alloc;

pub type AgentId = u16;
// pub type ClientSeq = u32;

// More common/correct to use usize here but this will be fine in practice and faster.
pub type ItemCount = u32;

pub const CLIENT_INVALID: AgentId = AgentId::MAX;

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct CRDTId {
    pub agent: AgentId,
    pub seq: u32,
}

impl Debug for CRDTId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.agent == CLIENT_INVALID {
            f.write_str("ROOT")
        } else {
            // f.debug_tuple("CRDTId")
            f.debug_list()
                .entry(&self.agent)
                .entry(&self.seq)
                .finish()
        }
    }
}

impl Default for CRDTId {
    fn default() -> Self {
        CRDTId {
            agent: CLIENT_INVALID,
            seq: u32::MAX
        }
    }
}

pub const CRDT_DOC_ROOT: CRDTId = CRDTId {
    agent: CLIENT_INVALID,
    seq: 0
};

impl PartialOrd for CRDTId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.agent != other.agent {
            None
        } else {
            Some(self.seq.cmp(&other.seq))
        }
    }
}

// From https://github.com/rust-lang/rfcs/issues/997:
pub trait IndexGet<Idx: ?Sized> {
    type Output;
    fn index_get(&self, index: Idx) -> Self::Output;
}
