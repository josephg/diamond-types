// Should this be called range_tree?
use std::cmp::Ordering;
use smartstring::alias::{String as SmartString};

pub type ClientName = SmartString;
pub type AgentId = u16;
// pub type ClientSeq = u32;


// More common/correct to use usize here but this will be fine in practice and faster.
pub type ItemCount = u32;

pub const CLIENT_INVALID: AgentId = AgentId::MAX;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CRDTLocation {
    pub agent: AgentId,
    pub seq: u32,
}

impl Default for CRDTLocation {
    fn default() -> Self {
        CRDTLocation {
            agent: CLIENT_INVALID,
            seq: u32::MAX
        }
    }
}

pub const CRDT_DOC_ROOT: CRDTLocation = CRDTLocation {
    agent: CLIENT_INVALID,
    seq: 0
};

impl PartialOrd for CRDTLocation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.agent != other.agent {
            None
        } else {
            Some(self.seq.cmp(&other.seq))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalOp {
    pub pos: usize,
    pub ins_content: SmartString,
    pub del_span: usize
}

// From https://github.com/rust-lang/rfcs/issues/997:
pub trait IndexGet<Idx: ?Sized> {
    type Output;
    fn index_get(&self, index: Idx) -> Self::Output;
}