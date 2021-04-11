// Should this be called range_tree?
use inlinable_string::InlinableString;

pub type ClientName = InlinableString;
pub type AgentId = u16;
// pub type ClientSeq = u32;


// More common/correct to use usize here but this will be fine in practice and faster.
pub type ItemCount = u32;

pub const CLIENT_INVALID: AgentId = AgentId::MAX;

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
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