//! This is a rewrite from diamond type lists to prototype out the positional update approach.
//! It is not yet feature complete in any way.
//!
//! This module should not share any code with list/.

use jumprope::JumpRope;
use smallvec::SmallVec;
use smartstring::alias::String as SmartString;

use crate::list::operation::Operation;
use crate::list::history::History;
use crate::localtime::TimeSpan;
use crate::remotespan::CRDTSpan;
use crate::rle::{KVPair, RleVec};

pub mod operation;
mod history;
pub mod list;
mod check;
mod history_tools;
mod frontier;
mod op_iter;

// m1 is still wip.
// mod m1;

mod m2;
mod oplog;
mod branch;
pub mod encoding;
mod remote_ids;
mod internal_op;

// TODO: Consider changing this to u64 to add support for very long lived documents even on 32 bit
// systems.
pub type Time = usize;

pub type Frontier = SmallVec<[Time; 4]>;


#[derive(Clone, Debug)]
struct ClientData {
    /// Used to map from client's name / hash to its numerical ID.
    name: SmartString,

    /// This is a packed RLE in-order list of all operations from this client.
    ///
    /// Each entry in this list is grounded at the client's sequence number and maps to the span of
    /// local time entries.
    item_orders: RleVec<KVPair<TimeSpan>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Branch {
    /// The set of txn orders with no children in the document. With a single writer this will
    /// always just be the last order we've seen.
    ///
    /// Never empty. Starts at usize::max (which is the root order).
    pub frontier: Frontier,

    pub content: JumpRope,
}

#[derive(Debug, Clone)]
pub struct OpLog {
    /// This is a bunch of ranges of (item order -> CRDT location span).
    /// The entries always have positive len.
    ///
    /// This is used to map Local time -> External CRDT locations.
    ///
    /// List is packed.
    client_with_localtime: RleVec<KVPair<CRDTSpan>>,

    /// For each client, we store some data (above). This is indexed by AgentId.
    ///
    /// This is used to map external CRDT locations -> Order numbers.
    client_data: Vec<ClientData>,

    ins_content: String,
    del_content: String,
    // TODO: Replace me with a compact form of this data.
    operations: RleVec<KVPair<Operation>>,

    /// Transaction metadata (succeeds, parents) for all operations on this document. This is used
    /// for `diff` and `branchContainsVersion` calls on the document, which is necessary to merge
    /// remote changes.
    ///
    /// Along with deletes, this essentially contains the time DAG.
    ///
    /// TODO: Consider renaming this field
    pub history: History,

    /// This is the frontier of the entire oplog. So, if you merged every change we store into a
    /// branch, this is the frontier of that branch.
    frontier: Frontier,
}

/// This is the default (obvious) construction for a list.
#[derive(Debug, Clone)]
pub struct ListCRDT {
    pub branch: Branch,
    pub ops: OpLog,
}

// impl OpSet {
//     pub fn blah(&self, a: &[Time], b: &[Time]) -> bool {
//         self.history.diff(a, b).common_branch[0] == ROOT_TIME
//     }
// }