//! This is a rewrite from diamond type lists to prototype out the positional update approach.
//! It is not yet feature complete in any way.
//!
//! This module should not share any code with list/.

pub mod operation;
mod timedag;
mod list;
mod check;
mod history;
mod branch;

use jumprope::JumpRope;
use smallvec::SmallVec;
use crate::rle::{KVPair, RleVec};
use smartstring::alias::{String as SmartString};
use crate::list::operation::PositionalComponent;
use crate::list::timedag::HistoryEntry;
use crate::localtime::TimeSpan;
use crate::remotespan::CRDTSpan;

// TODO: Consider changing this to u64 to add support for very long lived documents even on 32 bit
// systems.
pub type Time = usize;

pub type Branch = SmallVec<[Time; 4]>;


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


#[derive(Debug)]
pub struct ListCRDT {
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

    // TODO: Replace me with a compact form of this data.
    operations: RleVec<KVPair<PositionalComponent>>,

    /// The set of txn orders with no children in the document. With a single writer this will
    /// always just be the last order we've seen.
    ///
    /// Never empty. Starts at usize::max (which is the root order).
    frontier: Branch,


    // /// Transaction metadata (succeeds, parents) for all operations on this document. This is used
    // /// for `diff` and `branchContainsVersion` calls on the document, which is necessary to merge
    // /// remote changes.
    // ///
    // /// Along with deletes, this essentially contains the time DAG.
    // ///
    // /// TODO: Consider renaming this field
    history: RleVec<HistoryEntry>,

    // Temporary. This will be moved out into a reference to another data structure I think.
    text_content: Option<JumpRope>,

    // /// This is a big ol' string containing everything that's been deleted (self.deletes) in order.
    // deleted_content: Option<String>,
}
