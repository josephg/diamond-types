//! This module contains all the code to handle list CRDTs.
//!
//! Some code in here will be moved out when diamond types supports more data structures.
//!
//! Currently this code only supports lists of unicode characters (text documents). Support for
//! more data types will be added over time.

use jumprope::JumpRope;
use smallvec::SmallVec;
use smartstring::alias::String as SmartString;

use crate::list::operation::InsDelTag;
use crate::list::history::History;
use crate::list::internal_op::OperationInternal;
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

mod merge;
mod oplog;
mod branch;
pub mod encoding;
pub mod remote_ids;
mod internal_op;
mod eq;
mod oplog_merge;

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
    ///
    /// A single agent ID might be used to modify multiple concurrent branches. Because of this, and
    /// the propensity of diamond types to reorder operations for performance, the
    /// time spans here will *almost* always (but not always) be monotonically increasing. Eg, they
    /// might be ordered as (0, 2, 1). This will only happen when changes are concurrent. The order
    /// of time spans must always obey the partial order of changes. But it will not necessarily
    /// agree with the order amongst time spans.
    item_times: RleVec<KVPair<TimeSpan>>,
}

// TODO!
// trait InlineReplace<T> {
//     fn insert(pos: usize, vals: &[T]);
//     fn remove(pos: usize, num: usize);
// }
//
// trait ListValueType {
//     type EditableList: InlineReplace<T>;
//
// }

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

    /// This contains all content ever inserted into the document, in time order (not document
    /// order). This object is indexed by the operation set.
    ins_content: String,
    del_content: String,
    // TODO: Replace me with a compact form of this data.
    operations: RleVec<KVPair<OperationInternal>>,

    /// Transaction metadata (succeeds, parents) for all operations on this document. This is used
    /// for `diff` and `branchContainsVersion` calls on the document, which is necessary to merge
    /// remote changes.
    ///
    /// Along with deletes, this essentially contains the time DAG.
    ///
    /// TODO: Consider renaming this field
    /// TODO: Remove pub marker.
    history: History,

    /// This is the frontier of the entire oplog. So, if you merged every change we store into a
    /// branch, this is the frontier of that branch.
    ///
    /// This is only stored as a convenience - we could recalculate it as needed from history. But
    /// it takes up very little space, and its mildly convenient. So here it is!
    frontier: Frontier,
}

/// This is the default (obvious) construction for a list.
#[derive(Debug, Clone)]
pub struct ListCRDT {
    pub branch: Branch,
    pub ops: OpLog,
}

fn switch<T>(tag: InsDelTag, ins: T, del: T) -> T {
    match tag {
        InsDelTag::Ins => ins,
        InsDelTag::Del => del,
    }
}
