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
use crate::list::internal_op::{OperationCtx, OperationInternal};
use crate::localtime::TimeSpan;
use crate::remotespan::CRDTSpan;
use crate::rle::{KVPair, RleVec};

pub mod operation;
mod history;
mod list;
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

#[cfg(test)]
mod fuzzer_tools;
#[cfg(test)]
mod oplog_merge_fuzzer;

#[cfg(feature = "serde")]
mod serde;
mod buffered_iter;

// TODO: Consider changing this to u64 to add support for very long lived documents even on 32 bit
// systems.
pub type Time = usize;

/// A LocalVersion is a set of local Time values which point at the set of changes with no children
/// at this point in time. When there's a single writer this will
/// always just be the last order we've seen.
///
/// This is never empty.
///
/// At the start of time (when there are no changes), LocalVersion is usize::max (which is the root
/// order).
pub type LocalVersion = SmallVec<[Time; 4]>;

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

/// A branch stores a checkout / snapshot of a document at some moment in time. Branches are the
/// normal way for editors to interact with an [OpLog](OpLog), which stores the actual change set.
///
/// Internally, branches simply have two fields:
///
/// - Content (Ie, the list with all its values)
/// - Version
///
/// At the root version (the start of history), a branch is always empty.
///
/// Branches obey a very strict mutability rule: Whenever the content changes, the version
/// *must change*. A branch (with content at some named version) is always valid. But future changes
/// can always be merged in to the branch via [`branch.merge()`](Branch::merge).
///
/// Branches also provide a simple way to edit documents, via the [`insert`](Branch::insert) and
/// [`delete`](Branch::delete) methods. These methods append new operations to the oplog, and modify
/// the branch to contain the named changes.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Branch {
    /// The version the branch is currently at. This is used to track which changes the branch has
    /// or has not locally merged.
    ///
    /// This field is public for convenience, but you should never modify it directly. Instead use
    /// the associated functions on Branch.
    version: LocalVersion,

    /// The document's content.
    content: JumpRope,
}

/// An OpLog is a collection of Diamond Types operations, stored in a super fancy compact way. Each
/// operation has a number of fields:
///
/// - Type (insert or delete)
/// - ID (unique Agent ID + sequence number pair)
/// - Parents - which names the document's version right before this edit was created.
/// - Origin edit position (when the edit was created) + length
/// - Content of what was inserted or deleted. Storing this is optional.
///
/// The OpLog structure stores all this data in a SoA (Struct Of Arrays) format, which lets us
/// run-length encode each field individually. This makes all operations significantly faster, but
/// it makes the code to read changes significantly more complex.
///
/// The OpLog API supports:
///
/// - Reading operations (via a few iterator methods + helpers)
/// - Reading transformed operations (This is useful for applying changes to a local document state)
/// - Encoding and decoding operation sets to and from binary formats
/// - Appending operations which have been created locally or remotely
/// - Creating a "checkout" (snapshot) of the document at any requested point in time
/// - Interacting with the time DAG, to merge changes or list which operations a peer might be
///   missing.
///
/// Well, it should. The public API is still a work in progress. I'm going to be tweaking method
/// names and things a fair bit before we hit 1.0.
#[derive(Debug, Clone)]
pub struct OpLog {
    /// The ID of the document (if any). This is useful if you want to give a document a GUID or
    /// something to make sure you're merging into the right place.
    ///
    /// Optional - only used if you set it.
    doc_id: Option<SmartString>,

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
    operation_ctx: OperationCtx,
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

    /// This is the LocalVersion for the entire oplog. So, if you merged every change we store into
    /// a branch, this is the version of that branch.
    ///
    /// This is only stored as a convenience - we could recalculate it as needed from history when
    /// needed, but thats a hassle. And it takes up very little space, and its very convenient to
    /// have on hand! So here it is.
    version: LocalVersion,
}

/// This is a simple helper structure which wraps an [`OpLog`](OpLog) and [`Branch`](Branch)
/// together into a single structure to make edits easy.
///
/// When getting started with diamond types, this is the API you probably want to use.
///
/// The times you don't want to use a ListCRDT:
///
/// - Nodes often don't care about the current document state. If you're using DT in a context
///   where you're mostly just passing patches around and you don't actually need a live copy of the
///   document state, just use an OpLog. You can always call [`oplog.checkout()`](OpLog::checkout)
///   to figure out what the document looks like at any specified moment in time.
/// - If you're interacting with a document with multiple branches, you'll probably want to
///   instantiate the oplog (and any visible branches) separately.
#[derive(Debug, Clone)]
pub struct ListCRDT {
    pub branch: Branch,
    pub oplog: OpLog,
}

fn switch<T>(tag: InsDelTag, ins: T, del: T) -> T {
    match tag {
        InsDelTag::Ins => ins,
        InsDelTag::Del => del,
    }
}
