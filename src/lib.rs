//! > NOTE: This documentation is out of date with the current DT code
//!
//! This is a super fast CRDT implemented in rust. It currently only supports plain text documents
//! but the plan is to support all kinds of data.
//!
//! Diamond types is built on top of two core abstractions:
//!
//! 1. The [Operation Log](list::OpLog)
//! 2. [Branches](list::Branch)
//!
//! A branch is a copy of the document state at some point in time. The most common & useful way to
//! use branches is to make a single branch at the most recent version of the document. When more
//! changes come in, a branch can be moved forward in time by calling [`merge`](list::Branch::merge).
//!
//! Branches in diamond types aren't the same as branches in git. They're a lower level construct.
//! Diamond types doesn't store a list of the active branches in your data set. A branch is much
//! simplier than that - internally its just a temporary in-memory tuple of
//! (version, document state).
//!
//! Branches can change over time by referencing the *Operation Log* (OpLog). The oplog is an
//! append-only log of all the changes which have happened to a document over time. The operation
//! log can be replayed to generate a branch at any point of time within its range.
//!
//! For every operation in the oplog we store a few fields:
//!
//! - What the change actually is (eg *insert 'hi' at position 20*)
//! - Parents (A logical clock of *when* an operation happened)
//! - ID (Agent & Sequence number). The agent can be used to figure out who made the change.
//!
//! ## Example
//!
//! For local edits to an oplog, just use [`oplog.add_insert`](list::OpLog::add_insert) or
//! [`oplog.add_delete_without_content`](list::OpLog::add_delete_without_content):
//!
//! ```
//! use diamond_types::list::*;
//!
//! let mut oplog = ListOpLog::new();
//! let fred = oplog.get_or_create_agent_id("fred");
//! oplog.add_insert(fred, 0, "abc");
//! oplog.add_delete_without_content(fred, 1..2); // Delete the 'b'
//! ```
//!
//! There are also other methods like [`oplog.add_insert_at`](list::OpLog::add_insert_at) which
//! append a change at some specific point in time. This is useful if you want to append a change to
//! a branch.
//!
//! To create a branch from an oplog, use [`Branch::new` methods](list::Branch::new_at_tip):
//!
//! ```
//! use diamond_types::list::*;
//! let mut oplog = ListOpLog::new();
//! // ...
//! let mut branch = ListBranch::new_at_tip(&oplog);
//! // Equivalent to let mut branch = Branch::new_at_local_version(&oplog, oplog.get_local_version());
//! println!("branch content {}", branch.content().to_string());
//! ```
//!
//! Once a branch has been created, you can merge new changes using [`branch.merge`](list::Branch::merge):
//!
//! ```
//! use diamond_types::list::*;
//! let mut oplog = ListOpLog::new();
//! // ...
//! let mut branch = ListBranch::new_at_tip(&oplog);
//! let george = oplog.get_or_create_agent_id("george");
//! oplog.add_insert(george, 0, "asdf");
//! branch.merge(&oplog, oplog.local_frontier_ref());
//! ```
//!
//! If you aren't using branches, you can use the simplified [`ListCRDT` API](list::ListCRDT). The
//! ListCRDT struct simply wraps an oplog and a branch together so you don't need to muck about
//! with manual merging. This API is also slightly faster.
//!
//! I'm holding off on adding examples using this API for now because the API is in flux. TODO: Fix!
//!
//!
//! ## Consuming IDs
//!
//! The ID of a change is made up of an agent ID (usually an opaque string) and a sequence number.
//! Each successive change from the same agent will use the next sequence number - eg: (*fred*, 0),
//! (*fred*, 1), (*fred*, 2), etc.
//!
//! But its important to note what constitutes a change! In diamond types, every inserted character
//! or deleted character increments (consumes) a sequence number. Typing a run of characters one at
//! a time is indistinguishable from pasting the same run of characters all at once.
//!
//! Note that this is a departure from other CRDTs. Automerge does not work this way.
//!
//! For example,
//!
//! ```
//! use diamond_types::list::*;
//! let mut oplog = ListOpLog::new();
//! let fred = oplog.get_or_create_agent_id("fred");
//! oplog.add_insert(fred, 0, "a");
//! oplog.add_insert(fred, 1, "b");
//! oplog.add_insert(fred, 2, "c");
//! ```
//!
//! Produces an identical oplog to this:
//!
//! ```
//! use diamond_types::list::*;
//! let mut oplog = ListOpLog::new();
//! let fred = oplog.get_or_create_agent_id("fred");
//! oplog.add_insert(fred, 0, "abc");
//! ```
//!
//! Diamond types does this by very aggressively run-length encoding everything it can whenever
//! possible.
//!
//! ### Warning: Do not reuse IDs 💣!
//!
//! Every ID in diamond types *must be unique*. If two operations are created with the same ID,
//! peers will only merge one of them - and the document state will diverge. This is really bad!
//!
//! Its tempting to reuse agent IDs because they waste disk space. But there's lots of ways to
//! introduce subtle bugs if you try. Disk space is cheap. Bugs are expensive.
//!
//! I recommend instead just generating a new agent ID in every editing session. So, in a text
//! editor, generate an ID in memory when the user opens the document. Don't save the ID to disk.
//! Just discard it when the user's editing session ends.
//!
//!
//! ### Aside on atomic transactions
//!
//! Grouping changes in atomic blocks is out of the scope of diamond types. But you can implement it
//! in the code you write which consumes diamond types. Briefly, either:
//!
//! 1. Make all the changes you want to make atomically in diamond types, but delay sending those
//! changes over the network until you're ready, or
//! 2. Add a special commit message to your network protocol which "commits" marks when a set of
//! operations in the oplog is safe to merge.
//!
//! Diamond types does not (yet) support deleting operations from the oplog. If this matters to you,
//! please start open an issue about it.
//!
//!
//! ## Parents
//!
//! The parents list names the version of the document right before it was changed. An new,
//! empty document always has the version of *ROOT*. After an operation has happened, the version of
//! the document is the same as that operation's ID.
//!
//! Sometimes changes are concurrent. This can happen in realtime - for example, two users type in a
//! collaborative document at the same time. Or it can happen asyncronously - for example, two users
//! edit two different branches, and later merge their results. We can describe what happened with
//! a *time DAG*, where each change is represented by a node in a DAG (Directed Acyclic Graph).
//! Edges represent the *directly after* relationship. See [INTERNALS.md](INTERNALS.md) in this
//! repository for more theoretical information.
//!
//! For example, in this time DAG operations `a` and `b` are concurrent:
//!
//! ```text
//!   ROOT
//!   / \
//!  a   b
//!   \ /
//!    c
//! ```
//!
//! Concurrent changes have some repercussions for the oplog:
//!
//! - The order of changes in the oplog isn't canonical. Other peers may have oplogs with a
//! different order. This is fine. DT uses "local time" numbers heavily internally - which refer to
//! the local index of a change, as if it were stored in an array. But these indexes cannot be
//! shared with other peers. However, the order of changes must always obey the partial order of
//! chronology. If operation A happened before operation B, they must maintain that relative order
//! in the oplog. In the diagram above, the operations could be stored in the order `[a, b, c]` or
//! `[b, a, c]` but not `[a, c, b]` because `c` comes after both `a` and `b`.
//! - We represent a point in time in the oplog using a *list* of (agent, seq) pairs. This list
//! usually only contains one entry - which is the ID of the preceding operation. But sometimes
//! we need to merge two threads of history together. In this case, the parents list names all
//! immediate predecessors. In the diagram above, operation `c` has a parents list of both `a` and
//! `b`.
//!
//! Unlike git (and some other CRDTs), diamond types represents merges *implicitly*. We don't create
//! a special node in the time DAG for merges. Merges simply happen whenever an operation has
//! multiple parents.

#![allow(clippy::module_inception)]
#![allow(unused)] // During dev. TODO: Take me out!

extern crate core;

use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use jumprope::JumpRope;
use smallvec::SmallVec;
use smartstring::alias::String as SmartString;
pub use crate::causalgraph::CausalGraph;
pub use crate::dtrange::DTRange;
use causalgraph::parents::Parents;
use crate::causalgraph::storage::CGStorage;
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::rle::{KVPair, RleVec};
use crate::wal::WriteAheadLog;
use num_enum::TryFromPrimitive;
pub use ::rle::HasLength;
pub use frontier::Frontier;
use crate::causalgraph::agent_span::AgentVersion;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

// use crate::list::internal_op::OperationInternal as TextOpInternal;

pub mod list;
mod rle;
mod dtrange;
mod unicount;
mod rev_range;
pub mod frontier;
mod oplog;
mod check;
mod branch;
mod path;
mod encoding;
pub mod causalgraph;
mod simpledb;
mod operation;
mod wal;

#[cfg(feature = "serde")]
pub(crate) mod serde_helpers;
mod hack;

pub type AgentId = u32;

// TODO: Consider changing this to u64 to add support for very long lived documents even on 32 bit
// systems like wasm32
/// An LV (LocalVersion) is used all over the place internally to identify a single operation.
///
/// A local version (as the name implies) is local-only. Local versions generally need to be
/// converted to RawVersions before being sent over the wire or saved to disk.
pub type LV = usize;

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(untagged))]
// #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Primitive {
    Nil,
    Bool(bool),
    I64(i64),
    // F64(f64),
    Str(SmartString),

    #[cfg_attr(feature = "serde", serde(skip))]
    InvalidUninitialized,
}

// #[derive(Debug, Eq, PartialEq, Copy, Clone, TryFromPrimitive)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
// #[repr(u16)]
pub enum CRDTKind {
    Map, // String => Register (like a JS object)
    Register,
    Collection, // SQL table / mongo collection
    Text,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CreateValue {
    Primitive(Primitive),
    NewCRDT(CRDTKind),
    // Deleted, // Marks that the key / contents should be deleted.
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CollectionOp {
    Insert(CreateValue),
    Remove(LV),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum OpContents {
    RegisterSet(CreateValue),
    MapSet(SmartString, CreateValue), // TODO: Inline the index here.
    MapDelete(SmartString), // TODO: And here.
    Collection(CollectionOp), // TODO: Consider just inlining this.
    Text(ListOpMetrics),


    // The other way to write this would be:


    // SetInsert(CRDTKind),
    // SetRemove(Time),

    // TextInsert(..),
    // TextRemove(..)
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct Op {
    pub target_id: LV,
    pub contents: OpContents,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct Ops {
    /// Local version + op pairs
    ops: RleVec<KVPair<Op>>,
    list_ctx: ListOperationCtx,
}

pub const ROOT_CRDT_ID: LV = usize::MAX;
pub const ROOT_CRDT_ID_AV: AgentVersion = (AgentId::MAX, 0);

#[derive(Debug)]
pub struct OpLog {
    // /// The ID of the document (if any). This is useful if you want to give a document a GUID or
    // /// something to make sure you're merging into the right place.
    // ///
    // /// Optional - only used if you set it.
    // doc_id: Option<SmartString>,

    /// The causal graph stores the mapping from (local) time values <-> (agent, seq) pairs.
    /// This is loaded from disk on startup in its entirety and appended to with each change when
    /// the WAL is flushed.
    pub(crate) cg: CausalGraph,

    cg_storage: Option<CGStorage>,
    wal_storage: Option<WriteAheadLog>,

    /// The version that contains everything from CG.
    /// This might make more sense in CG?
    version: Frontier,

    /// Values which have not yet been flushed to the WAL.
    uncommitted_ops: Ops,
}


#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SnapshotValue {
    Primitive(Primitive),
    InnerCRDT(LV),
    // Ref(LV),
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct RegisterState {
    value: SnapshotValue,
    version: LV,
}

/// Guaranteed to always have at least 1 value inside.
type MVRegister = SmallVec<[RegisterState; 1]>;

// TODO: Probably should also store a dirty flag for when we flush to disk.
#[derive(Debug, Clone, Eq, PartialEq)]
enum OverlayValue {
    Register(MVRegister),
    Map(BTreeMap<SmartString, MVRegister>),
    Collection(BTreeMap<LV, SnapshotValue>),
    Text(Box<JumpRope>),
}

/// The branch object stores the *data* at some particular version of the database. This is
/// implemented via an overlay of data fields on top of a snapshot database. The overlay data is
/// periodically flushed to disk.
#[derive(Debug, Clone)]
pub struct Branch {
    /// The overlay contents. This stores values which have either diverged from the persisted data
    /// or are cached.
    ///
    /// Later this will only contain the "overlay" data - ie, data which has diverged from whatever
    /// we're storing on disk.
    data: BTreeMap<LV, OverlayValue>,

    /// The version the branch is currently at. This is used to track which changes the branch has
    /// or has not locally merged.
    ///
    /// This field is public for convenience, but you should never modify it directly.
    version: Frontier,

    // persisted_data: BTreeMap<Time, OverlayValue>, // TODO. Not actually an in-memory object.
    // persisted_version: LocalVersion,

    num_invalid: usize,
}
