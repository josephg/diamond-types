use std::pin::Pin;
use jumprope::JumpRope;

use smallvec::SmallVec;
use smartstring::alias::String as SmartString;

use content_tree::*;
use diamond_core::AgentId;
pub use ot::traversal::TraversalComponent;
pub use positional::{PositionalComponent, PositionalOp, InsDelTag};

use crate::common::ClientName;
use crate::crdtspan::CRDTSpan;
use crate::list::double_delete::DoubleDelete;
use crate::list::markers::MarkerEntry;
use crate::list::span::YjsSpan;
use crate::list::txn::TxnSpan;
use crate::order::TimeSpan;
// use crate::list::delete::DeleteEntry;
use crate::rle::{KVPair, RleVec};

mod span;
mod doc;
mod markers;
mod txn;
mod double_delete;
pub mod external_txn;
mod eq;
mod encoding;
mod check;
mod ot;
mod branch;
pub mod time;
pub mod positional;
mod merge_positional;

// #[cfg(inlinerope)]
// pub const USE_INNER_ROPE: bool = true;
// #[cfg(not(inlinerope))]
// pub const USE_INNER_ROPE: bool = false;

// #[cfg(test)]
// mod tests;

pub type Time = u32;
pub const ROOT_TIME: Time = Time::MAX;
pub const ROOT_AGENT: AgentId = AgentId::MAX;

#[derive(Clone, Debug)]
struct ClientData {
    /// Used to map from client's name / hash to its numerical ID.
    name: ClientName,

    /// This is a run-length-encoded in-order list of all items inserted by this client.
    ///
    /// Each entry in this list internally has (seq base, {order base, len}). This maps CRDT
    /// location range -> item orders
    ///
    /// The OrderMarkers here always have positive len.
    item_localtime: RleVec<KVPair<TimeSpan>>,
}

pub(crate) const INDEX_IE: usize = DEFAULT_IE;
pub(crate) const INDEX_LE: usize = DEFAULT_LE;

pub(crate) const DOC_IE: usize = DEFAULT_IE;
pub(crate) const DOC_LE: usize = DEFAULT_LE;
// const DOC_LE: usize = 32;

// type DocRangeIndex = ContentIndex;
type DocRangeIndex = FullMetrics;

pub(crate) type RangeTree = Pin<Box<ContentTreeWithIndex<YjsSpan, DocRangeIndex>>>;
pub(crate) type RangeTreeLeaf = NodeLeaf<YjsSpan, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>;

type SpaceIndex = Pin<Box<ContentTreeWithIndex<MarkerEntry<YjsSpan, DocRangeIndex>, RawPositionMetrics>>>;

pub type DoubleDeleteList = RleVec<KVPair<DoubleDelete>>;

pub type Branch = SmallVec<[Time; 4]>;

#[derive(Debug)]
pub struct ListCRDT {
    /// The set of txn orders with no children in the document. With a single writer this will
    /// always just be the last order we've seen.
    ///
    /// Never empty. Starts at usize::max (which is the root order).
    frontier: Branch,

    /// This is a bunch of ranges of (item time -> CRDT location span).
    /// The entries always have positive len.
    ///
    /// This is used to map time -> External CRDT locations.
    client_with_time: RleVec<KVPair<CRDTSpan>>,

    /// For each client, we store some data (above). This is indexed by AgentId.
    ///
    /// This is used to map external CRDT locations -> Time numbers.
    client_data: Vec<ClientData>,

    /// The range tree maps from document positions to btree entries.
    ///
    /// This is the CRDT chum for the space DAG.
    range_tree: RangeTree,

    /// We need to be able to map each location to an item in the associated BST.
    /// Note for inserts which insert a lot of contiguous characters, this will
    /// contain a lot of repeated pointers. I'm trading off memory for simplicity
    /// here - which might or might not be the right approach.
    ///
    /// This is a map from insert time -> a pointer to the leaf node which contains that insert.
    index: SpaceIndex,

    /// This is a set of all deletes. Each delete names the set of times of inserts which were
    /// deleted. Keyed by the delete order, NOT the order of the item *being* deleted.
    deletes: RleVec<KVPair<TimeSpan>>,

    /// List of document items which have been deleted more than once. Usually empty. Keyed by the
    /// item *being* deleted (like content_tree, unlike deletes).
    double_deletes: DoubleDeleteList,

    /// Transaction metadata (succeeds, parents) for all operations on this document. This is used
    /// for `diff` and `branchContainsVersion` calls on the document, which is necessary to merge
    /// remote changes.
    ///
    /// Along with deletes, this essentially contains the time DAG.
    ///
    /// TODO: Consider renaming this field
    txns: RleVec<TxnSpan>,

    // Temporary. This will be moved out into a reference to another data structure I think.
    text_content: Option<JumpRope>,
    /// This is a big ol' string containing everything that's been deleted (self.deletes) in order.
    deleted_content: Option<String>,
}
