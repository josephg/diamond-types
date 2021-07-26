use std::pin::Pin;

use ropey::Rope;
use smallvec::SmallVec;
use smartstring::alias::String as SmartString;

use crate::common::{ClientName, CRDTLocation, AgentId};
use crate::order::OrderSpan;
use crate::range_tree::{ContentIndex, CRDTSpan, RangeTree};
use crate::list::span::YjsSpan;
use crate::list::markers::MarkerEntry;
// use crate::list::delete::DeleteEntry;
use crate::rle::{Rle, KVPair};
use crate::split_list::SplitList;
use crate::list::txn::TxnSpan;
use crate::list::double_delete::DoubleDelete;

mod span;
mod doc;
mod markers;
mod txn;
mod double_delete;
mod external_txn;
mod eq;
mod encoding;


// #[cfg(inlinerope)]
// pub const USE_INNER_ROPE: bool = true;
// #[cfg(not(inlinerope))]
// pub const USE_INNER_ROPE: bool = false;

// #[cfg(test)]
// mod tests;

pub type Order = u32;
pub const ROOT_ORDER: Order = Order::MAX;
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
    item_orders: Rle<KVPair<OrderSpan>>,
}

// pub type MarkerTree = Pin<Box<RangeTree<MarkerEntry<YjsSpan, ContentIndex>, RawPositionIndex>>>;
pub type SpaceIndex = SplitList<MarkerEntry<YjsSpan, ContentIndex>>;
// pub type MarkerTree = MutRle<MarkerEntry<YjsSpan, ContentIndex>>;

pub type Branch = SmallVec<[Order; 4]>;

#[derive(Debug)]
pub struct ListCRDT {
    /// The set of txn orders with no children in the document. With a single writer this will
    /// always just be the last order we've seen.
    ///
    /// Never empty. Starts at usize::max (which is the root order).
    frontier: Branch,

    /// This is a bunch of ranges of (item order -> CRDT location span).
    /// The entries always have positive len.
    ///
    /// This is used to map Order -> External CRDT locations.
    client_with_order: Rle<KVPair<CRDTSpan>>,

    /// For each client, we store some data (above). This is indexed by AgentId.
    ///
    /// This is used to map external CRDT locations -> Order numbers.
    client_data: Vec<ClientData>,

    /// The marker tree maps from order positions to btree entries, so we can map between orders and
    /// document locations.
    ///
    /// This is the CRDT chum for the space DAG.
    range_tree: Pin<Box<RangeTree<YjsSpan, ContentIndex>>>,

    /// We need to be able to map each location to an item in the associated BST.
    /// Note for inserts which insert a lot of contiguous characters, this will
    /// contain a lot of repeated pointers. I'm trading off memory for simplicity
    /// here - which might or might not be the right approach.
    index: SpaceIndex,

    /// This is a set of all deletes. Each delete names the set of orders of inserts which were
    /// deleted. Keyed by the delete order, NOT the order of the item *being* deleted.
    deletes: Rle<KVPair<OrderSpan>>,

    /// List of document items which have been deleted more than once. Usually empty. Keyed by the
    /// item *being* deleted (like range_tree, unlike deletes).
    double_deletes: Rle<KVPair<DoubleDelete>>,

    /// Transaction metadata (succeeds, parents) for all operations on this document. This is used
    /// for `diff` and `branchContainsVersion` calls on the document, which is necessary to merge
    /// remote changes.
    ///
    /// Along with deletes, this essentially contains the time DAG.
    txns: Rle<TxnSpan>,

    // Temporary.
    pub text_content: Rope,
}

// #[derive(Clone, Debug)]
// pub enum OpExternal {
//     Insert {
//         // The items in the run implicitly all have the same origin_right, and except for the first,
//         // each one has the previous item's ID as its origin_left.
//         content: InlinableString,
//         origin_left: CRDTLocation,
//         origin_right: CRDTLocation,
//     },
//     // Deleted characters in sequence. In a CRDT these characters must be
//     // contiguous from a single client.
//     Delete {
//         target: CRDTLocation,
//         span: usize,
//     }
// }
//
// #[derive(Clone, Debug)]
// pub struct TxnExternal {
//     id: CRDTLocation,
//     insert_seq_start: u32,
//     parents: SmallVec<[CRDTLocation; 2]>,
//     ops: SmallVec<[OpExternal; 1]>,
// }
//
//
// pub type Order = usize; // Feeling cute, might change later to u48 for less ram use.
//
// #[derive(Clone, Debug)]
// pub enum Op {
//     Insert {
//         content: InlinableString,
//         origin_left: Order,
//         origin_right: Order,
//     },
//     Delete {
//         target: Order,
//         span: usize,
//     }
// }
//
// #[derive(Clone, Debug)]
// pub struct TxnInternal {
//     id: CRDTLocation,
//     order: Order, // TODO: Remove this.
//     parents: SmallVec<[Order; 2]>,
//
//     insert_seq_start: u32, // From external op.
//     insert_order_start: Order,
//     num_inserts: usize, // Cached from ops.
//
//     dominates: Order,
//     submits: Order,
//
//     ops: SmallVec<[Op; 1]>,
// }


