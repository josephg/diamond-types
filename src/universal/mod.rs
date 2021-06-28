mod span;
mod doc;
mod simple_rle;

use crate::common::{CRDTLocation, ClientName};
use smallvec::SmallVec;
use std::pin::Pin;
use crate::range_tree::{RangeTree, ContentIndex, Entry};
use crate::universal::span::YjsSpan;
use crate::split_list::SplitList;
use crate::automerge::MarkerEntry;
use smartstring::alias::{String as SmartString};
use crate::order::OrderMarker;
use crate::universal::simple_rle::Rle;
use ropey::Rope;

pub type Order = u32;
pub const ROOT_ORDER: Order = Order::MAX;

#[derive(Clone, Debug)]
struct ClientData {
    /// Used to map from client's name / hash to its numerical ID.
    name: ClientName,

    /// This is a run-length-encoded in-order list of all items inserted by this client.
    ///
    /// This contains a set of (CRDT location range -> item orders).
    ///
    /// The OrderMarkers here always have positive len.
    item_orders: Rle<OrderMarker>,
}

// pub type MarkerTree = Pin<Box<RangeTree<MarkerEntry<YjsSpan, ContentIndex>, RawPositionIndex>>>;
pub type MarkerTree = SplitList<MarkerEntry<YjsSpan, ContentIndex>>;

#[derive(Debug)]
pub struct YjsDoc {
    /// This is a bunch of ranges of (item order -> CRDT location span).
    /// The entries always have positive len.
    client_with_order: Rle<Entry>,

    /// The set of txn orders with no children in the document. With a single writer this will
    /// always just be the last order we've seen.
    ///
    /// Never empty. Starts at usize::max (which is the root order).
    frontier: SmallVec<[Order; 4]>,

    /// For each client, we store some data (above). This is indexed by AgentId.
    client_data: Vec<ClientData>,

    /// The marker tree maps from order positions to btree entries, so we can map between orders and
    /// document locations.
    range_tree: Pin<Box<RangeTree<YjsSpan, ContentIndex>>>,

    /// We need to be able to map each location to an item in the associated BST.
    /// Note for inserts which insert a lot of contiguous characters, this will
    /// contain a lot of repeated pointers. I'm trading off memory for simplicity
    /// here - which might or might not be the right approach.
    // markers: SplitList<MarkerEntry<YjsSpan, ContentIndex>>,
    markers: MarkerTree,

    // Probably temporary, eventually.
    text_content: Rope,
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


