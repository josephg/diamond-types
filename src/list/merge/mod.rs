//! This module is the second implementation for handling positional updates. Instead of generating
//! a series of patches and merging them, this code applies patches by making a positional map and
//! moving backwards in time.
//!
//! There's a bunch of ways this code could be written:
//!
//! 1. We could store the content tree + position map in the same structure or separately (as in
//! PositionMap in dt-crdt)
//! 2. When moving around, we could either scan the list and rewrite it (activating and deactivating
//! entries as we go). Or we could figure it out by walking the txns forwards and backwards through
//! time.

use std::pin::Pin;
use content_tree::{ContentTreeRaw, RawPositionMetricsUsize};
use rle::{HasLength, MergableSpan};
use crate::list::merge::markers::MarkerEntry;
use crate::list::merge::metrics::MarkerMetrics;
use crate::list::merge::yjsspan::YjsSpan;
use crate::{CRDTSpan, DTRange, SmartString, Time};
use crate::list::ListOpLog;
use crate::list::remote_ids::RemoteIdSpan;
use crate::rev_range::RangeRev;

mod yjsspan;
pub(crate) mod merge;
mod markers;
mod advance_retreat;
pub(crate) mod txn_trace;
mod metrics;
#[cfg(test)]
pub mod fuzzer;
#[cfg(feature = "dot_export")]
mod dot;

#[cfg(feature = "ops_to_old")]
pub mod to_old;

type DocRangeIndex = MarkerMetrics;
type CRDTList2 = Pin<Box<ContentTreeRaw<YjsSpan, DocRangeIndex>>>;

type SpaceIndex = Pin<Box<ContentTreeRaw<MarkerEntry, RawPositionMetricsUsize>>>;

#[derive(Debug)]
struct M2Tracker {
    range_tree: CRDTList2,

    /// The index is used for 2 things:
    ///
    /// - For inserts, this contains a pointer to the node in range_tree which contains this time.
    /// - For deletes, this names the time at which the delete happened.
    index: SpaceIndex,

    // This is a set of all deletes. Each delete names the set of times of inserts which were
    // deleted. Keyed by the delete order, NOT the order of the item *being* deleted.
    //
    // The problem here is that when we merge into a branch, we might merge items with an earlier
    // time. So as a result, this collection is not append-only.
    //
    // TODO: Trial using BTreeMap here.
    // deletes: Pin<Box<ContentTreeWithIndex<KVPair<Delete>, RawPositionMetricsUsize>>>,
    // deletes: BTreeMap<usize, Delete>,

    #[cfg(feature = "ops_to_old")]
    dbg_ops: Vec<to_old::OldCRDTOp>,
}
