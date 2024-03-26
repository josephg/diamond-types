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
use content_tree::{ContentTreeRaw, MutCursor, RawPositionMetricsUsize, UnsafeCursor};
use crate::listmerge::markers::{Marker, Marker2, MarkerEntry};
use crate::listmerge::metrics::MarkerMetrics;
use crate::listmerge::yjsspan::CRDTSpan;
use crate::LV;
use crate::ost::IndexTree;

pub(crate) mod yjsspan;
pub(crate) mod merge;
pub(crate) mod markers;
mod advance_retreat;
// pub(crate) mod txn_trace;
mod metrics;
#[cfg(test)]
pub mod fuzzer;
#[cfg(feature = "dot_export")]
mod dot;

#[cfg(feature = "ops_to_old")]
pub mod to_old;
#[cfg(any(test, feature = "gen_test_data"))]
pub(crate) mod simple_oplog;
pub(crate) mod plan;

type DocRangeIndex = MarkerMetrics;
type CRDTList2 = Pin<Box<ContentTreeRaw<CRDTSpan, DocRangeIndex>>>;

type SpaceIndex = Pin<Box<ContentTreeRaw<MarkerEntry, RawPositionMetricsUsize>>>;
type SpaceIndexCursor = UnsafeCursor<MarkerEntry, RawPositionMetricsUsize>;


#[derive(Debug)]
struct Index {
    index_old: SpaceIndex,
    index_cursor: Option<(LV, SpaceIndexCursor)>,
    index2: IndexTree<Marker2>,
}

#[derive(Debug)]
struct M2Tracker {
    range_tree: CRDTList2,

    /// The index is used for 2 things:
    ///
    /// - For inserts, this contains a pointer to the node in range_tree which contains this version
    /// - For deletes, this names the time at which the delete happened.
    index: Index,

    #[cfg(feature = "merge_conflict_checks")]
    concurrent_inserts_collide: bool,

    #[cfg(feature = "ops_to_old")]
    dbg_ops: Vec<to_old::OldCRDTOpInternal>,
}
