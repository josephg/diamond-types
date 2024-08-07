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

use crate::listmerge::markers::Marker;
use crate::listmerge::yjsspan::CRDTSpan;
use crate::ost::content_tree::ContentTree;
use crate::ost::IndexTree;

pub(crate) mod yjsspan;
pub(crate) mod merge;
pub(crate) mod markers;
mod advance_retreat;
// pub(crate) mod txn_trace;
#[cfg(test)]
pub mod fuzzer;
#[cfg(feature = "dot_export")]
mod dot;

#[cfg(any(test, feature = "gen_test_data"))]
pub(crate) mod simple_oplog;
pub(crate) mod plan;

pub(crate) mod xf_old;

type Index = IndexTree<Marker>;

#[derive(Debug)]
struct M2Tracker {
    /// The index is used for 2 things:
    ///
    /// - For inserts, this contains a pointer to the node in range_tree which contains this version
    /// - For deletes, this names the time at which the delete happened.
    index: Index,
    
    range_tree: ContentTree<CRDTSpan>,

    #[cfg(feature = "merge_conflict_checks")]
    concurrent_inserts_collide: bool,
}
