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
use content_tree::{ContentMetrics, ContentTreeWithIndex, FullMetricsUsize, RawPositionMetrics};
use crate::list::ListCRDT;
use crate::list::m2::markers::MarkerEntry;
use crate::list::m2::merge::notify_for;
use crate::list::m2::metrics::MarkerMetrics;
use crate::list::m2::yjsspan2::YjsSpan2;
use crate::localtime::TimeSpan;
use crate::rle::{KVPair, RleVec};

mod yjsspan2;
mod merge;
mod markers;
mod advance_retreat;
mod txn_trace;
mod metrics;

type DocRangeIndex = MarkerMetrics;
type CRDTList2 = Pin<Box<ContentTreeWithIndex<YjsSpan2, DocRangeIndex>>>;

type SpaceIndex = Pin<Box<ContentTreeWithIndex<MarkerEntry<YjsSpan2, DocRangeIndex>, RawPositionMetrics>>>;

#[derive(Debug)]
struct M2Tracker {
    range_tree: CRDTList2,

    index: SpaceIndex,

    /// This is a set of all deletes. Each delete names the set of times of inserts which were
    /// deleted. Keyed by the delete order, NOT the order of the item *being* deleted.
    deletes: RleVec<KVPair<TimeSpan>>,
}
