pub mod textinfo;
pub mod oplog;
mod branch;

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use bumpalo::Bump;
use jumprope::JumpRopeBuf;
use smallvec::{SmallVec, smallvec};
use crate::{AgentId, CausalGraph, CRDTKind, CreateValue, DTRange, Frontier, LV, Primitive, ROOT_CRDT_ID, SnapshotValue};
use smartstring::alias::String as SmartString;
use rle::{HasLength, SplitableSpan, SplitableSpanCtx};
use crate::branch::DTValue;
use crate::causalgraph::agent_assignment::remote_ids::{RemoteVersion, RemoteVersionOwned};
use crate::encoding::bufparser::BufParser;
use crate::encoding::cg_entry::{read_cg_entry_into_cg, write_cg_entry_iter};
use crate::encoding::map::{ReadMap, WriteMap};
use crate::list::op_iter::{OpIterFast, OpMetricsIter};
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::operation::TextOperation;
use crate::rle::{KVPair, RleSpanHelpers, RleVec};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize, Serializer};
use crate::causalgraph::graph::Graph;
use crate::causalgraph::graph::tools::DiffFlag;
use crate::encoding::parseerror::ParseError;
use crate::experiments::oplog::create_to_snapshot;
use crate::experiments::textinfo::TextInfo;
use crate::frontier::{debug_assert_frontier_sorted, diff_frontier_entries, is_sorted_iter, is_sorted_iter_uniq, is_sorted_slice};

// type Pair<T> = (LV, T);
type ValPair = (LV, CreateValue);
// type RawPair<'a, T> = (RemoteVersion<'a>, T);
type LVKey = LV;


#[derive(Debug, Clone, Default)]
pub(crate) struct RegisterInfo {
    // I bet there's a clever way to use RLE to optimize this. Right now this contains the full
    // history of values this register has ever held.
    ops: Vec<ValPair>,

    // Indexes into ops.
    supremum: SmallVec<[usize; 2]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterValue {
    Primitive(Primitive),
    OwnedCRDT(CRDTKind, LVKey),
}


#[derive(Debug, Clone, Default)]
pub struct ExperimentalOpLog {
    pub cg: CausalGraph,

    // Information about whether the map still exists!
    // maps: BTreeMap<LVKey, MapInfo>,

    map_keys: BTreeMap<(LVKey, SmartString), RegisterInfo>,
    texts: BTreeMap<LVKey, TextInfo>,

    // A different index for each data set, or one index with an enum?
    map_index: BTreeMap<LV, (LVKey, SmartString)>,
    text_index: BTreeMap<LV, LVKey>,

    // TODO: Vec -> SmallVec.
    // registers: BTreeMap<LVKey, RegisterInfo>,

    // The set of CRDTs which have been deleted or superceded in the current version. This data is
    // pretty similar to the _index data, in that its mainly just useful for branches doing
    // checkouts.
    deleted_crdts: BTreeSet<LVKey>,
}

/// The register stores the specified value, but if conflicts_with is not empty, it has some
/// conflicting concurrent values too. The `value` field will be consistent across all peers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RegisterState {
    value: RegisterValue,
    conflicts_with: Vec<RegisterValue>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExperimentalBranch {
    pub frontier: Frontier,

    // Objects are always created at the highest version ID, but can be deleted anywhere in the
    // range.
    //
    // TODO: Replace BTreeMap with something more appropriate later.
    // registers: BTreeMap<LVKey, SmallVec<[LV; 2]>>, // TODO.
    maps: BTreeMap<LVKey, BTreeMap<SmartString, RegisterState>>, // any objects.
    pub texts: BTreeMap<LVKey, JumpRopeBuf>,
}

fn subgraph_rev_iter(ops: &RleVec<KVPair<ListOpMetrics>>) -> impl Iterator<Item=DTRange> + '_ {
    ops.0.iter().rev().map(|e| e.range())
}


#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SerializedOps<'a> {
    cg_changes: Vec<u8>,

    // The version of the op, and the name of the containing CRDT.
    #[cfg_attr(feature = "serde", serde(borrow))]
    map_ops: Vec<(RemoteVersion<'a>, RemoteVersion<'a>, &'a str, CreateValue)>,
    text_ops: Vec<(RemoteVersion<'a>, RemoteVersion<'a>, ListOpMetrics)>,
    text_context: ListOperationCtx,
}
