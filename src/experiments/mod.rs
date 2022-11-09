use std::collections::BTreeMap;
use crate::{CausalGraph, Frontier, LV, SnapshotValue};
use smartstring::alias::String as SmartString;
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::rle::{KVPair, RleVec};

// type Pair<T> = (LV, T);
type ValPair = (LV, SnapshotValue);
// type RawPair<'a, T> = (RemoteVersion<'a>, T);
type DocName = LV;


// struct RegisterInfo {
//
// }

#[derive(Debug, Clone)]
struct TextInfo {
    ctx: ListOperationCtx,
    ops: RleVec<KVPair<ListOpMetrics>>
}

#[derive(Debug, Clone, Default)]
struct ExperimentalOpLog {
    cg: CausalGraph,
    version: Frontier,

    // TODO: Vec -> SmallVec.
    registers: BTreeMap<DocName, Vec<ValPair>>,
    maps: BTreeMap<(DocName, SmartString), Vec<ValPair>>,
    texts: BTreeMap<DocName, TextInfo>,

    index: BTreeMap<LV, DocName>,
}


impl ExperimentalOpLog {
    pub fn new() -> Self {
        Default::default()
    }
}