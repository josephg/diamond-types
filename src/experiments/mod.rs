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
    frontier: Frontier,

    // Objects are always created at the highest version ID, but can be deleted anywhere in the
    // range.
    //
    // TODO: Replace BTreeMap with something more appropriate later.
    // registers: BTreeMap<LVKey, SmallVec<[LV; 2]>>, // TODO.
    maps: BTreeMap<LVKey, BTreeMap<SmartString, RegisterState>>, // any objects.
    texts: BTreeMap<LVKey, JumpRopeBuf>,
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


#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};
    use crate::experiments::{ExperimentalOpLog, SerializedOps};
    use crate::{CRDTKind, CreateValue, Primitive, ROOT_CRDT_ID};
    use crate::causalgraph::agent_assignment::remote_ids::RemoteVersion;
    use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
    use crate::list::operation::TextOperation;

    #[test]
    fn smoke() {
        let mut oplog = ExperimentalOpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        oplog.local_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(123)));
        oplog.local_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(321)));

        dbg!(&oplog);
        oplog.dbg_check(true);
    }

    #[test]
    fn text() {
        let mut oplog = ExperimentalOpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        let text = oplog.local_map_set(seph, ROOT_CRDT_ID, "content", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, text, TextOperation::new_insert(0, "Oh hai!"));
        oplog.local_text_op(seph, text, TextOperation::new_delete(0..3));

        let title = oplog.local_map_set(seph, ROOT_CRDT_ID, "title", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, title, TextOperation::new_insert(0, "Please read this cool info"));

        // dbg!(&oplog);

        assert_eq!(oplog.checkout_text(text).to_string(), "hai!");
        oplog.dbg_check(true);

        // dbg!(oplog.checkout());

        // dbg!(oplog.changes_since(&[]));
        // dbg!(oplog.changes_since(&[title]));


        let c = oplog.ops_since(&[]);
        let mut oplog_2 = ExperimentalOpLog::new();
        oplog_2.merge_ops(c).unwrap();
        assert_eq!(oplog_2.cg, oplog.cg);
        // dbg!(oplog_2)
        // dbg!(oplog_2.checkout());
        oplog_2.dbg_check(true);

        assert_eq!(oplog.checkout(), oplog_2.checkout());
    }

    #[test]
    fn concurrent_changes() {
        let mut oplog1 = ExperimentalOpLog::new();
        let mut oplog2 = ExperimentalOpLog::new();


        let seph = oplog1.cg.get_or_create_agent_id("seph");
        let text = oplog1.local_map_set(seph, ROOT_CRDT_ID, "content", CreateValue::NewCRDT(CRDTKind::Text));
        oplog1.local_text_op(seph, text, TextOperation::new_insert(0, "Oh hai!"));


        let kaarina = oplog2.cg.get_or_create_agent_id("kaarina");
        let title = oplog2.local_map_set(kaarina, ROOT_CRDT_ID, "title", CreateValue::NewCRDT(CRDTKind::Text));
        oplog2.local_text_op(kaarina, title, TextOperation::new_insert(0, "Better keep it clean"));


        // let c = oplog1.changes_since(&[]);
        // dbg!(serde_json::to_string(&c).unwrap());
        // let c = oplog2.changes_since(&[]);
        // dbg!(serde_json::to_string(&c).unwrap());

        oplog2.merge_ops(oplog1.ops_since(&[])).unwrap();
        oplog2.dbg_check(true);

        oplog1.merge_ops(oplog2.ops_since(&[])).unwrap();
        oplog1.dbg_check(true);

        // dbg!(oplog1.checkout());
        // dbg!(oplog2.checkout());
        assert_eq!(oplog1.checkout(), oplog2.checkout());

        dbg!(oplog1.crdt_at_path(&["title"]));
    }

    #[test]
    fn checkout() {
        let mut oplog = ExperimentalOpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        oplog.local_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(123)));
        let map = oplog.local_map_set(seph, ROOT_CRDT_ID, "yo", CreateValue::NewCRDT(CRDTKind::Map));
        oplog.local_map_set(seph, map, "yo", CreateValue::Primitive(Primitive::Str("blah".into())));

        dbg!(oplog.checkout());
        oplog.dbg_check(true);
    }






    #[cfg(feature = "serde")]
    #[test]
    fn serde_stuff() {
        // let line = r##"{"type":"DocsDelta","deltas":[[["RUWYEZu",0],{"cg_changes":[1,6,83,67,72,69,77,65,10,1],"map_ops":[[["ROOT",0],["SCHEMA",9],"content",{"NewCRDT":"Text"}],[["ROOT",0],["SCHEMA",0],"title",{"NewCRDT":"Text"}]],"text_ops":[[["SCHEMA",0],["SCHEMA",1],{"loc":{"start":0,"end":8,"fwd":true},"kind":"Ins","content_pos":[0,8]}]],"text_context":{"ins_content":[85,110,116,105,116,108,101,100],"del_content":[]}}]]}"##;
        // let line = r##"{"cg_changes":[1,6,83,67,72,69,77,65,10,1],"map_ops":[[["ROOT",0],["SCHEMA",9],"content",{"NewCRDT":"Text"}],[["ROOT",0],["SCHEMA",0],"title",{"NewCRDT":"Text"}]],"text_ops":[[["SCHEMA",0],["SCHEMA",1],{"loc":{"start":0,"end":8,"fwd":true},"kind":"Ins","content_pos":[0,8]}]],"text_context":{"ins_content":[85,110,116,105,116,108,101,100],"del_content":[]}}"##;
        //
        // let msg: SerializedOps = serde_json::from_str(&line).unwrap();

        #[derive(Debug, Clone)]
        #[derive(Serialize, Deserialize)]
        pub struct SS {
            // cg_changes: Vec<u8>,

            // The version of the op, and the name of the containing CRDT.
            // map_ops: Vec<(RemoteVersion<'a>, RemoteVersion<'a>, &'a str, CreateValue)>,
            // text_ops: Vec<ListOpMetrics>,
            // text_context: ListOperationCtx,
        }

        // let line = r#"{"cg_changes":[1,6,83,67,72,69,77,65,10,1],"map_ops":[[["ROOT",0],["SCHEMA",9],"content",{"NewCRDT":"Text"}],[["ROOT",0],["SCHEMA",0],"title",{"NewCRDT":"Text"}]],"text_ops":[[["SCHEMA",0],["SCHEMA",1],{"loc":{"start":0,"end":8,"fwd":true},"kind":"Ins","content_pos":[0,8]}]],"text_context":{"ins_content":[85,110,116,105,116,108,101,100],"del_content":[]}}"#;
        // let x: SS = serde_json::from_str(&line).unwrap();
        // let line = r#"{"text_ops":[{"loc":{"start":0,"end":8,"fwd":true},"kind":"Ins","content_pos":[0,8]}]}"#;
        // let x: SS = serde_json::from_str(&line).unwrap();
        let line = r#"{"loc":{"start":0,"end":8,"fwd":true},"kind":"Ins","content_pos":[0,8]}"#;
        let _x: ListOpMetrics = serde_json::from_str(&line).unwrap();

    }
}