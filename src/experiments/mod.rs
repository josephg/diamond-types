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
use crate::frontier::{debug_assert_frontier_sorted, diff_frontier_entries, is_sorted_iter, is_sorted_iter_uniq, is_sorted_slice};

// type Pair<T> = (LV, T);
type ValPair = (LV, CreateValue);
// type RawPair<'a, T> = (RemoteVersion<'a>, T);
type LVKey = LV;


#[derive(Debug, Clone, Default)]
struct RegisterInfo {
    // I bet there's a clever way to use RLE to optimize this.
    ops: Vec<ValPair>,

    // Indexes into ops.
    supremum: SmallVec<[usize; 2]>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TextInfo {
    pub ctx: ListOperationCtx,
    pub ops: RleVec<KVPair<ListOpMetrics>>,
    frontier: Frontier,
}

fn subgraph_rev_iter(ops: &RleVec<KVPair<ListOpMetrics>>) -> impl Iterator<Item=DTRange> + '_ {
    ops.0.iter().rev().map(|e| e.range())
}

impl TextInfo {
    pub(crate) fn iter_metrics_range(&self, range: DTRange) -> OpMetricsIter {
        OpMetricsIter::new(&self.ops, &self.ctx, range)
    }
    pub(crate) fn iter_metrics(&self) -> OpMetricsIter {
        OpMetricsIter::new(&self.ops, &self.ctx, (0..self.ops.end()).into())
    }

    pub(crate) fn iter_fast(&self) -> OpIterFast {
        self.iter_metrics().into()
    }

    pub fn iter(&self) -> impl Iterator<Item = TextOperation> + '_ {
        self.iter_fast().map(|pair| (pair.0.1, pair.1).into())
    }

    fn push_op_internal(&mut self, op: TextOperation, v_range: DTRange) {
        debug_assert_eq!(v_range.len(), op.len());

        let content_pos = op.content.as_ref().map(|content| {
            self.ctx.push_str(op.kind, content)
        });

        self.ops.push(KVPair(v_range.start, ListOpMetrics {
            loc: op.loc,
            kind: op.kind,
            content_pos
        }));
    }

    pub(crate) fn remote_push_op(&mut self, op: TextOperation, v_range: DTRange, parents: &[LV], graph: &Graph) {
        self.push_op_internal(op, v_range);
        // // TODO: Its probably simpler to just call advance_sparse() here.
        // let local_parents = graph.project_onto_subgraph_raw(
        //     subgraph_rev_iter(&self.ops),
        //     parents
        // );
        // self.frontier.advance_by_known_run(local_parents.as_ref(), v_range);
        self.frontier.advance_sparse_known_run(graph, parents, v_range);
    }

    pub(crate) fn remote_push_op_unknown_parents(&mut self, op: TextOperation, v_range: DTRange, graph: &Graph) {
        self.push_op_internal(op, v_range);
        self.frontier.advance_sparse(graph, v_range);
    }

    pub(crate) fn local_push_op(&mut self, op: TextOperation, v_range: DTRange) {
        self.push_op_internal(op, v_range);
        self.frontier.replace_with_1(v_range.last());
    }
}


#[derive(Debug, Clone, Default)]
pub struct ExperimentalOpLog {
    pub cg: CausalGraph,

    // TODO: Vec -> SmallVec.
    registers: BTreeMap<LVKey, RegisterInfo>,

    // Information about whether the map still exists!
    // maps: BTreeMap<LVKey, MapInfo>,

    map_keys: BTreeMap<(LVKey, SmartString), RegisterInfo>,
    texts: BTreeMap<LVKey, TextInfo>,

    // A different index for each data set, or one index with an enum?
    map_index: BTreeMap<LV, (LVKey, SmartString)>,
    text_index: BTreeMap<LV, LVKey>,
}

// #[derive(Debug, Clone, Default)]
// struct ExperimentalBranch {
//     v: Frontier,
//
//     registers: BTreeMap<LVKey, SmallVec<[LV; 2]>>,
//     maps: BTreeMap<(LVKey, SmartString), SmallVec<[LV; 2]>>,
//     texts: BTreeMap<LVKey, JumpRopeBuf>,
// }

#[derive(Debug, Clone, PartialEq, Eq)]
enum RegisterValue {
    Primitive(Primitive),
    OwnedCRDT(CRDTKind, LVKey),
}

#[cfg(feature = "serde")]
impl Serialize for ExperimentalOpLog {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        self.ops_since(&[]).serialize(serializer)
    }
}

impl ExperimentalOpLog {
    pub(crate) fn dbg_check(&self, deep: bool) {
        self.cg.dbg_check(deep);
        let cg_len = self.cg.len();

        let mut item_type = BTreeMap::new();
        item_type.insert(ROOT_CRDT_ID, CRDTKind::Map);

        // Map operations
        let mut expected_idx_count = 0;
        for ((crdt, key), info) in self.map_keys.iter() {
            // Check the supremum is sorted
            assert!(is_sorted_slice::<true, _>(&info.supremum));

            // Record the type of all the items
            for op in &info.ops {
                match op.1 {
                    CreateValue::Primitive(_) => {}
                    CreateValue::NewCRDT(crdt_type) => {
                        item_type.insert(op.0, crdt_type);
                    }
                }

                assert!(op.0 < cg_len);
            }

            // Check the operations are sorted
            assert!(is_sorted_iter_uniq(info.ops.iter().map(|(v, _)| *v)));

            // Check the index contains the correct items
            for idx in info.supremum.iter() {
                let v = info.ops[*idx].0;
                let (idx_crdt, idx_key) = self.map_index.get(&v).unwrap();
                assert_eq!(idx_crdt, crdt);
                assert_eq!(idx_key, key);
                expected_idx_count += 1;

                if deep {
                    // Check the supremum is correct.
                    let all_versions = info.ops.iter().map(|(v, _)| *v).collect::<Vec<_>>();
                    let dominators = self.cg.graph.find_dominators(&all_versions);

                    let sup_versions = info.supremum.iter().map(|idx| info.ops[*idx].0).collect::<Vec<_>>();
                    assert_eq!(dominators.as_ref(), &sup_versions);
                }
            }
        }
        assert_eq!(self.map_index.len(), expected_idx_count);

        // And now text operations
        let mut expected_idx_count = 0;
        for (crdt, info) in self.texts.iter() {
            assert_ne!(*crdt, ROOT_CRDT_ID);
            assert_eq!(*item_type.get(crdt).unwrap(), CRDTKind::Text);

            // Check the operations are sorted
            assert!(is_sorted_iter_uniq(info.ops.iter().map(|KVPair(v, _)| *v)));

            for v in info.frontier.as_ref() {
                assert!(*v < cg_len);

                let index_crdt = self.text_index.get(v).unwrap();
                assert_eq!(index_crdt, crdt);
                expected_idx_count += 1;
            }

            if deep {
                // Also check the version is correct.
                let all_versions = info.ops.iter().map(|op| op.last()).collect::<Vec<_>>();
                let dominators = self.cg.graph.find_dominators(&all_versions);
                assert_eq!(dominators, info.frontier);
            }
        }
        assert_eq!(self.text_index.len(), expected_idx_count);
    }

    pub fn new() -> Self {
        Default::default()
    }

    // The way I'm using this below, it should be idempotent.
    fn create_child_crdt(&mut self, v: LV, kind: CRDTKind) {
        match kind {
            CRDTKind::Map => {}
            CRDTKind::Register => {}
            CRDTKind::Collection => {}
            CRDTKind::Text => {
                self.texts.entry(v).or_default();
            }
        }
    }


    pub fn local_map_set(&mut self, agent: AgentId, crdt: LVKey, key: &str, value: CreateValue) -> LV {
        let v = self.cg.assign_local_op(agent, 1).start;
        if let CreateValue::NewCRDT(kind) = value {
            self.create_child_crdt(v, kind);
        }

        let mut entry = self.map_keys.entry((crdt, key.into()))
            .or_default();

        let new_idx = entry.ops.len();

        // Remove the old supremum from the index
        for idx in &entry.supremum {
            self.map_index.remove(&entry.ops[*idx].0);
        }

        entry.supremum = smallvec![new_idx];
        entry.ops.push((v, value));

        self.map_index.insert(v, (crdt, key.into()));
        v
    }

    // This function requires that the lv has already been added to the causal graph.
    pub fn remote_map_set(&mut self, crdt: LVKey, v: LV, key: &str, value: CreateValue) {
        if let CreateValue::NewCRDT(kind) = value {
            self.create_child_crdt(v, kind);
        }

        let mut entry = self.map_keys.entry((crdt, key.into()))
            .or_default();

        // If the entry already contains the new op, ignore it.
        if entry.ops.binary_search_by_key(&v, |e| e.0).is_ok() {
            return;
        }

        if let Some(last_op) = entry.ops.last() {
            // The added operation must have a higher local version than the last version.
            assert!(last_op.0 < v);
        }

        let new_idx = entry.ops.len();
        entry.ops.push((v, value));

        // The normal case is that the new operation replaces the old value. A faster implementation
        // would special case that and fall back to the more complex version if need be.
        let mut new_sup = smallvec![new_idx];
        self.map_index.insert(v, (crdt, key.into()));

        for s_idx in &entry.supremum {
            let s_v = entry.ops[*s_idx].0;
            match self.cg.graph.version_cmp(s_v, v) {
                None => {
                    // Versions are concurrent. Leave the old entry in index.
                    new_sup.push(*s_idx);
                }
                Some(Ordering::Less) => {
                    // The most common case. The new version dominates the old version. Remove the
                    // old version from the index.
                    self.map_index.remove(&s_v);
                }
                Some(_) => {
                    panic!("Invalid state");
                }
            }
        }
        entry.supremum = new_sup;
    }

    pub fn local_text_op(&mut self, agent: AgentId, crdt: LVKey, op: TextOperation) {
        let v_range = self.cg.assign_local_op(agent, op.len());

        let entry = self.texts.get_mut(&crdt).unwrap();

        // Remove it from the index
        for v in entry.frontier.as_ref() {
            let old_index_item = self.text_index.remove(v);
            assert!(old_index_item.is_some());
        }

        entry.local_push_op(op, v_range);

        // And add it back to the index.
        self.text_index.insert(v_range.last(), crdt);
    }

    pub fn remote_text_op(&mut self, crdt: LVKey, v_range: DTRange, op: TextOperation) {
        debug_assert_eq!(v_range.len(), op.len());

        // What should we do here if the item is missing?
        let entry = self.texts.get_mut(&crdt).unwrap();

        // Remove it from the index
        for v in entry.frontier.as_ref() {
            let old_index_item = self.text_index.remove(v);
            assert!(old_index_item.is_some());
        }

        entry.remote_push_op_unknown_parents(op, v_range, &self.cg.graph);

        // And add it back to the index.
        for v in entry.frontier.as_ref() {
            self.text_index.insert(*v, crdt);
        }
    }

    fn create_to_snapshot(v: LV, create: &CreateValue) -> RegisterValue {
        match create {
            CreateValue::Primitive(p) => RegisterValue::Primitive(p.clone()),
            CreateValue::NewCRDT(kind) => RegisterValue::OwnedCRDT(*kind, v)
        }
    }

    fn resolve_mv(&self, reg: &RegisterInfo) -> RegisterValue {
        let s = match reg.supremum.len() {
            0 => panic!("Internal consistency violation"),
            1 => reg.supremum[0],
            _ => {
                reg.supremum.iter()
                    .map(|s| (*s, self.cg.agent_assignment.lv_to_agent_version(reg.ops[*s].0)))
                    .max_by(|(_, a), (_, b)| {
                        self.cg.agent_assignment.tie_break_crdt_versions(*a, *b)
                    })
                    .unwrap().0
            }
        };

        let (v, value) = &reg.ops[s];
        Self::create_to_snapshot(*v, value)
    }

    pub fn checkout_text(&self, crdt: LVKey) -> JumpRopeBuf {
        let info = self.texts.get(&crdt).unwrap();

        let mut result = JumpRopeBuf::new();
        info.merge_into(&mut result, &self.cg, &[], self.cg.version.as_ref());
        result
    }

    pub fn checkout_map(&self, crdt: LVKey) -> BTreeMap<SmartString, Box<DTValue>> {
        let empty_str: SmartString = "".into();
        // dbg!((crdt, empty_str.clone())..(crdt, empty_str));
        let iter = if crdt == ROOT_CRDT_ID {
            // For the root CRDT we can't use the crdt+1 trick because the range wraps around.
            self.map_keys.range((crdt, empty_str)..)
        } else {
            self.map_keys.range((crdt, empty_str.clone())..(crdt + 1, empty_str))
        };

        iter.map(|((_, key), info)| {
            let inner = match self.resolve_mv(info) {
                RegisterValue::Primitive(p) => DTValue::Primitive(p),
                RegisterValue::OwnedCRDT(kind, child_crdt) => {
                    match kind {
                        CRDTKind::Map => DTValue::Map(self.checkout_map(child_crdt)),
                        CRDTKind::Text => DTValue::Text(self.checkout_text(child_crdt).to_string()),
                        _ => unimplemented!(),
                        // CRDTKind::Register => {}
                        // CRDTKind::Collection => {}
                        // CRDTKind::Text => {}
                    }
                }
            };
            (key.clone(), Box::new(inner))
        }).collect()
    }

    pub fn checkout(&self) -> BTreeMap<SmartString, Box<DTValue>> {
        self.checkout_map(ROOT_CRDT_ID)
    }
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

impl ExperimentalOpLog {
    fn crdt_name_to_remote(&self, crdt: LVKey) -> RemoteVersion {
        if crdt == ROOT_CRDT_ID {
            RemoteVersion("ROOT", 0)
        } else {
            self.cg.agent_assignment.local_to_remote_version(crdt)
        }
    }

    fn remote_to_crdt_name(&self, crdt_rv: RemoteVersion) -> LVKey {
        if crdt_rv.0 == "ROOT" { ROOT_CRDT_ID }
        else { self.cg.agent_assignment.remote_to_local_version(crdt_rv) }
    }

    // pub fn xf_text_changes_since(&self, text_item: LVKey, since_frontier: &[LV]) {
    //     let crdt = self.texts.get(&text_item).unwrap();
    //
    //     crdt.iter_xf_operations_from(&sel)
    // }

    pub fn ops_since(&self, since_frontier: &[LV]) -> SerializedOps {
        let mut write_map = WriteMap::with_capacity_from(&self.cg.agent_assignment.client_data);

        let diff = self.cg.graph.diff(since_frontier, self.cg.version.as_ref()).1;
        // let bump = Bump::new();
        // let mut result = bumpalo::collections::Vec::new_in(&bump);
        let mut cg_changes = Vec::new();
        let mut text_crdts_to_send = BTreeSet::new();
        let mut map_crdts_to_send = BTreeSet::new();
        for range in diff.iter() {
            let iter = self.cg.iter_range(*range);
            write_cg_entry_iter(&mut cg_changes, iter, &mut write_map, &self.cg);

            for (_, text_crdt) in self.text_index.range(*range) {
                // dbg!(text_crdt);
                // self.texts[text_crdt].
                text_crdts_to_send.insert(*text_crdt);
            }

            for (_, (map_crdt, key)) in self.map_index.range(*range) {
                // dbg!(map_crdt, key);
                map_crdts_to_send.insert((*map_crdt, key));
            }
        }
        // dbg!(write_map);

        // Serialize map operations
        let mut map_ops = Vec::new();
        for (crdt, key) in map_crdts_to_send {
            let crdt_name = self.crdt_name_to_remote(crdt);
            let entry = self.map_keys.get(&(crdt, key.clone()));
            if let Some(entry) = entry {
                for r in diff.iter() {
                    // Find all the unknown ops.
                    // TODO: Add a flag to trim this to only the most recent ops.
                    let start_idx = entry.ops
                        .binary_search_by_key(&r.start, |e| e.0)
                        .unwrap_or_else(|idx| idx);

                    for pair in &entry.ops[start_idx..] {
                        if pair.0 >= r.end { break; }

                        // dbg!(pair);
                        let rv = self.cg.agent_assignment.local_to_remote_version(pair.0);
                        map_ops.push((crdt_name, rv, key.as_str(), pair.1.clone()));
                    }
                }
            }
        }

        // Serialize text operations
        let mut text_context = ListOperationCtx::new();
        let mut text_ops = Vec::new();
        for crdt in text_crdts_to_send {
            let crdt_name = self.crdt_name_to_remote(crdt);
            let info = &self.texts[&crdt];
            for r in diff.iter() {
                for KVPair(lv, op) in info.ops.iter_range_ctx(*r, &info.ctx) {
                    // dbg!(&op);

                    let op_out = ListOpMetrics {
                        loc: op.loc,
                        kind: op.kind,
                        content_pos: op.content_pos.map(|content_pos| {
                            let content = info.ctx.get_str(op.kind, content_pos);
                            text_context.push_str(op.kind, content)
                        }),
                    };
                    let rv = self.cg.agent_assignment.local_to_remote_version(lv);
                    text_ops.push((crdt_name, rv, op_out));
                }
            }
        }

        // dbg!(std::str::from_utf8(&text_content).unwrap());
        // dbg!(&text_ops);

        // And changes from text edits


        SerializedOps {
            cg_changes,
            map_ops,
            text_ops,
            text_context,
        }
        // dbg!(&result);

        // let mut new_cg = CausalGraph::new();
        // let mut read_map = ReadMap::new();
        // read_cg_entry_into_cg(&mut BufParser(&result), true, &mut new_cg, &mut read_map).unwrap();
        // dbg!(new_cg);
    }


    pub fn merge_ops(&mut self, changes: SerializedOps) -> Result<(), ParseError> {
        let mut read_map = ReadMap::new();
        let new_range = read_cg_entry_into_cg(&mut BufParser(&changes.cg_changes), true, &mut self.cg, &mut read_map)?;
        // dbg!(read_map);

        for (crdt_r_name, rv, key, val) in changes.map_ops {
            let lv = self.cg.agent_assignment.remote_to_local_version(rv);
            if new_range.contains(lv) {
                let crdt_id = self.remote_to_crdt_name(crdt_r_name);
                // dbg!(crdt_id, lv, key, val);
                self.remote_map_set(crdt_id, lv, key, val);
            }
        }

        for (crdt_r_name, rv, mut op_metrics) in changes.text_ops {
            let lv = self.cg.agent_assignment.remote_to_local_version(rv);
            let v_range: DTRange = (lv..lv + op_metrics.len()).into();

            if v_range.end <= new_range.start { continue; }
            else if v_range.start < new_range.start {
                // Trim the new operation.
                op_metrics.truncate_keeping_right_ctx(new_range.start - v_range.start, &changes.text_context);
            }

            let crdt_id = self.remote_to_crdt_name(crdt_r_name);

            let op = op_metrics.to_operation(&changes.text_context);
            self.remote_text_op(crdt_id, v_range, op);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::experiments::ExperimentalOpLog;
    use crate::{CRDTKind, CreateValue, Primitive, ROOT_CRDT_ID};
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
}