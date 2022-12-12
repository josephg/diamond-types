use std::collections::{BTreeMap, BTreeSet};
use smallvec::smallvec;
use std::cmp::Ordering;
use jumprope::JumpRopeBuf;
use smartstring::alias::String as SmartString;

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer};

use rle::{HasLength, SplitableSpanCtx};
use crate::causalgraph::agent_assignment::remote_ids::RemoteVersion;
use crate::experiments::{ExperimentalOpLog, LVKey, RegisterInfo, RegisterValue, SerializedOps, ValPair};
use crate::{AgentId, CRDTKind, CreateValue, DTRange, LV, ROOT_CRDT_ID};
use crate::branch::DTValue;
use crate::encoding::bufparser::BufParser;
use crate::encoding::cg_entry::{read_cg_entry_into_cg, write_cg_entry_iter};
use crate::encoding::map::{ReadMap, WriteMap};
use crate::encoding::parseerror::ParseError;
use crate::frontier::{is_sorted_iter_uniq, is_sorted_slice};
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::operation::TextOperation;
use crate::rle::{KVPair, RleSpanHelpers};

#[cfg(feature = "serde")]
impl Serialize for ExperimentalOpLog {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        self.ops_since(&[]).serialize(serializer)
    }
}

pub(super) fn create_to_snapshot(v: LV, create: &CreateValue) -> RegisterValue {
    match create {
        CreateValue::Primitive(p) => RegisterValue::Primitive(p.clone()),
        CreateValue::NewCRDT(kind) => RegisterValue::OwnedCRDT(*kind, v)
    }
}
// Hmmmm... If this is equivalent, could I just use ValPair() instead of RegisterValue?
impl From<&ValPair> for RegisterValue {
    fn from((version, value): &ValPair) -> Self {
        create_to_snapshot(*version, value)
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

    // Its quite annoying, but RegisterInfo objects store the supremum as an array of indexes. This
    // returns the active index and (if necessary) the set of indexes of conflicting values.
    pub(crate) fn tie_break_mv<'a>(&self, reg: &'a RegisterInfo) -> (usize, Option<impl Iterator<Item = usize> + 'a>) {
        match reg.supremum.len() {
            0 => panic!("Internal consistency violation"),
            1 => (reg.supremum[0], None),
            _ => {
                let active_idx = reg.supremum.iter()
                    .map(|s| (*s, self.cg.agent_assignment.local_to_agent_version(reg.ops[*s].0)))
                    .max_by(|(_, a), (_, b)| {
                        self.cg.agent_assignment.tie_break_agent_versions(*a, *b)
                    })
                    .unwrap().0;

                (
                    active_idx,
                    Some(reg.supremum.iter().copied().filter(move |i| *i != active_idx))
                )
            }
        }
    }

    fn resolve_mv(&self, reg: &RegisterInfo) -> RegisterValue {
        let (active_idx, _) = self.tie_break_mv(reg);

        let (v, value) = &reg.ops[active_idx];
        create_to_snapshot(*v, value)
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

    pub fn crdt_at_path(&self, path: &[&str]) -> (CRDTKind, LVKey) {
        let mut kind = CRDTKind::Map;
        let mut key = ROOT_CRDT_ID;

        for p in path {
            match kind {
                CRDTKind::Map => {
                    let container = self.map_keys.get(&(key, (*p).into()))
                        .unwrap();
                    match self.resolve_mv(container) {
                        RegisterValue::Primitive(_) => {
                            panic!("Found primitive, not CRDT");
                        }
                        RegisterValue::OwnedCRDT(new_kind, new_key) => {
                            kind = new_kind;
                            key = new_key;
                        }
                    }
                }
                _ => {
                    panic!("Invalid path in document");
                }
            }
        }

        (kind, key)
    }

    pub fn text_at_path(&self, path: &[&str]) -> LVKey {
        let (kind, key) = self.crdt_at_path(path);
        if kind != CRDTKind::Text {
            panic!("Unexpected CRDT kind {:?}", kind);
        } else { key }
    }

    pub fn text_changes_since(&self, text: LVKey, since_frontier: &[LV]) -> Vec<(DTRange, Option<TextOperation>)> {
        let info = self.texts.get(&text).unwrap();
        info.xf_operations_from(&self.cg, since_frontier, self.cg.version.as_ref())
    }
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
                text_crdts_to_send.insert(*text_crdt);
            }

            for (_, (map_crdt, key)) in self.map_index.range(*range) {
                // dbg!(map_crdt, key);
                map_crdts_to_send.insert((*map_crdt, key));
            }
        }

        // Serialize map operations
        let mut map_ops = Vec::new();
        for (crdt, key) in map_crdts_to_send {
            let crdt_name = self.crdt_name_to_remote(crdt);
            let entry = self.map_keys.get(&(crdt, key.clone()))
                .unwrap();
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

        SerializedOps {
            cg_changes,
            map_ops,
            text_ops,
            text_context,
        }
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
