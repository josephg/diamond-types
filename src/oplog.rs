use std::collections::{BTreeMap, BTreeSet};
use smallvec::smallvec;
use std::cmp::Ordering;
use jumprope::JumpRopeBuf;
use smartstring::alias::String as SmartString;

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer};

use rle::{HasLength, SplitableSpanCtx};
use crate::causalgraph::agent_assignment::remote_ids::RemoteVersion;
use crate::{AgentId, CRDTKind, CreateValue, DTRange, DTValue, OpLog, LV, LVKey, RegisterInfo, RegisterValue, ROOT_CRDT_ID, SerializedOps, ValPair};
use crate::encoding::bufparser::BufParser;
use crate::encoding::cg_entry::{read_cg_entry_into_cg, write_cg_entry_iter};
use crate::encoding::map::{ReadMap, WriteMap};
use crate::encoding::parseerror::ParseError;
use crate::branch::btree_range_for_crdt;
use crate::frontier::{is_sorted_iter_uniq, is_sorted_slice};
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::operation::TextOperation;
use crate::rle::{KVPair, RleSpanHelpers};

#[cfg(feature = "serde")]
impl Serialize for OpLog {
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

impl OpLog {
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

        if deep {
            // Find all the CRDTs which have been created then later overwritten or deleted.
            let mut deleted_crdts = BTreeSet::new();
            let mut directly_overwritten_maps = vec![];
            for (_, reg_info) in &self.map_keys {
                for (idx, (lv, val)) in reg_info.ops.iter().enumerate() {
                    if !reg_info.supremum.contains(&idx) {
                        if let CreateValue::NewCRDT(kind) = val {
                            deleted_crdts.insert(*lv);

                            if *kind == CRDTKind::Map {
                                directly_overwritten_maps.push(*lv);
                            }
                        }
                    }
                }
            }

            // Now find everything that has been removed indirectly
            let mut queue = directly_overwritten_maps;
            while let Some(crdt_id) = queue.pop() {
                for (_, info) in btree_range_for_crdt(&self.map_keys, crdt_id) {
                    for s in info.supremum.iter() {
                        let (lv, create_val) = &info.ops[*s];
                        if let CreateValue::NewCRDT(kind) = create_val {
                            assert_eq!(true, deleted_crdts.insert(*lv));

                            if *kind == CRDTKind::Map {
                                // Go through this CRDT's children.
                                queue.push(*lv);
                            }
                        }
                    }
                }
            }

            assert_eq!(deleted_crdts, self.deleted_crdts);

            // // Recursively traverse the "alive" data, checking that the deleted_crdts data is
            // // correct.
            //
            // // First lets make a set of all the CRDTs which are "alive".
            // let mut all_crdts: BTreeSet<LV> = self.texts.keys().copied().collect();
            // let mut last_crdt = ROOT_CRDT_ID;
            // for (crdt, _) in self.map_keys.keys() {
            //     if *crdt != last_crdt {
            //         last_crdt = *crdt;
            //         all_crdts.insert(*crdt);
            //     }
            // }
            // dbg!(&all_crdts);
            //
            // // Now recursively walk the map CRDTs looking for items which aren't deleted.
            //
            // let mut dead_crdts = all_crdts;
            // let mut crdt_maps = vec![ROOT_CRDT_ID];
            // dead_crdts.remove(&ROOT_CRDT_ID);
            //
            // // Recursively go through all the "alive" items and remove them from dead_crdts.
            // while let Some(crdt) = crdt_maps.pop() {
            //     for (_, info) in btree_range_for_crdt(&self.map_keys, crdt) {
            //         for s in info.supremum.iter() {
            //             let (lv, create_val) = &info.ops[*s];
            //             if let CreateValue::NewCRDT(kind) = create_val {
            //                 assert!(dead_crdts.remove(lv));
            //                 if *kind == CRDTKind::Map {
            //                     // Go through this CRDT's children.
            //                     crdt_maps.push(*lv);
            //                 }
            //             }
            //         }
            //     }
            // }
        }
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

    fn recursive_mark_deleted_inner(&mut self, mut to_delete: Vec<LV>) {
        while let Some(crdt) = to_delete.pop() {
            for (_, info) in btree_range_for_crdt(&self.map_keys, crdt) {
                for s in info.supremum.iter() {
                    let (lv, create_val) = &info.ops[*s];
                    if let CreateValue::NewCRDT(kind) = create_val {
                        assert!(self.deleted_crdts.insert(*lv));

                        if *kind == CRDTKind::Map {
                            // Go through this CRDT's children.
                            to_delete.push(*lv);
                        }
                    }
                }
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

        let mut to_delete = vec![];
        // Remove the old supremum from the index
        for idx in &entry.supremum {
            let (lv, val) = &entry.ops[*idx];
            if let CreateValue::NewCRDT(kind) = val {
                assert!(self.deleted_crdts.insert(*lv));
                if *kind == CRDTKind::Map {
                    to_delete.push(*lv);
                }
            }

            self.map_index.remove(&lv);
        }

        entry.supremum = smallvec![new_idx];
        entry.ops.push((v, value));

        self.map_index.insert(v, (crdt, key.into()));

        // dbg!((crdt, key, &to_delete));
        self.recursive_mark_deleted_inner(to_delete);
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
        let mut to_delete = vec![];

        for s_idx in &entry.supremum {
            let (old_lv, old_val) = &entry.ops[*s_idx];
            match self.cg.graph.version_cmp(*old_lv, v) {
                None => {
                    // Versions are concurrent. Leave the old entry in index.
                    new_sup.push(*s_idx);
                }
                Some(Ordering::Less) => {
                    // The most common case. The new version dominates the old version. Remove the
                    // old (version, value) pair.
                    if let CreateValue::NewCRDT(kind) = old_val {
                        assert!(self.deleted_crdts.insert(*old_lv));
                        if *kind == CRDTKind::Map {
                            to_delete.push(*old_lv);
                        }
                    }
                    self.map_index.remove(old_lv);
                }
                Some(_) => {
                    // Either the versions are equal, or the newly inserted version is earlier than
                    // the existing version. Either way, this is an invalid operation.
                    panic!("Invalid state");
                }
            }
        }
        entry.supremum = new_sup;
        self.recursive_mark_deleted_inner(to_delete);
    }

    pub fn local_text_op(&mut self, agent: AgentId, crdt: LVKey, op: TextOperation) -> DTRange {
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

        v_range
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

impl OpLog {
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

        let diff_rev = self.cg.graph.diff_rev(since_frontier, self.cg.version.as_ref()).1;
        // let bump = Bump::new();
        // let mut result = bumpalo::collections::Vec::new_in(&bump);
        let mut cg_changes = Vec::new();
        let mut text_crdts_to_send = BTreeSet::new();
        let mut map_crdts_to_send = BTreeSet::new();
        for range_rev in diff_rev.iter() {
            let iter = self.cg.iter_range(*range_rev);
            write_cg_entry_iter(&mut cg_changes, iter, &mut write_map, &self.cg);

            for (_, text_crdt) in self.text_index.range(*range_rev) {
                text_crdts_to_send.insert(*text_crdt);
            }

            for (_, (map_crdt, key)) in self.map_index.range(*range_rev) {
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
            for r in diff_rev.iter() {
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
            for r in diff_rev.iter() {
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


    pub fn merge_ops(&mut self, changes: SerializedOps) -> Result<DTRange, ParseError> {
        let mut read_map = ReadMap::new();

        let old_end = self.cg.len();

        let mut buf = BufParser(&changes.cg_changes);
        while !buf.is_empty() {
            read_cg_entry_into_cg(&mut buf, true, &mut self.cg, &mut read_map)?;
        }

        let new_end = self.cg.len();
        let new_range: DTRange = (old_end..new_end).into();

        if new_range.is_empty() { return Ok(new_range); }

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
            let mut v_range: DTRange = (lv..lv + op_metrics.len()).into();

            if v_range.end <= new_range.start { continue; }
            else if v_range.start < new_range.start {
                // Trim the new operation.
                op_metrics.truncate_keeping_right_ctx(new_range.start - v_range.start, &changes.text_context);
                v_range.start = new_range.start;
            }

            let crdt_id = self.remote_to_crdt_name(crdt_r_name);

            let op = op_metrics.to_operation(&changes.text_context);
            self.remote_text_op(crdt_id, v_range, op);
        }

        Ok(new_range)
    }

    pub fn xf_text_changes_since(&self, text_crdt: LVKey, since: &[LV]) -> Vec<(DTRange, Option<TextOperation>)> {
        let textinfo = self.texts.get(&text_crdt).unwrap();
        textinfo.xf_operations_from(&self.cg, since, textinfo.frontier.as_ref())
    }
}


#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};
    use crate::{CRDTKind, CreateValue, OpLog, Primitive, ROOT_CRDT_ID, SerializedOps};
    use crate::causalgraph::agent_assignment::remote_ids::RemoteVersion;
    use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
    use crate::list::operation::TextOperation;

    #[test]
    fn smoke() {
        let mut oplog = OpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        oplog.local_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(123)));
        oplog.local_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(321)));

        dbg!(&oplog);
        oplog.dbg_check(true);
    }

    #[test]
    fn text() {
        let mut oplog = OpLog::new();

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
        let mut oplog_2 = OpLog::new();
        oplog_2.merge_ops(c).unwrap();
        assert_eq!(oplog_2.cg, oplog.cg);
        // dbg!(oplog_2)
        // dbg!(oplog_2.checkout());
        oplog_2.dbg_check(true);

        assert_eq!(oplog.checkout(), oplog_2.checkout());
    }

    #[test]
    fn concurrent_changes() {
        let mut oplog1 = OpLog::new();
        let mut oplog2 = OpLog::new();


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
        let mut oplog = OpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        oplog.local_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(123)));
        let map = oplog.local_map_set(seph, ROOT_CRDT_ID, "yo", CreateValue::NewCRDT(CRDTKind::Map));
        oplog.local_map_set(seph, map, "yo", CreateValue::Primitive(Primitive::Str("blah".into())));

        dbg!(oplog.checkout());
        oplog.dbg_check(true);
    }

    #[test]
    fn overwrite_local() {
        let mut oplog = OpLog::new();
        let seph = oplog.cg.get_or_create_agent_id("seph");

        let child_obj = oplog.local_map_set(seph, ROOT_CRDT_ID, "overwritten", CreateValue::NewCRDT(CRDTKind::Map));
        let text_item = oplog.local_map_set(seph, child_obj, "text_item", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, text_item, TextOperation::new_insert(0, "yooo"));
        oplog.local_map_set(seph, child_obj, "smol_embedded", CreateValue::NewCRDT(CRDTKind::Map));

        // Now overwrite the parent item.
        oplog.local_map_set(seph, ROOT_CRDT_ID, "overwritten", CreateValue::Primitive(Primitive::I64(123)));

        // dbg!(&oplog);
        oplog.dbg_check(true);
    }

    #[test]
    fn overwrite_remote() {
        let mut oplog = OpLog::new();
        let seph = oplog.cg.get_or_create_agent_id("seph");

        let child_obj = oplog.local_map_set(seph, ROOT_CRDT_ID, "overwritten", CreateValue::NewCRDT(CRDTKind::Map));
        let text_item = oplog.local_map_set(seph, child_obj, "text_item", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, text_item, TextOperation::new_insert(0, "yooo"));
        oplog.local_map_set(seph, child_obj, "smol_embedded", CreateValue::NewCRDT(CRDTKind::Map));

        // Now overwrite the parent item with a remote operation.
        let lv = oplog.cg.assign_local_op(seph, 1).start;
        oplog.remote_map_set(ROOT_CRDT_ID, lv, "overwritten", CreateValue::Primitive(Primitive::I64(123)));

        oplog.dbg_check(true);
    }

    #[test]
    fn overlapping_updates() {
        // Regression.
        let mut oplog = OpLog::new();
        let mut oplog2 = OpLog::new();
        let seph = oplog.cg.get_or_create_agent_id("seph");

        let text_item = oplog.local_map_set(seph, ROOT_CRDT_ID, "overwritten", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, text_item, TextOperation::new_insert(0, "a"));

        let partial_update = oplog.ops_since(&[]);
        oplog2.merge_ops(partial_update).unwrap();

        oplog.local_text_op(seph, text_item, TextOperation::new_insert(1, "b"));
        let full_update = oplog.ops_since(&[]);

        oplog2.merge_ops(full_update).unwrap();
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