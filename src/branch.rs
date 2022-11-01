use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use smallvec::{Array, smallvec};
use crate::*;
use smartstring::alias::String as SmartString;
use ::rle::HasLength;
use crate::frontier::{advance_frontier_by, advance_frontier_by_known_run};
use crate::list::operation::ListOpKind;
use crate::oplog::ROOT_MAP;

/// This is used for checkouts.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DTValue {
    Primitive(Primitive),
    // Register(Box<DTValue>),
    Map(BTreeMap<SmartString, Box<DTValue>>),
    Set(BTreeMap<LV, Box<DTValue>>),
    Text(String),
}

impl DTValue {
    pub fn unwrap_primitive(self) -> Primitive {
        match self {
            DTValue::Primitive(p) => p,
            _ => { panic!("Expected primitive") }
        }
    }

    pub fn unwrap_map(self) -> BTreeMap<SmartString, Box<DTValue>> {
        match self {
            DTValue::Map(map) => map,
            _ => { panic!("Expected map") }
        }
    }

    pub fn unwrap_set(self) -> BTreeMap<LV, Box<DTValue>> {
        match self {
            DTValue::Set(set) => set,
            _ => { panic!("Expected set") }
        }
    }
}

/// Separate items in the slice into two groups based on a predicate function.
///
/// Returns the index of the first item in the right set (or the length of the slice).
fn separate_by<T, R>(arr: &mut [T], mut right: R)
    -> usize
    where R: FnMut(&T) -> bool
{
    let mut right_size = 0;
    let len = arr.len();
    for i in 0..len {
        if right(&arr[i]) {
            right_size += 1;
        } else if right_size > 0 {
            arr.swap(i - right_size, i);
        }
    }

    len - right_size
}

impl Branch {
    pub fn new() -> Self {
        let mut overlay = BTreeMap::new();
        overlay.insert(ROOT_MAP, OverlayValue::Map(BTreeMap::new()));

        Self {
            overlay,
            overlay_version: Default::default(),
            num_invalid: 0
        }
    }

    fn snapshot_to_value(&self, val: &SnapshotValue, cg: &CausalGraph) -> Option<DTValue> {
        match val {
            SnapshotValue::Primitive(prim) => {
                Some(DTValue::Primitive(prim.clone()))
            }
            SnapshotValue::InnerCRDT(inner_crdt) => {
                self.get_recursive_at(*inner_crdt, cg)
            }
        }
    }

    fn state_to_value(&self, reg: &RegisterState, cg: &CausalGraph) -> Option<DTValue> {
        self.snapshot_to_value(&reg.value, cg)
    }

    fn resolve_mv<'a>(&'a self, reg: &'a MVRegister, cg: &CausalGraph) -> &'a RegisterState {
        match reg.len() {
            0 => panic!("Internal consistency violation: Register has no value"),
            1 => &reg[0],
            _ => {
                // We need to pick a winner. Winner is chosen by max(agent,seq).
                let idx = reg.iter()
                    .enumerate()
                    .map(|(idx, r)| (idx, cg.version_to_crdt_id(r.version)))
                    .max_by(|a, b| {
                        // TODO: Check this is the right way around.
                        let a_id = &cg.client_data[a.1.agent as usize].name;
                        let b_id = &cg.client_data[b.1.agent as usize].name;
                        a_id.cmp(b_id)
                            .then_with(|| a.1.seq.cmp(&b.1.seq))
                    })
                    .unwrap().0;

                &reg[idx]
            }
        }
    }

    fn mv_to_single_value(&self, reg: &MVRegister, cg: &CausalGraph) -> Option<DTValue> {
        self.state_to_value(self.resolve_mv(reg, cg), cg)
    }

    pub fn get_recursive_at(&self, crdt_id: LV, cg: &CausalGraph) -> Option<DTValue> {
        match self.overlay.get(&crdt_id)? {
            OverlayValue::Register(reg) => self.mv_to_single_value(reg, cg),
            OverlayValue::Map(map) => {
                Some(DTValue::Map(map.iter().filter_map(|(key, reg)| {
                    Some((key.clone(), Box::new(self.mv_to_single_value(reg, cg)?)))
                }).collect()))
            }
            OverlayValue::Set(id_set) => {
                Some(DTValue::Set(id_set.iter().filter_map(|(t, val)| {
                    Some((*t, Box::new(self.snapshot_to_value(val, cg)?)))
                }).collect()))
            }
            OverlayValue::Text(rope) => {
                Some(DTValue::Text(rope.to_string()))
            }
        }
    }

    pub fn get_recursive(&self, cg: &CausalGraph) -> Option<DTValue> {
        self.get_recursive_at(ROOT_MAP, cg)
    }

    pub(super) fn get_value_of_lww(&self, lww_id: LV) -> Option<&MVRegister> {
        self.overlay.get(&lww_id).and_then(|val| {
            match val {
                OverlayValue::Register(val) => Some(val),
                _ => None,
            }
        })
    }

    fn get_map(&self, map: LV) -> Option<&BTreeMap<SmartString, MVRegister>> {
        self.overlay.get(&map).and_then(|val| {
            match val {
                OverlayValue::Map(inner_map) => Some(inner_map),
                _ => None,
            }
        })
    }

    pub(super) fn get_map_value(&self, map: LV, key: &str) -> Option<&MVRegister> {
        self.get_map(map).and_then(|inner_map| {
            inner_map.get(key)
        })
    }

    fn get_register(&self, crdt_id: LV, key: Option<&str>) -> Option<&MVRegister> {
        if let Some(key) = key {
            match self.overlay.get(&crdt_id)? {
                OverlayValue::Map(inner_map) => {
                    Some(inner_map.get(key)?)
                },
                _ => None,
            }
        } else {
            match self.overlay.get(&crdt_id)? {
                OverlayValue::Register(val) => Some(val),
                _ => None,
            }
        }
    }

    // fn get_register_mut(&mut self, crdt_id: Time, key: Option<&str>) -> Option<&mut LWWValue> {
    //     if let Some(key) = key {
    //         match self.overlay.get_mut(&crdt_id)? {
    //             OverlayValue::Map(inner_map) => {
    //                 Some(inner_map.entry(key.into()).or_insert_with(|| {
    //                     self.num_invalid += 1;
    //                     LWWValue {
    //                         value: Some(Value::Primitive(Primitive::InvalidUninitialized)),
    //                         last_modified: 0
    //                     }
    //                 }))
    //             },
    //             _ => None,
    //         }
    //     } else {
    //         match self.overlay.entry(crdt_id).or_insert_with(|| {
    //             self.num_invalid += 1;
    //             OverlayValue::LWW(LWWValue {
    //                 value: Some(Value::Primitive(Primitive::InvalidUninitialized)),
    //                 last_modified: 0
    //             })
    //         }) {
    //             OverlayValue::LWW(val) => Some(val),
    //             _ => None
    //         }
    //     }
    // }

    pub(crate) fn get_kind(&self, id: LV) -> CRDTKind {
        // TODO: Remove this unwrap() when we have an actual database.
        match self.overlay.get(&id).unwrap() {
            OverlayValue::Register(_) => CRDTKind::Register,
            OverlayValue::Map(_) => CRDTKind::Map,
            OverlayValue::Set(_) => CRDTKind::Set,
            OverlayValue::Text(_) => CRDTKind::Text,
        }
    }

    // *** Mutation operations ***

    fn internal_remove_crdt(&mut self, crdt_id: LV) {
        // This needs to recursively delete things.
        let old_value = self.overlay.remove(&crdt_id);
        todo!("Recurse!");
    }
    fn remove_old_value(&mut self, old_value: &SnapshotValue) {
        match old_value {
            SnapshotValue::Primitive(Primitive::InvalidUninitialized) => {
                self.num_invalid -= 1;
            }
            SnapshotValue::InnerCRDT(crdt_id) => {
                self.internal_remove_crdt(*crdt_id);
            }
            _ => {}
        }
    }


    fn inner_create_crdt(&mut self, time: LV, kind: CRDTKind) {
        let new_value = match kind {
            CRDTKind::Map => OverlayValue::Map(BTreeMap::new()),
            CRDTKind::Set => OverlayValue::Set(BTreeMap::new()),
            CRDTKind::Register => {
                self.num_invalid += 1;
                OverlayValue::Register(smallvec![RegisterState {
                    value: SnapshotValue::Primitive(Primitive::InvalidUninitialized),
                    version: time
                }])
            }
            CRDTKind::Text => {
                OverlayValue::Text(Box::new(JumpRope::new()))
            }
        };

        let old_val = self.overlay.insert(time, new_value);
        assert!(old_val.is_none());
    }

    fn op_to_snapshot_value(&mut self, time: LV, value: &CreateValue) -> SnapshotValue {
        match value {
            CreateValue::Primitive(p) => SnapshotValue::Primitive(p.clone()),
            CreateValue::NewCRDT(kind) => {
                self.inner_create_crdt(time, *kind);
                SnapshotValue::InnerCRDT(time)
            }
            // OpValue::Deleted => None,
        }
    }

    pub(crate) fn set_time(&mut self, time: LV) {
        self.overlay_version = smallvec![time];
    }


    fn modify_reg_internal(&mut self, time: LV, reg_id: LV, op_value: &CreateValue, _cg: &CausalGraph) {
        let value = self.op_to_snapshot_value(time, op_value);

        let inner = match self.overlay.get_mut(&reg_id).unwrap() {
            OverlayValue::Register(lww) => lww,
            _ => { panic!("Cannot set register value in map"); }
        };
        // inner.version = time;

        todo!()
        // let old_value = std::mem::replace(&mut inner.value, value);
        //
        // self.remove_old_value(old_value);
    }

    pub fn modify_reg_local(&mut self, time: LV, lww_id: LV, op_value: &CreateValue, cg: &CausalGraph) {
        self.modify_reg_internal(time, lww_id, op_value, cg);
        self.set_time(time);
    }

    fn modify_map_internal(&mut self, time: LV, map_id: LV, key: &str, op_value: &CreateValue, cg: &CausalGraph) {
        let value = self.op_to_snapshot_value(time, op_value);

        let inner = match self.overlay.get_mut(&map_id).unwrap() {
            OverlayValue::Map(map) => map,
            _ => { panic!("Cannot set map value in LWW"); }
        };

        let entry = inner.entry(key.into())
            .or_default();

        // We need to remove values after the retain() loop to prevent borrowck issues.
        let keep_idx = separate_by(&mut entry.as_mut(), |reg| {
            cg.parents.version_contains_time(&[time], reg.version)
        });

        // Borrowck is the worst.
        let mut crdts_to_remove: SmallVec<[_; 2]> = smallvec![];
        for e in entry[keep_idx..].iter() {
            match e.value {
                SnapshotValue::Primitive(Primitive::InvalidUninitialized) => {
                    self.num_invalid -= 1;
                }
                SnapshotValue::InnerCRDT(crdt_id) => {
                    crdts_to_remove.push(crdt_id);
                }
                _ => {}
            }
            // self.remove_old_value(&e.value);
        }
        entry.truncate(keep_idx);
        entry.push(RegisterState {
            value,
            version: time
        });

        for id in crdts_to_remove.iter() {
            self.internal_remove_crdt(*id);
        }

        // TODO: Consider sorting the values here in case of a conflict, so we
        // don't need to refer to the causal graph on read operations.
    }

    pub fn modify_map_local(&mut self, time: LV, lww_id: LV, key: &str, op_value: &CreateValue, cg: &CausalGraph) {
        self.modify_map_internal(time, lww_id, key, op_value, cg);
        self.set_time(time);
    }

    // pub(crate) fn create_inner(&mut self, time_now: Time, agent_id: AgentId, crdt_id: Time, key: Option<&str>, kind: CRDTKind) {
    //     self.inner_register_set(time_now, crdt_id, key, SnapshotValue::InnerCRDT(time_now));
    //     self.inner_create_crdt(time_now, kind);
    // }

    fn internal_get_set(&mut self, set_id: LV) -> &mut BTreeMap<LV, SnapshotValue> {
        match self.overlay.get_mut(&set_id).unwrap() {
            OverlayValue::Set(set) => set,
            _ => { panic!("Not a set"); }
        }
    }

    pub(crate) fn modify_set_internal(&mut self, time: LV, set_id: LV, op: &SetOp) {
        match op {
            SetOp::Insert(create) => {
                let val = self.op_to_snapshot_value(time, create);
                let old_val = self.internal_get_set(set_id).insert(time, val); // Add it to the set
                assert!(old_val.is_none(), "Item was already in set");
            }
            SetOp::Remove(target) => {
                let removed = self.internal_get_set(set_id).remove(&target); // Remove it from the set
                // We actually don't care if the item was already deleted - this can happen due to
                // concurrency.
                if let Some(SnapshotValue::InnerCRDT(id)) = removed {
                    self.overlay.remove(&id); // And from the branch.
                }
            }
        }
    }

    pub(crate) fn modify_text_local(&mut self, crdt_id: LV, text_metrics: &ListOpMetrics, ctx: &ListOperationCtx) {
        let rope = if let OverlayValue::Text(rope) = self.overlay.get_mut(&crdt_id).unwrap() {
            rope
        } else { panic!("Not a rope"); };

        match text_metrics.kind {
            ListOpKind::Ins => {
                let content = ctx.get_str(ListOpKind::Ins, text_metrics.content_pos.unwrap());
                rope.insert(text_metrics.loc.span.start, content);
            }
            ListOpKind::Del => {
                rope.remove(text_metrics.loc.into());
            }
        }
    }

    pub(crate) fn apply_local_op(&mut self, time: LV, op: &Op, ctx: &ListOperationCtx, cg: &CausalGraph) {
        debug_assert!(self.overlay_version.iter().all(|v| time > *v));

        match &op.contents {
            OpContents::RegisterSet(op_value) => {
                self.modify_reg_internal(time, op.target_id, op_value, cg);
            }
            OpContents::MapSet(key, op_value) => {
                self.modify_map_internal(time, op.target_id, key, op_value, cg);
            }
            OpContents::Set(set_op) => {
                self.modify_set_internal(time, op.target_id, set_op);
            }
            OpContents::Text(text_metrics) => {
                self.modify_text_local(op.target_id, text_metrics, ctx);
            }
        }

        self.set_time(time + op.len() - 1);
    }


    pub(crate) fn modify_map_reg_remote_internal(&mut self, cg: &CausalGraph, parents: &[LV], time: LV, crdt_id: LV, key: Option<&str>, op_value: &CreateValue) {
        // // We set locally if the new version (at time) dominates the current version of the value.
        // let should_write = if let Some(reg) = self.get_register(crdt_id, key) {
        //     // reg.last_modified
        //     debug_assert!(time > reg.version, "We should have already incorporated this change");
        //
        //     // We write if the new version dominates the old version.
        //     match cg.parents.version_cmp(time, reg.version) {
        //         Some(Ordering::Greater) => true,
        //         Some(Ordering::Less) | Some(Ordering::Equal) => false,
        //         None => {
        //             // Concurrent
        //             cg.tie_break_versions(time, reg.version) == Ordering::Greater
        //         }
        //     }
        // } else {
        //     // There's no previous value anyway. Just set it.
        //     true
        // };
        //
        // if should_write {
        if let Some(key) = key {
            self.modify_map_internal(time, crdt_id, key, op_value, cg);
        } else {
            self.modify_reg_internal(time, crdt_id, op_value, cg);
        }
        // }
    }

    pub(crate) fn apply_remote_op(&mut self, cg: &CausalGraph, parents: &[LV], time: LV, op: &Op, ctx: &ListOperationCtx) {
        match &op.contents {
            OpContents::RegisterSet(op_value) => {
                self.modify_map_reg_remote_internal(cg, parents, time, op.target_id, None, op_value);
            }
            OpContents::MapSet(key, op_value) => {
                self.modify_map_reg_remote_internal(cg, parents, time, op.target_id, Some(key.as_str()), op_value);
            }
            OpContents::Set(set_op) => {
                // Set ops have no concurrency problems anyway. An insert can only happen once, and
                // deleting an item twice is a no-op.
                self.modify_set_internal(time, op.target_id, set_op);
            }
            OpContents::Text(_) => {
                todo!()
            }
        }

        advance_frontier_by_known_run(&mut self.overlay_version, parents, time.into());
    }
}


// impl NewOpLog {
//     pub fn checkout(&self, version: &[Time]) -> Option<BTreeMap<SmartString, Box<DTValue>>> {
//         self.checkout_map(ROOT_MAP, version)
//     }
// }

#[cfg(test)]
mod test {
    use crate::{CRDTKind, OpLog};
    use smartstring::alias::String as SmartString;
    use crate::branch::separate_by;
    use crate::oplog::ROOT_MAP;

    // #[test]
    // fn checkout_inner_map() {
    //     let mut oplog = OpLog::new();
    //     // dbg!(oplog.checkout(&oplog.version));
    //
    //     let seph = oplog.get_or_create_agent_id("seph");
    //     let map_id = ROOT_MAP;
    //     // dbg!(oplog.checkout(&oplog.version));
    //
    //     let title_id = oplog.get_or_create_map_child(map_id, "title".into());
    //     oplog.append_set(seph, &oplog.version.clone(), title_id, Str("Cool title bruh".into()));
    //
    //     let author_id = oplog.get_or_create_map_child(map_id, "author".into());
    //     let author_map = oplog.append_set_new_map(seph, &oplog.version.clone(), author_id).1;
    //
    //     let email_id = oplog.get_or_create_map_child(author_map, "email".into());
    //     oplog.append_set(seph, &oplog.version.clone(), email_id, Str("me@josephg.com".into()));
    //
    //     // oplog.append_set(seph, &oplog.version.clone(), author_id, Value::);
    //
    //
    //
    //     dbg!(oplog.checkout(&oplog.version));
    //
    //
    //     // dbg!(oplog.get_value_of_register(ROOT_CRDT_ID, &oplog.version.clone()));
    //     // dbg!(&oplog);
    //     oplog.dbg_check(true);
    // }

    // #[test]
    // fn checkout_inner_map_path() {
    //     use PathComponent::*;
    //     use CRDTKind::*;
    //
    //     let mut oplog = NewOpLog::new();
    //     let seph = oplog.get_or_create_agent_id("seph");
    //
    //     oplog.set_at_path(seph, &[Key("title")], Str("Cool title bruh".into()));
    //
    //     oplog.create_map_at_path(seph, &[Key("author")]);
    //     oplog.set_at_path(seph, &[Key("author"), Key("name")], Str("Seph".into()));
    //
    //     dbg!(oplog.checkout(&oplog.version));
    //
    //     oplog.dbg_check(true);
    // }

    // #[test]
    // fn crdt_gets_overwritten() {
    //     use PathComponent::*;
    //     use CRDTKind::*;
    //
    //     let mut oplog = NewOpLog::new();
    //     let seph = oplog.get_or_create_agent_id("seph");
    //
    //     oplog.create_at_path(seph, &[], Map);
    //     oplog.create_at_path(seph, &[], Map);
    //
    //     dbg!(oplog.checkout(&oplog.version));
    //
    //     oplog.dbg_check(true);
    //     dbg!(&oplog);
    // }

    #[test]
    fn separate_by_test() {
        let mut arr = [3,1,2];
        let idx = separate_by(&mut arr, |e| *e >= 3);
        assert_eq!(idx, 2);
        assert_eq!(&arr[0..idx], &[1, 2]);
        assert_eq!(&arr[idx..arr.len()], &[3]);
    }
}






