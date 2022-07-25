use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use smallvec::smallvec;
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
    Set(BTreeMap<Time, Box<DTValue>>),
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

    pub fn unwrap_set(self) -> BTreeMap<Time, Box<DTValue>> {
        match self {
            DTValue::Set(set) => set,
            _ => { panic!("Expected set") }
        }
    }
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

    fn get_lww(&self, lww: &LWWValue) -> Option<DTValue> {
        match &lww.value {
            SnapshotValue::Primitive(prim) => {
                Some(DTValue::Primitive(prim.clone()))
            }
            SnapshotValue::InnerCRDT(inner_crdt) => {
                self.get_recursive_at(*inner_crdt)
            }
        }
    }

    pub fn get_recursive_at(&self, crdt_id: Time) -> Option<DTValue> {
        match self.overlay.get(&crdt_id)? {
            OverlayValue::LWW(lww) => self.get_lww(lww),
            OverlayValue::Map(map) => {
                Some(DTValue::Map(map.iter().filter_map(|(key, lww)| {
                    Some((key.clone(), Box::new(self.get_lww(lww)?)))
                }).collect()))
            }
            OverlayValue::Set(id_set) => {
                Some(DTValue::Set(id_set.iter().filter_map(|time| {
                    Some((*time, Box::new(self.get_recursive_at(*time)?)))
                }).collect()))
            }
            OverlayValue::Text(rope) => {
                Some(DTValue::Text(rope.to_string()))
            }
        }
    }

    pub fn get_recursive(&self) -> Option<DTValue> {
        self.get_recursive_at(ROOT_MAP)
    }

    pub(super) fn get_value_of_lww(&self, lww_id: Time) -> Option<&SnapshotValue> {
        self.overlay.get(&lww_id).and_then(|val| {
            match val {
                OverlayValue::LWW(val) => Some(&val.value),
                _ => None,
            }
        })
    }

    fn get_map(&self, map: Time) -> Option<&BTreeMap<SmartString, LWWValue>> {
        self.overlay.get(&map).and_then(|val| {
            match val {
                OverlayValue::Map(inner_map) => Some(inner_map),
                _ => None,
            }
        })
    }

    pub(super) fn get_map_value(&self, map: Time, key: &str) -> Option<&SnapshotValue> {
        self.get_map(map).and_then(|inner_map| {
            inner_map.get(key).map(|val| &val.value)
        })
    }

    fn get_register(&self, crdt_id: Time, key: Option<&str>) -> Option<&LWWValue> {
        if let Some(key) = key {
            match self.overlay.get(&crdt_id)? {
                OverlayValue::Map(inner_map) => {
                    Some(inner_map.get(key)?)
                },
                _ => None,
            }
        } else {
            match self.overlay.get(&crdt_id)? {
                OverlayValue::LWW(val) => Some(val),
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

    pub(crate) fn get_kind(&self, id: Time) -> CRDTKind {
        // TODO: Remove this unwrap() when we have an actual database.
        match self.overlay.get(&id).unwrap() {
            OverlayValue::LWW(_) => CRDTKind::LWW,
            OverlayValue::Map(_) => CRDTKind::Map,
            OverlayValue::Set(_) => CRDTKind::Set,
            OverlayValue::Text(_) => CRDTKind::Text,
        }
    }

    // *** Mutation operations ***

    fn remove_old_value(&mut self, old_value: SnapshotValue) {
        match old_value {
            SnapshotValue::Primitive(Primitive::InvalidUninitialized) => {
                self.num_invalid -= 1;
            }
            SnapshotValue::InnerCRDT(crdt_id) => {
                // This needs to recursively delete things.
                let old_value = self.overlay.remove(&crdt_id);

                // RECURSE!
                todo!()
            }
            _ => {}
        }
    }


    fn inner_create_crdt(&mut self, time: Time, kind: CRDTKind) {
        let new_value = match kind {
            CRDTKind::Map => OverlayValue::Map(BTreeMap::new()),
            CRDTKind::Set => OverlayValue::Set(BTreeSet::new()),
            CRDTKind::LWW => {
                self.num_invalid += 1;
                OverlayValue::LWW(LWWValue {
                    value: SnapshotValue::Primitive(Primitive::InvalidUninitialized),
                    last_modified: time
                })
            }
            CRDTKind::Text => {
                OverlayValue::Text(JumpRope::new())
            }
        };

        let old_val = self.overlay.insert(time, new_value);
        assert!(old_val.is_none());
    }

    fn op_to_snapshot_value(&mut self, time: Time, value: &OpValue) -> SnapshotValue {
        match value {
            OpValue::Primitive(p) => SnapshotValue::Primitive(p.clone()),
            OpValue::NewCRDT(kind) => {
                self.inner_create_crdt(time, *kind);
                SnapshotValue::InnerCRDT(time)
            }
            // OpValue::Deleted => None,
        }
    }

    pub(crate) fn set_time(&mut self, time: Time) {
        self.overlay_version = smallvec![time];
    }


    fn modify_lww_internal(&mut self, time: Time, lww_id: Time, op_value: &OpValue) {
        let value = self.op_to_snapshot_value(time, op_value);

        let inner = match self.overlay.get_mut(&lww_id).unwrap() {
            OverlayValue::LWW(lww) => lww,
            _ => { panic!("Cannot set register value in map"); }
        };
        inner.last_modified = time;

        let old_value = std::mem::replace(&mut inner.value, value);

        self.remove_old_value(old_value);
    }

    pub fn modify_lww_local(&mut self, time: Time, lww_id: Time, op_value: &OpValue) {
        self.modify_lww_internal(time, lww_id, op_value);
        self.set_time(time);
    }

    fn modify_map_internal(&mut self, time: Time, map_id: Time, key: &str, op_value: &OpValue) {
        let value = self.op_to_snapshot_value(time, op_value);

        let inner = match self.overlay.get_mut(&map_id).unwrap() {
            OverlayValue::Map(map) => map,
            _ => { panic!("Cannot set map value in LWW"); }
        };

        let prev = inner.insert(key.into(), LWWValue {
            value,
            last_modified: time
        });

        if let Some(val) = prev {
            self.remove_old_value(val.value);
        }
    }

    pub fn modify_map_local(&mut self, time: Time, lww_id: Time, key: &str, op_value: &OpValue) {
        self.modify_map_internal(time, lww_id, key, op_value);
        self.set_time(time);
    }

    // pub(crate) fn create_inner(&mut self, time_now: Time, agent_id: AgentId, crdt_id: Time, key: Option<&str>, kind: CRDTKind) {
    //     self.inner_register_set(time_now, crdt_id, key, SnapshotValue::InnerCRDT(time_now));
    //     self.inner_create_crdt(time_now, kind);
    // }

    pub(crate) fn modify_set_internal(&mut self, time: Time, set_id: Time, op: &SetOp) {
        let inner = match self.overlay.get_mut(&set_id).unwrap() {
            OverlayValue::Set(set) => set,
            _ => { panic!("Not a set"); }
        };

        match op {
            SetOp::Insert(kind) => {
                let inserted = inner.insert(time); // Add it to the set
                assert!(inserted, "Item was already in set");
                self.inner_create_crdt(time, *kind); // And create the inner CRDT in the branch.
            }
            SetOp::Remove(target) => {
                inner.remove(&target); // Remove it from the set
                // We actually don't care if the item was already deleted - this can happen due to
                // concurrency.
                self.overlay.remove(&target); // And from the branch.
            }
        }
    }

    pub(crate) fn modify_text_local(&mut self, crdt_id: Time, text_metrics: &ListOpMetrics, ctx: &ListOperationCtx) {
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

    pub(crate) fn apply_local_op(&mut self, time: Time, op: &Op, ctx: &ListOperationCtx) {
        debug_assert!(self.overlay_version.iter().all(|v| time > *v));

        match &op.contents {
            OpContents::RegisterSet(op_value) => {
                self.modify_lww_internal(time, op.crdt_id, op_value);
            }
            OpContents::MapSet(key, op_value) => {
                self.modify_map_internal(time, op.crdt_id, key, op_value);
            }
            OpContents::Set(set_op) => {
                self.modify_set_internal(time, op.crdt_id, set_op);
            }
            OpContents::Text(text_metrics) => {
                self.modify_text_local(op.crdt_id, text_metrics, ctx);
            }
        }

        self.set_time(time + op.len() - 1);
    }


    pub(crate) fn modify_map_lww_remote_internal(&mut self, cg: &CausalGraph, parents: &[Time], time: Time, crdt_id: Time, key: Option<&str>, op_value: &OpValue) {
        // We set locally if the new version (at time) dominates the current version of the value.
        let should_write = if let Some(reg) = self.get_register(crdt_id, key) {
            // reg.last_modified
            debug_assert!(time > reg.last_modified, "We should have already incorporated this change");

            // We write if the new version dominates the old version.
            match cg.parents.version_cmp(time, reg.last_modified) {
                Some(Ordering::Greater) => true,
                Some(Ordering::Less) | Some(Ordering::Equal) => false,
                None => {
                    // Concurrent
                    cg.tie_break_versions(time, reg.last_modified) == Ordering::Greater
                }
            }
        } else {
            // There's no previous value anyway. Just set it.
            true
        };

        if should_write {
            if let Some(key) = key {
                self.modify_map_internal(time, crdt_id, key, op_value);
            } else {
                self.modify_lww_internal(time, crdt_id, op_value);
            }
        }
    }

    pub(crate) fn apply_remote_op(&mut self, cg: &CausalGraph, parents: &[Time], time: Time, op: &Op, ctx: &ListOperationCtx) {
        match &op.contents {
            OpContents::RegisterSet(op_value) => {
                self.modify_map_lww_remote_internal(cg, parents, time, op.crdt_id, None, op_value);
            }
            OpContents::MapSet(key, op_value) => {
                self.modify_map_lww_remote_internal(cg, parents, time, op.crdt_id, Some(key.as_str()), op_value);
            }
            OpContents::Set(set_op) => {
                // Set ops have no concurrency problems anyway. An insert can only happen once, and
                // deleting an item twice is a no-op.
                self.modify_set_internal(time, op.crdt_id, set_op);
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

    // #[test]
    // fn checkout_inner_map() {
    //     let mut oplog = NewOpLog::new();
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
}






