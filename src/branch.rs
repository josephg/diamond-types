use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use smallvec::smallvec;
use crate::*;
use smartstring::alias::String as SmartString;
use crate::frontier::{advance_frontier_by, advance_frontier_by_known_run};
use crate::oplog::ROOT_MAP;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DTValue {
    Primitive(Primitive),
    // Register(Box<DTValue>),
    Map(BTreeMap<SmartString, Box<DTValue>>),
    Set(BTreeMap<Time, Box<DTValue>>),
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
        match lww.value.as_ref()? {
            Value::Primitive(prim) => {
                Some(DTValue::Primitive(prim.clone()))
            }
            Value::InnerCRDT(inner_crdt) => {
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
        }
    }

    pub fn get_recursive(&self) -> Option<DTValue> {
        self.get_recursive_at(ROOT_MAP)
    }

    pub(super) fn get_value_of_lww(&self, lww_id: Time) -> Option<&Value> {
        self.overlay.get(&lww_id).and_then(|val| {
            match val {
                OverlayValue::LWW(val) => val.value.as_ref(),
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

    pub(super) fn get_map_value(&self, map: Time, key: &str) -> Option<&Value> {
        self.get_map(map).and_then(|inner_map| {
            inner_map.get(key).and_then(|val| val.value.as_ref())
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
        }
    }

    fn remove_old_value(&mut self, old_value: Option<Value>) {
        match old_value {
            Some(Value::InnerCRDT(crdt_id)) => {
                // This needs to recursively delete things.
                let old_value = self.overlay.remove(&crdt_id);

                // RECURSE!
                todo!()
            }
            Some(Value::Primitive(Primitive::InvalidUninitialized)) => {
                self.num_invalid -= 1;
            }
            _ => {}
        }
    }

    pub(crate) fn inner_set_lww(&mut self, time: Time, lww_id: Time, value: Option<Value>) {
        let inner = match self.overlay.get_mut(&lww_id).unwrap() {
            OverlayValue::LWW(lww) => lww,
            _ => { panic!("Cannot set register value in map"); }
        };
        inner.last_modified = time;

        let old_value = std::mem::replace(&mut inner.value, value);

        self.remove_old_value(old_value);
    }

    pub(crate) fn inner_set_map(&mut self, time: Time, map_id: Time, key: &str, value: Option<Value>) {
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

    pub(crate) fn inner_set(&mut self, time: Time, crdt_id: Time, key: Option<&str>, value: Option<Value>) {
        // let entry = self.get_register_mut(crdt_id, key).unwrap();
        // let old_val = std::mem::replace(entry, LWWValue {
        //     value,
        //     last_modified: time
        // });
        //
        // if let Some(old_value) = old_val.value {
        //     self.remove_old_value(old_value);
        // }
        if let Some(key) = key {
            self.inner_set_map(time, crdt_id, key, value);
        } else {
            self.inner_set_lww(time, crdt_id, value);
        }
    }

    pub(crate) fn remote_set(&mut self, cg: &CausalGraph, parents: &[Time], time: Time, crdt_id: Time, key: Option<&str>, value: Option<Value>) {
        // We set locally if the new version (at time) dominates the current version of the value.
        let should_write = if let Some(reg) = self.get_register(crdt_id, key) {
            // reg.last_modified
            debug_assert!(time > reg.last_modified, "We should have already incorporated this change");

            // We write if the new version dominates the old version.
            cg.history.version_contains_time(&[time], reg.last_modified)
                || cg.tie_break_versions(time, reg.last_modified) == Ordering::Greater
        } else {
            // Just set it.
            true
        };

        if should_write {
            self.inner_set(time, crdt_id, key, value);
        }

        // advance_frontier_by(&mut self.overlay_version, &cg.history, time.into());
        advance_frontier_by_known_run(&mut self.overlay_version, parents, time.into());
    }

    fn inner_create_crdt(&mut self, time: Time, kind: CRDTKind) {
        let new_value = match kind {
            CRDTKind::Map => OverlayValue::Map(BTreeMap::new()),
            CRDTKind::Set => OverlayValue::Set(BTreeSet::new()),
            CRDTKind::LWW => {
                OverlayValue::LWW(LWWValue {
                    value: None,
                    last_modified: time
                })
            }
        };

        let old_val = self.overlay.insert(time, new_value);
        assert!(old_val.is_none());
    }

    pub(crate) fn set_time(&mut self, time: Time) {
        self.overlay_version = smallvec![time];
    }

    pub(crate) fn create_inner(&mut self, time_now: Time, agent_id: AgentId, crdt_id: Time, key: Option<&str>, kind: CRDTKind) {
        self.inner_set(time_now, crdt_id, key, Some(Value::InnerCRDT(time_now)));
        self.inner_create_crdt(time_now, kind);
    }

    pub(crate) fn modify_set(&mut self, time: Time, set_id: Time, op: SetOp) {
        let inner = match self.overlay.get_mut(&set_id).unwrap() {
            OverlayValue::Set(set) => set,
            _ => { panic!("Not a set"); }
        };

        match op {
            SetOp::Insert(kind) => {
                inner.insert(time); // Add it to the set
                self.inner_create_crdt(time, kind); // And create the inner CRDT in the branch.
            }
            SetOp::Remove(target) => {
                inner.remove(&target); // Remove it from the set
                self.overlay.remove(&target); // And from the branch.
            }
        }
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






