use std::collections::BTreeMap;
use smallvec::smallvec;
use crate::*;
use smartstring::alias::String as SmartString;
use crate::oplog::ROOT_MAP;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DTValue {
    Primitive(Primitive),
    // Register(Box<DTValue>),
    Map(BTreeMap<SmartString, Box<DTValue>>),
    Set(BTreeMap<Time, Box<DTValue>>),
}

impl Branch {
    pub fn new() -> Self {
        let mut overlay = BTreeMap::new();
        overlay.insert(ROOT_MAP, OverlayValue::Map(BTreeMap::new()));

        Self {
            overlay,
            overlay_version: Default::default()
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

    fn get_value_of_lww(&self, lww_id: Time) -> Option<&Value> {
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

    fn get_map_value(&self, map: Time, key: &str) -> Option<&Value> {
        self.get_map(map).and_then(|inner_map| {
            inner_map.get(key).and_then(|val| val.value.as_ref())
        })
    }

    // fn get(&self, path: &PathElement) -> Option<&Value> {
    //     match path {
    //         PathElement::CRDT(crdt_id) => self.get_value_of_lww(*crdt_id),
    //         PathElement::MapValue(crdt_id, key) => self.get_value_of_map(*crdt_id, key)
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

    pub(crate) fn inner_set_lww(&mut self, time: Time, agent_id: AgentId, lww_id: Time, value: Value) {
        let inner = match self.overlay.get_mut(&lww_id).unwrap() {
            OverlayValue::LWW(lww) => lww,
            _ => { panic!("Cannot set register value in map"); }
        };
        inner.last_modified = time;
        inner.value = Some(value.clone());
    }

    pub(crate) fn inner_set_map(&mut self, time: Time, agent_id: AgentId, map_id: Time, key: &str, value: Value) {
        let inner = match self.overlay.get_mut(&map_id).unwrap() {
            OverlayValue::Map(map) => map,
            _ => { panic!("Cannot set map value in LWW"); }
        };

        inner.insert(key.into(), LWWValue {
            value: Some(value.clone()),
            last_modified: time
        });
    }

    pub(crate) fn inner_set(&mut self, time: Time, agent_id: AgentId, crdt_id: Time, key: Option<&str>, value: Value) {
        if let Some(key) = key {
            self.inner_set_map(time, agent_id, crdt_id, key, value);
        } else {
            self.inner_set_lww(time, agent_id, crdt_id, value);
        }
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
        self.inner_set(time_now, agent_id, crdt_id, key, Value::InnerCRDT(time_now));
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






