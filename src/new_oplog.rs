
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::Path;
use smallvec::{smallvec, SmallVec};
use smartstring::alias::String as SmartString;
use rle::{AppendRle, HasLength, RleRun, Searchable};
use crate::{AgentId, CausalGraph, DTRange, KVPair, LocalVersion, LWWValue, NewOpLog, OverlayValue, Primitive, RleVec, ROOT_AGENT, ROOT_TIME, ScopedHistory, Time, Value, WriteAheadLogRaw};
use crate::frontier::{advance_frontier_by_known_run, clone_smallvec, debug_assert_frontier_sorted, frontier_is_sorted};
use crate::history::History;

use crate::remotespan::{CRDT_DOC_ROOT, CRDTGuid, CRDTSpan};
use crate::rle::{RleKeyed, RleSpanHelpers};
use crate::storage::wal::WALError;
use crate::storage::wal_encoding::{SetOp, WALValue, WriteAheadLog};

/*

Invariants:

- Client data item_times <-> client_with_localtime

- Item owned_times <-> operations
- Item owned_times <-> crdt_assignments


 */

// #[derive(Debug, Clone, PartialEq, Eq, Copy)]
// pub enum CRDTKind {
//     LWWRegister,
//     // MVRegister,
//     // Maps aren't CRDTs here because they don't receive events!
//     // Set,
//     Text,
// }


// impl Value {
//     pub fn unwrap_crdt(&self) -> CRDTItemId {
//         match self {
//             Value::InnerCRDT(scope) => *scope,
//             other => {
//                 panic!("Cannot unwrap {:?}", other);
//             }
//         }
//     }
//     pub fn scope(&self) -> Option<CRDTItemId> {
//         match self {
//             Value::InnerCRDT(scope) => Some(*scope),
//             _ => None
//         }
//     }
// }

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum CRDTKind {
    Map, LWW,
}

pub const ROOT_MAP: Time = Time::MAX;

#[derive(Debug, Eq, PartialEq, Clone)]
enum PathElement {
    CRDT(Time),
    MapValue(Time, SmartString),
}

impl NewOpLog {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WALError> {
        let mut overlay = BTreeMap::new();
        overlay.insert(ROOT_MAP, OverlayValue::Map(BTreeMap::new()));

        Ok(Self {
            // doc_id: None,
            cg: Default::default(),
            snapshot: (),
            snapshot_version: smallvec![],
            overlay,
            overlay_version: smallvec![],
            wal: WriteAheadLog::open(path)?,
        })
    }

    pub fn len(&self) -> usize { self.cg.len() }
    pub fn is_empty(&self) -> bool { self.cg.is_empty() }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.cg.get_or_create_agent_id(name)
    }

    fn get_value_of_lww(&self, lww_id: Time) -> Option<&Value> {
        self.overlay.get(&lww_id).and_then(|val| {
            match val {
                OverlayValue::LWW(val) => val.value.as_ref(),
                _ => None,
            }
        })
    }

    fn try_get_map(&self, map: Time) -> Option<&BTreeMap<SmartString, LWWValue>> {
        self.overlay.get(&map).and_then(|val| {
            match val {
                OverlayValue::Map(inner_map) => Some(inner_map),
                _ => None,
            }
        })
    }

    fn get_value_of_map(&self, map: Time, key: &str) -> Option<&Value> {
        self.try_get_map(map).and_then(|inner_map| {
            inner_map.get(key).and_then(|val| val.value.as_ref())
        })
    }

    // fn get(&self, path: &PathElement) -> Option<&Value> {
    //     match path {
    //         PathElement::CRDT(crdt_id) => self.get_value_of_lww(*crdt_id),
    //         PathElement::MapValue(crdt_id, key) => self.get_value_of_map(*crdt_id, key)
    //     }
    // }

    fn inner_assign_op_span(&mut self, span: DTRange, agent_id: AgentId) {
        self.cg.assign_next_time_to_client_known(agent_id, span);
        self.cg.history.insert(&self.overlay_version, span);

        self.overlay_version = smallvec![span.last()];
    }

    fn inner_assign_op(&mut self, time: Time, agent_id: AgentId) {
        self.inner_assign_op_span((time..time+1).into(), agent_id);
    }

    pub(crate) fn get_kind(&self, id: Time) -> CRDTKind {
        // TODO: Remove this unwrap() when we have an actual database.
        match self.overlay.get(&id).unwrap() {
            OverlayValue::LWW(_) => CRDTKind::LWW,
            OverlayValue::Map(_) => CRDTKind::Map,
        }
    }

    fn inner_set_lww(&mut self, time: Time, agent_id: AgentId, lww_id: Time, value: Value, wal_val: WALValue) {
        let inner = match self.overlay.get_mut(&lww_id).unwrap() {
            OverlayValue::LWW(lww) => lww,
            _ => { panic!("Cannot set register value in map"); }
        };
        inner.last_modified = time;
        inner.value = Some(value.clone());

        self.wal.unwritten_values.push(SetOp {
            time, crdt_id: lww_id, key: None, new_value: wal_val
        });
    }

    fn inner_set_map(&mut self, time: Time, agent_id: AgentId, map_id: Time, key: &str, value: Value, wal_val: WALValue) {
        let inner = match self.overlay.get_mut(&map_id).unwrap() {
            OverlayValue::Map(map) => map,
            _ => { panic!("Cannot set map value in LWW"); }
        };

        inner.insert(key.into(), LWWValue {
            value: Some(value.clone()),
            last_modified: time
        });

        self.wal.unwritten_values.push(SetOp {
            time, crdt_id: map_id, key: Some(key.into()), new_value: wal_val
        });
    }

    fn set_lww(&mut self, agent_id: AgentId, lww_id: Time, value: Primitive) -> Time {
        let time_now = self.cg.len();
        self.inner_assign_op(time_now, agent_id);
        self.inner_set_lww(time_now, agent_id, lww_id, Value::Primitive(value.clone()), WALValue::Primitive(value));
        time_now
    }

    fn set_map(&mut self, agent_id: AgentId, map_id: Time, key: &str, value: Primitive) -> Time {
        let time_now = self.cg.len();
        self.inner_assign_op(time_now, agent_id);
        self.inner_set_map(time_now, agent_id, map_id, key, Value::Primitive(value.clone()), WALValue::Primitive(value));
        time_now
    }

    fn inner_create_crdt(&mut self, time: Time, kind: CRDTKind) {
        let new_value = match kind {
            CRDTKind::Map => OverlayValue::Map(BTreeMap::new()),
            CRDTKind::LWW => {
                unimplemented!("This is weird");
            }
        };

        self.overlay.insert(time, new_value);
        // self.wal.unwritten_values.push((time, None, Some(Value::InnerCRDT(time))));
    }

    fn create_inner_map(&mut self, agent_id: AgentId, map_id: Time, key: &str) -> Time {
        let time_now = self.cg.len();
        let kind = CRDTKind::Map;
        self.inner_set_map(time_now, agent_id, map_id, key, Value::InnerCRDT(time_now), WALValue::NewCRDT(kind));
        self.inner_create_crdt(time_now, kind);
        self.inner_assign_op(time_now, agent_id);
        time_now
    }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::new_oplog::ROOT_MAP;
    use crate::{NewOpLog, Primitive, Value};

    #[test]
    fn smoke_test() {
        drop(std::fs::remove_file("test.wal"));
        let mut oplog = NewOpLog::open("test.wal").unwrap();
        // dbg!(&oplog);

        let seph = oplog.get_or_create_agent_id("seph");

        oplog.set_map(seph, ROOT_MAP, "name", Primitive::Str("Seph!".into()));
        let inner = oplog.create_inner_map(seph, ROOT_MAP, "deets");
        oplog.set_map(seph, inner, "cool factor", Primitive::I64(9000));

        dbg!(oplog.try_get_map(ROOT_MAP).unwrap());
        dbg!(oplog.try_get_map(inner).unwrap());
        
        dbg!(&oplog);
        oplog.dbg_check(true);
    }

    // #[test]
    // fn inner_map() {
    //     let mut oplog = NewOpLog::new();
    //
    //     let seph = oplog.get_or_create_agent_id("seph");
    //     let item = oplog.get_or_create_map_child(ROOT_MAP, "child".into());
    //     // let map_id = oplog.append_create_inner_crdt(seph, &[], item, CRDTKind::Map).1;
    //
    //     let map_id = oplog.append_set_new_map(seph, &[], item).1;
    //     let title_id = oplog.get_or_create_map_child(map_id, "title".into());
    //     oplog.append_set(seph, &oplog.version.clone(), title_id, Str("Cool title bruh".into()));
    //
    //     dbg!(oplog.checkout(&oplog.version));
    //
    //     // dbg!(oplog.get_value_of_map(1, &oplog.version.clone()));
    //     // // dbg!(oplog.get_value_of_register(ROOT_CRDT_ID, &oplog.version.clone()));
    //     // dbg!(&oplog);
    //     oplog.dbg_check(true);
    // }

    // #[test]
    // fn foo() {
    //     let mut oplog = NewOpLog::new();
    //     // dbg!(&oplog);
    //
    //     let seph = oplog.get_or_create_agent_id("seph");
    //     let mut v = 0;
    //     let root = oplog.create_root(seph, &[], RootKind::Register);
    //     // v = oplog.create_root(seph, &[v], RootKind::Register);
    //     v = oplog.version[0];
    //     v = oplog.append_set(seph, &[v], root, Value::I64(123));
    //     dbg!(&oplog);
    // }

    // #[test]
    // fn foo() {
    //     let mut oplog = NewOpLog::new();
    //     dbg!(oplog.checkout_tip());
    //
    //     let seph = oplog.get_or_create_agent_id("seph");
    //     let v1 = oplog.append_set(seph, &[], Value::I64(123));
    //     dbg!(oplog.checkout_tip());
    //
    //     let v2 = oplog.append_set(seph, &[v1], Value::I64(456));
    //     dbg!(oplog.checkout_tip());
    //
    //     let mike = oplog.get_or_create_agent_id("mike");
    //     let v3 = oplog.append_set(mike, &[v1], Value::I64(999));
    //     // dbg!(&oplog);
    //     dbg!(oplog.checkout_tip());
    // }
}