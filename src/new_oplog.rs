
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::Deref;
use smallvec::{smallvec, SmallVec};
use smartstring::alias::String as SmartString;
use rle::{AppendRle, HasLength, RleRun, Searchable};
use crate::{AgentId, CausalGraph, CRDTItemId, DTRange, InnerCRDTInfo, KVPair, LocalVersion, MapId, MapInfo, NewOperationCtx, NewOpLog, RleVec, ROOT_AGENT, ROOT_TIME, ScopedHistory, Time};
use crate::frontier::{advance_frontier_by_known_run, clone_smallvec, debug_assert_frontier_sorted, frontier_is_sorted};
use crate::history::History;

use crate::remotespan::{CRDT_DOC_ROOT, CRDTGuid, CRDTSpan};
use crate::rle::{RleKeyed, RleSpanHelpers};

/*

Invariants:

- Client data item_times <-> client_with_localtime

- Item owned_times <-> operations
- Item owned_times <-> crdt_assignments


 */

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum CRDTKind {
    LWWRegister,
    // MVRegister,
    // Maps aren't CRDTs here because they don't receive events!
    // Set,
    Text,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Primitive {
    I64(i64),
    Str(SmartString), // TODO: Put this in op_content
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Value {
    Primitive(Primitive),
    Map(MapId),
    InnerCRDT(CRDTItemId),
}

impl Value {
    pub fn unwrap_crdt(&self) -> CRDTItemId {
        match self {
            Value::InnerCRDT(scope) => *scope,
            other => {
                panic!("Cannot unwrap {:?}", other);
            }
        }
    }
    pub fn scope(&self) -> Option<CRDTItemId> {
        match self {
            Value::InnerCRDT(scope) => Some(*scope),
            _ => None
        }
    }
}

// pub type DocRoot = usize;

// impl ValueKind {
//     fn create_root(&self) -> Value {
//         match self {
//             ValueKind::Primitivei64 => Value::I64(0),
//         }
//     }
// }

// impl Deref for NewOpLog {
//     type Target = CausalGraph;
//
//     fn deref(&self) -> &Self::Target {
//         &self.cg
//     }
// }

pub const ROOT_MAP: CRDTItemId = 0;

impl NewOpLog {
    pub fn new() -> Self {
        Self {
            // doc_id: None,
            cg: Default::default(),
            version: smallvec![],
            // The root is always the first item in items, which is a register.
            maps: vec![MapInfo { // Map 0 is the root document.
                children: Default::default(),
                created_at: ROOT_TIME,
            }],
            known_crdts: vec![],
            register_set_operations: vec![],
        }
    }

    pub fn len(&self) -> usize { self.cg.len() }
    pub fn is_empty(&self) -> bool { self.cg.is_empty() }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.cg.get_or_create_agent_id(name)
    }

    fn get_value_of_map(&self, map: MapId, version: &[Time]) -> Option<BTreeMap<SmartString, Value>> {
        let mut result = BTreeMap::new();

        // This will only work with paths we know about!
        let info = &self.maps[map];

        // if !info.history.exists_at(&self.history, version) { return None; }

        for (key, id) in info.children.iter() {
            if let Some(value) = self.get_value_of_register(*id, version) {
                result.insert(key.clone(), value);
            }
        }

        Some(result)
    }

    pub(crate) fn get_value_of_register(&self, item_id: CRDTItemId, version: &[Time]) -> Option<Value> {
        let info = &self.known_crdts[item_id];

        // For now. Other kinds are NYI.
        assert_eq!(info.kind, CRDTKind::LWWRegister);

        if !info.history.exists_at(&self.cg.history, version) { return None; }

        let v = self.cg.history.version_in_scope(version, &info.history)?;
        // dbg!(&v);

        let content = match v.len() {
            0 => {
                // We're at ROOT. The current value is ??? what exactly?
                return None;
            },
            1 => {
                self.find_register_set_op(v[0]).clone()
            },
            _ => {
                // Disambiguate based on agent id.
                let v = self.version.iter().map(|v| {
                    let (id, offset) = self.cg.client_with_localtime.find_packed_with_offset(*v);
                    (*v, id.1.at_offset(offset))
                }).reduce(|(v1, id1), (v2, id2)| {
                    let name1 = &self.cg.client_data[id1.agent as usize].name;
                    let name2 = &self.cg.client_data[id2.agent as usize].name;
                    match name2.cmp(name1) {
                        Ordering::Less => (v1, id1),
                        Ordering::Greater => (v2, id2),
                        Ordering::Equal => {
                            match id2.seq.cmp(&id1.seq) {
                                Ordering::Less => (v1, id1),
                                Ordering::Greater => (v2, id2),
                                Ordering::Equal => panic!("Version CRDT IDs match!")
                            }
                        }
                    }
                }).unwrap().0;
                self.find_register_set_op(v).clone()
            }
        };

        Some(content)
    }

    fn find_register_set_op(&self, version: usize) -> &Value {
        let idx = self.register_set_operations.binary_search_by(|entry| {
            entry.0.cmp(&version)
        }).unwrap();

        &self.register_set_operations[idx].1
        // &self.set_operations[idx].1
    }

    pub(crate) fn advance_frontier(&mut self, parents: &[Time], span: DTRange) {
        advance_frontier_by_known_run(&mut self.version, parents, span);
    }

    fn inner_assign_op(&mut self, span: DTRange, agent_id: AgentId, parents: &[Time], crdt_id: CRDTItemId) {
        // TODO: Consider rewriting using self.cg.assign_op().
        self.cg.assign_next_time_to_client_known(agent_id, span);
        self.cg.history.insert(parents, span);

        self.known_crdts[crdt_id].history.owned_times.push(span);
        // self.crdt_assignment.push(KVPair(span.start, RleRun::new(crdt_id, span.len())));

        self.advance_frontier(parents, span);
    }

    pub(crate) fn append_set(&mut self, agent_id: AgentId, parents: &[Time], crdt_id: CRDTItemId, primitive: Primitive) -> Time {
        let v = self.len();

        // TODO: Delete old item
        self.register_set_operations.push((v, Value::Primitive(primitive)));
        self.inner_assign_op(v.into(), agent_id, parents, crdt_id);

        v
    }

    pub(crate) fn append_set_new_map(&mut self, agent_id: AgentId, parents: &[Time], crdt_id: CRDTItemId) -> (Time, MapId) {
        let v = self.len();

        // TODO: Delete old item
        let map_id = self.maps.len();
        self.maps.push(MapInfo {
            children: Default::default(),
            created_at: v
        });
        self.register_set_operations.push((v, Value::Map(map_id)));
        self.inner_assign_op(v.into(), agent_id, parents, crdt_id);

        (v, map_id)
    }

    fn inner_create_crdt(&mut self, kind: CRDTKind, ctime: usize) -> CRDTItemId {
        let crdt_id = self.known_crdts.len();

        self.known_crdts.push(InnerCRDTInfo {
            kind,
            history: ScopedHistory {
                created_at: ctime,
                deleted_at: smallvec![],
                owned_times: RleVec::new(), // Maps will never have any owned times.
            },
        });

        crdt_id
    }

    pub(crate) fn append_create_inner_crdt(&mut self, agent_id: AgentId, parents: &[Time], parent_item: CRDTItemId, kind: CRDTKind) -> (Time, CRDTItemId) {
        // this operation sets a register to contain a new (inner) CRDT with the named type.
        let info = &self.known_crdts[parent_item];
        assert_eq!(info.kind, CRDTKind::LWWRegister);

        if let Some(Value::InnerCRDT(old_scope)) = self.get_value_of_register(parent_item, parents) {
            // TODO: Mark deleted
        }

        let v = self.len();

        let new_crdt_id = self.inner_create_crdt(kind, v);
        self.register_set_operations.push((v, Value::InnerCRDT(new_crdt_id)));
        self.inner_assign_op(v.into(), agent_id, parents, parent_item);

        (v, new_crdt_id)
    }

    pub fn get_map_child(&self, map_id: MapId, field_name: &str) -> Option<CRDTItemId> {
        let map = &self.maps[map_id];
        map.children.get(field_name).copied()
    }

    // TODO: Figure out a way to pass a &str and only convert to a SmartString lazily.
    pub fn get_or_create_map_child(&mut self, map_id: MapId, field_name: SmartString) -> CRDTItemId {
        let next_crdt_id = self.known_crdts.len();
        let map = &mut self.maps[map_id];
        let ctime = map.created_at;
        // assert_eq!(info.kind, CRDTKind::Map);

        let inner_id = *map.children.entry(field_name)
            .or_insert_with(|| next_crdt_id);

        if inner_id == next_crdt_id { // Hacky.
            // A new item was created.
            self.inner_create_crdt(CRDTKind::LWWRegister, ctime);
        } else {
            assert_eq!(self.known_crdts[inner_id].kind, CRDTKind::LWWRegister);
        }

        inner_id
    }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::new_oplog::{CRDTKind, ROOT_MAP, Value};
    use crate::new_oplog::Primitive::*;
    use crate::NewOpLog;

    #[test]
    fn smoke_test() {
        let mut oplog = NewOpLog::new();
        // dbg!(&oplog);

        let seph = oplog.get_or_create_agent_id("seph");
        let mut v = 0;

        dbg!(oplog.checkout(&oplog.version));

        let item = oplog.get_or_create_map_child(ROOT_MAP, "name".into());
        oplog.append_set(seph, &[], item, I64(321));
        dbg!(oplog.checkout(&oplog.version));

        // oplog.append_set(seph, &[], ROOT_MAP, I64(123));
        //
        dbg!(oplog.get_value_of_register(item, &[]));
        dbg!(oplog.get_value_of_register(item, &oplog.version));

        dbg!(&oplog);
        oplog.dbg_check(true);
    }

    #[test]
    fn inner_map() {
        let mut oplog = NewOpLog::new();

        let seph = oplog.get_or_create_agent_id("seph");
        let item = oplog.get_or_create_map_child(ROOT_MAP, "child".into());
        // let map_id = oplog.append_create_inner_crdt(seph, &[], item, CRDTKind::Map).1;

        let map_id = oplog.append_set_new_map(seph, &[], item).1;
        let title_id = oplog.get_or_create_map_child(map_id, "title".into());
        oplog.append_set(seph, &oplog.version.clone(), title_id, Str("Cool title bruh".into()));

        dbg!(oplog.checkout(&oplog.version));

        // dbg!(oplog.get_value_of_map(1, &oplog.version.clone()));
        // // dbg!(oplog.get_value_of_register(ROOT_CRDT_ID, &oplog.version.clone()));
        // dbg!(&oplog);
        oplog.dbg_check(true);
    }

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