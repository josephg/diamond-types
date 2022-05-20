
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use smallvec::{smallvec, SmallVec};
use smartstring::alias::String as SmartString;
use rle::{AppendRle, HasLength, RleRun, Searchable};
use crate::{AgentId, ClientData, CRDTId, DTRange, InnerCRDTInfo, KVPair, LocalVersion, NewOperationCtx, NewOpLog, RleVec, ROOT_AGENT, ROOT_TIME, ScopedHistory, Time};
use crate::frontier::{advance_frontier_by_known_run, clone_smallvec, debug_assert_frontier_sorted, frontier_is_sorted};
use crate::history::History;

use crate::remotespan::CRDTSpan;
use crate::rle::{RleKeyed, RleSpanHelpers};

/*

Invariants:

- Client data item_times <-> client_with_localtime

- Item owned_times <-> operations
- Item owned_times <-> crdt_assignments


 */

// #[derive(Debug, Clone, PartialEq, Eq)]
// pub(crate) enum PathItem {
//     GoIn,
//     AtKey(SmartString)
// }

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum CRDTKind {
    LWWRegister,
    // MVRegister,
    Map,
    Text,
}








// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct SetOperation(Value);

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum ValueKind {
//     Primitivei64,
//     // DynamicAny,
// }
//
// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum CRDTContainer {
//     AWWRegister(ValueKind),
//     // Text,
// }

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Primitive {
    I64(i64),
    String(SmartString), // TODO: Put this in op_content
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Value {
    Primitive(Primitive),
    InnerCRDT(CRDTId),
}

// pub type DocRoot = usize;

// impl ValueKind {
//     fn create_root(&self) -> Value {
//         match self {
//             ValueKind::Primitivei64 => Value::I64(0),
//         }
//     }
// }


pub const ROOT_CRDT_ID: CRDTId = 0;

impl NewOpLog {
    pub fn new() -> Self {
        let mut oplog = Self {
            // doc_id: None,
            client_with_localtime: RleVec::new(),
            client_data: Vec::new(),
            history: History::new(),
            version: smallvec![],
            // The root is always the first item in items, which is a register.
            items: vec![InnerCRDTInfo {
                kind: CRDTKind::LWWRegister,
                map_children: None,
                history: ScopedHistory {
                    created_at: ROOT_TIME,
                    deleted_at: smallvec![],
                    owned_times: RleVec::new(),
                }
            }],
            register_set_operations: vec![],
            // map_set_operations: vec![],
            // text_operations: RleVec::new(),
            // operation_ctx: NewOperationCtx {
            //     set_content: Vec::new(),
            //     ins_content: vec![],
            //     del_content: vec![]
            // },
            // crdt_assignment: RleVec::new(),
        };

        // // Documents always start with a default root.
        // oplog.root_info.push(RootInfo {
        //     created_at: ROOT_TIME,
        //     owned_times: RleVec::new(),
        //     kind: RootKind::Register
        // });

        oplog
    }

    fn get_value_of_map(&self, crdt: CRDTId, version: &[Time]) -> Option<BTreeMap<SmartString, Value>> {
        let mut result = BTreeMap::new();

        let info = &self.items[crdt];
        assert_eq!(info.kind, CRDTKind::Map);

        if !info.history.exists_at(&self.history, version) { return None; }

        for (key, id) in info.map_children.as_ref().unwrap() {
            if let Some(value) = self.get_value_of_register(*id, version) {
                result.insert(key.clone(), value);
            }
        }

        Some(result)
    }

    fn get_value_of_register(&self, crdt: CRDTId, version: &[Time]) -> Option<Value> {
        let info = &self.items[crdt];

        // For now. Other kinds are NYI.
        assert_eq!(info.kind, CRDTKind::LWWRegister);

        if !info.history.exists_at(&self.history, version) { return None; }

        let v = self.history.version_in_scope(version, &info.history)?;
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
                    let (id, offset) = self.client_with_localtime.find_packed_with_offset(*v);
                    (*v, id.1.at_offset(offset))
                }).reduce(|(v1, id1), (v2, id2)| {
                    let name1 = &self.client_data[id1.agent as usize].name;
                    let name2 = &self.client_data[id2.agent as usize].name;
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

    pub(crate) fn get_agent_id(&self, name: &str) -> Option<AgentId> {
        if name == "ROOT" { Some(ROOT_AGENT) }
        else {
            self.client_data.iter()
                .position(|client_data| client_data.name == name)
                .map(|id| id as AgentId)
        }
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        if let Some(id) = self.get_agent_id(name) {
            id
        } else {
            // Create a new id.
            self.client_data.push(ClientData {
                name: SmartString::from(name),
                item_times: RleVec::new()
            });
            (self.client_data.len() - 1) as AgentId
        }
    }

    fn find_register_set_op(&self, version: usize) -> &Value {
        let idx = self.register_set_operations.binary_search_by(|entry| {
            entry.0.cmp(&version)
        }).unwrap();

        &self.register_set_operations[idx].1
        // &self.set_operations[idx].1
    }


    /// Get the number of operations
    pub fn len(&self) -> usize {
        if let Some(last) = self.client_with_localtime.last() {
            last.end()
        } else { 0 }
    }

    /// span is the local timespan we're assigning to the named agent.
    pub(crate) fn assign_next_time_to_client_known(&mut self, agent: AgentId, span: DTRange) {
        debug_assert_eq!(span.start, self.len());

        let client_data = &mut self.client_data[agent as usize];

        let next_seq = client_data.get_next_seq();
        client_data.item_times.push(KVPair(next_seq, span));

        self.client_with_localtime.push(KVPair(span.start, CRDTSpan {
            agent,
            seq_range: DTRange { start: next_seq, end: next_seq + span.len() },
        }));
    }

    pub(crate) fn advance_frontier(&mut self, parents: &[Time], span: DTRange) {
        advance_frontier_by_known_run(&mut self.version, parents, span);
    }

    fn inner_assign_op(&mut self, span: DTRange, agent_id: AgentId, parents: &[Time], crdt_id: CRDTId) {
        self.assign_next_time_to_client_known(agent_id, span);

        self.history.insert(parents, span);

        self.items[crdt_id].history.owned_times.push(span);
        // self.crdt_assignment.push(KVPair(span.start, RleRun::new(crdt_id, span.len())));

        self.advance_frontier(parents, span);
    }

    pub(crate) fn append_set(&mut self, agent_id: AgentId, parents: &[Time], crdt_id: CRDTId, value: Value) -> Time {
        let v = self.len();

        self.register_set_operations.push((v, value));
        self.inner_assign_op(v.into(), agent_id, parents, crdt_id);

        v
    }

    fn inner_create_owned_crdt(&mut self, kind: CRDTKind, ctime: usize) -> CRDTId {
        let crdt_id = self.items.len();

        self.items.push(InnerCRDTInfo {
            kind,
            history: ScopedHistory {
                created_at: ctime,
                deleted_at: smallvec![],
                owned_times: RleVec::new(),
            },
            map_children: if kind == CRDTKind::Map {
                Some(BTreeMap::new())
            } else { None },
        });

        crdt_id
    }

    pub(crate) fn append_create_inner_crdt(&mut self, agent_id: AgentId, parents: &[Time], parent_crdt_id: CRDTId, kind: CRDTKind) -> (Time, CRDTId) {
        // this operation sets a register to contain a new (inner) CRDT with the named type.
        let v = self.len();

        let new_crdt_id = self.inner_create_owned_crdt(kind, v);
        self.register_set_operations.push((v, Value::InnerCRDT(new_crdt_id)));
        self.inner_assign_op(v.into(), agent_id, parents, parent_crdt_id);

        (v, new_crdt_id)
    }

    pub fn get_or_create_map_child(&mut self, map_id: CRDTId, field_name: SmartString) -> CRDTId {
        let next_crdt_id = self.items.len();
        let info = &mut self.items[map_id];
        let ctime = info.history.created_at;
        // assert_eq!(info.kind, CRDTKind::Map);

        let inner_map = info.map_children.as_mut().unwrap();
        let inner_id = *inner_map.entry(field_name)
            .or_insert_with(|| next_crdt_id);

        if inner_id == next_crdt_id { // Hacky.
            // A new item was created.
            self.inner_create_owned_crdt(CRDTKind::LWWRegister, ctime);
        } else {
            assert_eq!(self.items[inner_id].kind, CRDTKind::LWWRegister);
        }

        inner_id
    }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::new_oplog::{CRDTKind, ROOT_CRDT_ID, Value};
    use crate::new_oplog::Primitive::*;
    use crate::NewOpLog;

    #[test]
    fn foo() {
        let mut oplog = NewOpLog::new();
        // dbg!(&oplog);

        let seph = oplog.get_or_create_agent_id("seph");
        let mut v = 0;
        oplog.append_set(seph, &[], ROOT_CRDT_ID, Value::Primitive(I64(123)));

        dbg!(oplog.get_value_of_register(0, &[]));
        dbg!(oplog.get_value_of_register(0, &oplog.version));

        dbg!(&oplog);
        oplog.dbg_check(true);
    }

    #[test]
    fn inner_map() {
        let mut oplog = NewOpLog::new();

        let seph = oplog.get_or_create_agent_id("seph");
        let map_id = oplog.append_create_inner_crdt(seph, &[], ROOT_CRDT_ID, CRDTKind::Map).1;

        let title_id = oplog.get_or_create_map_child(map_id, "title".into());
        oplog.append_set(seph, &oplog.version.clone(), title_id, Value::Primitive(String("Cool title bruh".into())));

        dbg!(oplog.get_value_of_map(1, &oplog.version.clone()));
        // dbg!(oplog.get_value_of_register(ROOT_CRDT_ID, &oplog.version.clone()));
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