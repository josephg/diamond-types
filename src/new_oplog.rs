#![allow(unused)]

use std::cmp::Ordering;
use smallvec::{smallvec, SmallVec};
use smartstring::alias::String as SmartString;
use rle::{AppendRle, HasLength, RleRun, Searchable};
use crate::{AgentId, ClientData, DTRange, KVPair, LocalVersion, RleVec, ROOT_AGENT, ROOT_TIME, Time};
use crate::frontier::{advance_frontier_by_known_run, clone_smallvec, debug_assert_frontier_sorted, frontier_is_sorted};
use crate::history::History;
use crate::list::internal_op::OperationInternal as TextOpInternal;
use crate::remotespan::CRDTSpan;
use crate::rle::{RleKeyed, RleSpanHelpers};

#[derive(Debug, Clone)]
pub(crate) struct NewOperationCtx {
    pub(crate) set_content: Vec<u8>,

    pub(crate) ins_content: Vec<u8>,
    pub(crate) del_content: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct NewOpLog {
    /// The ID of the document (if any). This is useful if you want to give a document a GUID or
    /// something to make sure you're merging into the right place.
    ///
    /// Optional - only used if you set it.
    doc_id: Option<SmartString>,

    /// This is a bunch of ranges of (item order -> CRDT location span).
    /// The entries always have positive len.
    ///
    /// This is used to map Local time -> External CRDT locations.
    ///
    /// List is packed.
    client_with_localtime: RleVec<KVPair<CRDTSpan>>,

    /// For each client, we store some data (above). This is indexed by AgentId.
    ///
    /// This is used to map external CRDT locations -> Order numbers.
    client_data: Vec<ClientData>,

    /// Transaction metadata (succeeds, parents) for all operations on this document. This is used
    /// for `diff` and `branchContainsVersion` calls on the document, which is necessary to merge
    /// remote changes.
    ///
    /// Along with deletes, this essentially contains the time DAG.
    history: History,

    /// This is the LocalVersion for the entire oplog. So, if you merged every change we store into
    /// a branch, this is the version of that branch.
    ///
    /// This is only stored as a convenience - we could recalculate it as needed from history when
    /// needed, but thats a hassle. And it takes up very little space, and its very convenient to
    /// have on hand! So here it is.
    version: LocalVersion,

    // value_kind: ValueKind,

    /// Works like client_data. Each ItemRef is a reference into this structure.
    pub(crate) items: Vec<InnerCRDTInfo>,

    register_set_operations: Vec<(Time, Value)>,
    map_set_operations: Vec<(Time, SmartString, Value)>,
    text_operations: RleVec<KVPair<TextOpInternal>>,

    /// This contains all content ever inserted into the document, in time order (not document
    /// order). This object is indexed by the operation set.
    operation_ctx: NewOperationCtx,

    /// map from local version -> which CRDT this operation references.
    crdt_assignment: RleVec<KVPair<RleRun<usize>>>,

    // pub(crate) root_info: Vec<RootInfo>,
    // /// Map from local version -> which root contains that time.
    // root_assignment: RleVec<KVPair<RleRun<usize>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathItem {
    GoIn,
    AtKey(SmartString)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CRDTKind {
    LWWRegister,
    Map,
    Text
}


#[derive(Debug, Clone)]
pub(crate) struct InnerCRDTInfo {
    // path: SmallVec<[PathItem; 1]>,
    pub(crate) created_at: Time,
    pub(crate) kind: CRDTKind,
    pub(crate) deleted_at: LocalVersion, // Empty when the item hasn't yet been deleted.
    pub(crate) owned_times: RleVec<DTRange>,
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Value {
    I64(i64),
    InnerCRDT(usize),
}

// pub type DocRoot = usize;

// impl ValueKind {
//     fn create_root(&self) -> Value {
//         match self {
//             ValueKind::Primitivei64 => Value::I64(0),
//         }
//     }
// }

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewBranch {
    /// The version the branch is currently at. This is used to track which changes the branch has
    /// or has not locally merged.
    ///
    /// This field is public for convenience, but you should never modify it directly. Instead use
    /// the associated functions on Branch.
    version: LocalVersion,

    /// The document's content
    content: Value,
}

impl NewOpLog {
    pub fn new() -> Self {
        let mut oplog = Self {
            doc_id: None,
            client_with_localtime: RleVec::new(),
            client_data: Vec::new(),
            history: History::new(),
            version: smallvec![],
            // The root is always the first item in items, which is a register.
            items: vec![InnerCRDTInfo {
                created_at: 0,
                kind: CRDTKind::LWWRegister,
                deleted_at: smallvec![],
                owned_times: RleVec::new(),
            }],
            register_set_operations: vec![],
            map_set_operations: vec![],
            text_operations: RleVec::new(),
            operation_ctx: NewOperationCtx {
                set_content: Vec::new(),
                ins_content: vec![],
                del_content: vec![]
            },
            crdt_assignment: RleVec::new(),
        };

        // // Documents always start with a default root.
        // oplog.root_info.push(RootInfo {
        //     created_at: ROOT_TIME,
        //     owned_times: RleVec::new(),
        //     kind: RootKind::Register
        // });

        oplog
    }

    fn get_value_of(&self, crdt: usize, version: &[Time]) -> Option<Value> {
        let info = &self.items[crdt];

        // For now. Other kinds are NYI.
        assert_eq!(info.kind, CRDTKind::LWWRegister);

        // If the item has not been created yet, return None.
        if !self.history.version_contains_time(version, info.created_at) {
            // Not created yet.
            return None;
        }

        // If the item has been deleted, return None.
        for v in &info.deleted_at {
            if self.history.version_contains_time(version, *v) {
                // Deleted.
                return None;
            }
        }

        let v = self.history.version_in_crdt_item(version, info)?;
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

    // pub fn checkout_tip(&self) -> NewBranch {
    //     // So if the version only contains one entry, we can just lift that last set operation.
    //     // But if there's multiple parents, we need to pick a winner (since this is an AWW
    //     // register).
    //
    //     let content = match self.version.len() {
    //         0 => Value::I64(0), // We're at ROOT. The value is the default (0).
    //         1 => {
    //             self.find_set_op(self.version[0]).0.clone()
    //         },
    //         _ => {
    //             // Disambiguate based on agent id.
    //             let v = self.version.iter().map(|v| {
    //                 let (id, offset) = self.client_with_localtime.find_packed_with_offset(*v);
    //                 (*v, id.1.at_offset(offset))
    //             }).reduce(|(v1, id1), (v2, id2)| {
    //                 let name1 = &self.client_data[id1.agent as usize].name;
    //                 let name2 = &self.client_data[id2.agent as usize].name;
    //                 match name2.cmp(name1) {
    //                     Ordering::Less => (v1, id1),
    //                     Ordering::Greater => (v2, id2),
    //                     Ordering::Equal => {
    //                         match id2.seq.cmp(&id1.seq) {
    //                             Ordering::Less => (v1, id1),
    //                             Ordering::Greater => (v2, id2),
    //                             Ordering::Equal => panic!("Version CRDT IDs match!")
    //                         }
    //                     }
    //                 }
    //             }).unwrap().0;
    //             self.find_set_op(v).0.clone()
    //         }
    //     };
    //
    //     NewBranch {
    //         version: clone_smallvec(&self.version),
    //         content
    //     }
    // }


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

    fn inner_assign_op(&mut self, span: DTRange, agent_id: AgentId, parents: &[Time], crdt_id: usize) {
        self.assign_next_time_to_client_known(agent_id, span);

        self.history.insert(parents, span);

        self.items[crdt_id].owned_times.push(span);
        self.crdt_assignment.push(KVPair(span.start, RleRun::new(crdt_id, span.len())));

        self.advance_frontier(parents, span);
    }

    fn append_set(&mut self, agent_id: AgentId, parents: &[Time], crdt_id: usize, value: Value) -> Time {
        let v = self.len();

        self.register_set_operations.push((v, value));
        self.inner_assign_op(v.into(), agent_id, parents, crdt_id);

        v
    }

    // fn create_root(&mut self, agent_id: AgentId, parents: &[Time], kind: RootKind) -> DocRoot {
    //     let v = self.len();
    //     let root_id = self.root_info.len();
    //
    //     self.root_operations.push(KVPair(v, RootOperation::Create(kind)));
    //
    //     // let mut owned_times = RleVec::new();
    //     // owned_times.push(v.into());
    //
    //     self.root_info.push(RootInfo {
    //         created_at: v,
    //         owned_times: RleVec::new(),
    //         // owned_times,
    //         kind
    //     });
    //
    //     // Root creation operations aren't assigned to the root being created itself.
    //     self.inner_assign_op(v.into(), agent_id, parents, usize::MAX);
    //
    //     root_id
    // }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::new_oplog::{NewOpLog, Value};

    #[test]
    fn foo() {
        let mut oplog = NewOpLog::new();
        // dbg!(&oplog);

        let seph = oplog.get_or_create_agent_id("seph");
        let mut v = 0;
        oplog.append_set(seph, &[], 0, Value::I64(123));

        dbg!(oplog.get_value_of(0, &[]));
        dbg!(oplog.get_value_of(0, &oplog.version));

        dbg!(&oplog);
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