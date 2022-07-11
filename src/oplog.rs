
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::Path;
use smallvec::{smallvec, SmallVec};
use smartstring::alias::String as SmartString;
use ::rle::*;
use crate::*;
use crate::frontier::*;
use crate::causalgraph::parents::Parents;

use crate::remotespan::{CRDT_DOC_ROOT, CRDTGuid, CRDTSpan};
use crate::rle::{RleKeyed, RleSpanHelpers};
use crate::storage::wal::WALError;

pub const ROOT_MAP: Time = Time::MAX;

#[derive(Debug, Eq, PartialEq, Clone)]
enum PathElement {
    CRDT(Time),
    MapValue(Time, SmartString),
}

impl OpLog {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WALError> {
        Ok(Self {
            // doc_id: None,
            cg: Default::default(),
            wal: WriteAheadLog::open(path)?,
            version: smallvec![], // ROOT version.
            unflushed_ops: vec![]
        })
    }

    pub fn len(&self) -> usize { self.cg.len() }
    pub fn is_empty(&self) -> bool { self.cg.is_empty() }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.cg.get_or_create_agent_id(name)
    }

    fn inner_assign_op_span(&mut self, span: DTRange, agent_id: AgentId) {
        self.cg.assign_next_time_to_client_known(agent_id, span);
        self.cg.history.insert(&self.version, span);
        self.version = smallvec![span.last()];
    }

    fn inner_assign_op(&mut self, time: Time, agent_id: AgentId) {
        self.inner_assign_op_span((time..time+1).into(), agent_id);
    }

    // fn push_lww_op(&mut self, time: Time, agent_id: AgentId, lww_id: Time, value: Value, wal_val: WALValue) {
    //     self.unflushed_ops.push(SetOp {
    //         time, crdt_id: lww_id, key: None, new_value: wal_val
    //     });
    // }
    //
    // fn push_map_op(&mut self, time: Time, agent_id: AgentId, map_id: Time, key: &str, value: Value, wal_val: WALValue) {
    //     self.unflushed_ops.push(SetOp {
    //         time, crdt_id: map_id, key: Some(key.into()), new_value: wal_val
    //     });
    // }

    fn push_op(&mut self, time: Time, agent_id: AgentId, crdt_id: Time, key: Option<&str>, value: Value, wal_val: WALValue) {
        self.unflushed_ops.push(SetOp {
            time, crdt_id, key: key.map(|k| k.into()), new_value: wal_val
        });
    }

    pub(crate) fn set_lww(&mut self, agent_id: AgentId, lww_id: Time, value: Primitive) -> Time {
        let time_now = self.cg.len();
        self.inner_assign_op(time_now, agent_id);
        self.push_op(time_now, agent_id, lww_id, None, Value::Primitive(value.clone()), WALValue::Primitive(value));
        time_now
    }

    pub(crate) fn set_map(&mut self, agent_id: AgentId, map_id: Time, key: &str, value: Primitive) -> Time {
        let time_now = self.cg.len();
        self.inner_assign_op(time_now, agent_id);
        self.push_op(time_now, agent_id, map_id, Some(key), Value::Primitive(value.clone()), WALValue::Primitive(value));
        time_now
    }

    pub(crate) fn create_inner_map(&mut self, agent_id: AgentId, crdt_id: Time, key: Option<&str>) -> Time {
        let time_now = self.cg.len();
        self.inner_assign_op(time_now, agent_id);
        let kind = CRDTKind::Map;
        self.push_op(time_now, agent_id, crdt_id, key, Value::InnerCRDT(time_now), WALValue::NewCRDT(kind));
        // self.inner_create_crdt(time_now, kind);
        time_now
    }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::oplog::ROOT_MAP;
    use crate::{OpLog, Primitive, Value};

    // #[test]
    // fn smoke_test() {
    //     drop(std::fs::remove_file("test.wal"));
    //     let mut oplog = NewOpLog::open("test.wal").unwrap();
    //     // dbg!(&oplog);
    //
    //     let seph = oplog.get_or_create_agent_id("seph");
    //
    //     oplog.set_map(seph, ROOT_MAP, "name", Primitive::Str("Seph!".into()));
    //     let inner = oplog.create_inner_map(seph, ROOT_MAP, "deets");
    //     oplog.set_map(seph, inner, "cool factor", Primitive::I64(9000));
    //
    //     dbg!(oplog.try_get_map(ROOT_MAP).unwrap());
    //     dbg!(oplog.try_get_map(inner).unwrap());
    //
    //     dbg!(&oplog);
    //     oplog.dbg_check(true);
    // }

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