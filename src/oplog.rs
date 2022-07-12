
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
use crate::list::operation::{ListOpKind, TextOperation};

use crate::remotespan::{CRDT_DOC_ROOT, CRDTGuid, CRDTSpan};
use crate::rev_range::RangeRev;
use crate::rle::{RleKeyed, RleSpanHelpers};
use crate::storage::wal::WALError;
use crate::unicount::count_chars;

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
            uncommitted_ops: Default::default()
        })
    }

    pub fn len(&self) -> usize { self.cg.len() }
    pub fn is_empty(&self) -> bool { self.cg.is_empty() }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.cg.get_or_create_agent_id(name)
    }

    fn inner_assign_local_op_span(&mut self, agent_id: AgentId, len: usize) -> DTRange {
        let first_time = self.cg.len();
        let span = (first_time .. first_time+len).into();
        self.cg.assign_next_time_to_client_known(agent_id, span);
        self.cg.history.insert(&self.version, span);
        self.version = smallvec![span.last()];
        span
    }

    fn inner_assign_local_op(&mut self, agent_id: AgentId) -> Time {
        self.inner_assign_local_op_span(agent_id, 1).start
    }


    fn inner_assign_remote_op_span(&mut self, parents: &[Time], crdt_span: CRDTSpan) -> DTRange {
        let time_span = self.cg.assign_times_to_agent(crdt_span);
        self.cg.history.insert(parents, time_span);
        advance_frontier_by_known_run(&mut self.version, parents, time_span);
        time_span
    }

    fn inner_assign_remote_op(&mut self, parents: &[Time], id: CRDTGuid) -> Time {
        self.inner_assign_remote_op_span(parents, id.into()).start
    }

    // *** LWW / Map operations
    fn push_register_op(&mut self, time: Time, crdt_id: Time, key: Option<&str>, wal_val: WALValue) {
        self.uncommitted_ops.ops.push(KVPair(time, Op {
            crdt_id,
            contents: OpContents::Register(RegisterOp {
                key: key.map(|k| k.into()), new_value: wal_val
            })
        }));
    }

    pub fn local_set_lww(&mut self, agent_id: AgentId, lww_id: Time, value: Primitive) -> Time {
        let time_now = self.inner_assign_local_op(agent_id);
        self.push_register_op(time_now, lww_id, None, WALValue::Primitive(value));
        time_now
    }

    pub fn local_set_map(&mut self, agent_id: AgentId, map_id: Time, key: &str, value: Primitive) -> Time {
        let time_now = self.inner_assign_local_op(agent_id);
        self.push_register_op(time_now, map_id, Some(key), WALValue::Primitive(value));
        time_now
    }

    pub fn local_create_inner(&mut self, agent_id: AgentId, crdt_id: Time, key: Option<&str>, kind: CRDTKind) -> Time {
        let time_now = self.inner_assign_local_op(agent_id);
        self.push_register_op(time_now, crdt_id, key, WALValue::NewCRDT(kind));
        // self.inner_create_crdt(time_now, kind);
        time_now
    }

    pub(crate) fn remote_set_lww(&mut self, parents: &[Time], op_id: CRDTGuid, crdt_id: CRDTGuid, key: Option<&str>, value: Primitive) -> (Time, Time) {
        let time = self.inner_assign_remote_op(parents, op_id);
        let c = self.cg.try_crdt_id_to_version(crdt_id).unwrap();
        self.push_register_op(time, c, key, WALValue::Primitive(value));

        (time, c)
    }


    // *** Sets ***
    pub(crate) fn modify_set(&mut self, agent_id: AgentId, crdt_id: Time, op: SetOp) -> Time {
        let time = self.inner_assign_local_op(agent_id);
        self.uncommitted_ops.ops.push(KVPair(time, Op {
            crdt_id,
            contents: OpContents::Set(op)
        }));
        time
    }

    pub fn insert_into_set(&mut self, agent_id: AgentId, set_id: Time, kind: CRDTKind) -> Time {
        self.modify_set(agent_id, set_id, SetOp::Insert(kind))
    }

    pub fn remove_from_set(&mut self, agent_id: AgentId, set_id: Time, item: Time) -> Time {
        self.modify_set(agent_id, set_id, SetOp::Remove(item))
    }

    // *** Text ***
    pub(crate) fn modify_text(&mut self, agent_id: AgentId, crdt_id: Time, kind: ListOpKind, loc: RangeRev, content: Option<&str>) -> (DTRange, Op) {
        let len = loc.len();
        let time_span = self.inner_assign_local_op_span(agent_id, len);

        let content_pos = if let Some(c) = content {
            Some(self.uncommitted_ops.list_ctx.push_str(kind, c))
        } else { None };

        let op = Op {
            crdt_id,
            contents: OpContents::Text(ListOpMetrics { loc, kind, content_pos })
        };
        self.uncommitted_ops.ops.push(KVPair(time_span.start, op.clone()));

        (time_span, op)
    }

    pub(crate) fn insert_into_text(&mut self, agent_id: AgentId, crdt_id: Time, pos: usize, ins_content: &str) -> (DTRange, Op) {
        let len = count_chars(ins_content);
        let pos_range = (pos..pos+len).into();
        self.modify_text(agent_id, crdt_id, ListOpKind::Ins, pos_range, Some(ins_content))
    }

    pub(crate) fn remove_from_text(&mut self, agent_id: AgentId, crdt_id: Time, range: RangeRev, content: Option<&str>) -> (DTRange, Op) {
        if let Some(content) = content {
            // The content must have the correct number of characters.
            let len = count_chars(content);
            assert_eq!(len, range.len());
        }

        self.modify_text(agent_id, crdt_id, ListOpKind::Del, range, content)
    }


    //     let len = count_chars(ins_content);
    //     let time = self.inner_assign_local_op_span(agent_id, len);
    //
    //     let content_pos = self.uncommitted_ops.list_ctx.push_str(ListOpKind::Ins, ins_content);
    //     self.uncommitted_ops.ops.push(KVPair(time.start, Op {
    //         crdt_id,
    //         contents: OpContents::Text(ListOpMetrics {
    //             loc: (pos..pos+len).into(),
    //             kind: ListOpKind::Ins,
    //             content_pos: Some(content_pos)
    //         })
    //     }));
    //
    //     time
    // }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::oplog::ROOT_MAP;
    use crate::{CRDTKind, OpLog, Primitive, Value};

    #[test]
    fn foo() {
        let mut oplog = OpLog::open("test").unwrap();
        let seph = oplog.get_or_create_agent_id("seph");
        let set = oplog.local_create_inner(seph, ROOT_MAP, Some("yoo"), CRDTKind::Set);
        let text = oplog.insert_into_set(seph, set, CRDTKind::Text);
        oplog.insert_into_text(seph, text, 0, "hi there");
        oplog.dbg_check(true);
        dbg!(&oplog);
    }

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