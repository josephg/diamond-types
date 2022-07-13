
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
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
use crate::unicount::count_chars;
use crate::wal::WALError;

pub const ROOT_MAP: Time = Time::MAX;

#[derive(Debug, Eq, PartialEq, Clone)]
enum PathElement {
    CRDT(Time),
    MapValue(Time, SmartString),
}

impl OpLog {
    /// Creates a new OpLog in memory only. This is useful in testing and in the browser.
    pub fn new_mem() -> Self {
        Self {
            cg: Default::default(),
            cg_storage: None,
            wal_storage: None,
            version: Default::default(),
            uncommitted_ops: Default::default()
        }
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WALError> {
        std::fs::create_dir_all(path.as_ref())?;
        let wal_path: PathBuf = [path.as_ref(), Path::new("wal")].iter().collect();
        let cg_path: PathBuf = [path.as_ref(), Path::new("cg")].iter().collect();

        let (mut cg, cgs) = CGStorage::open(cg_path).unwrap();
        let (wal, ops) = WriteAheadLog::open(path, &mut cg)?;

        Ok(Self {
            // doc_id: None,
            cg,
            cg_storage: Some(cgs),
            wal_storage: Some(wal),
            version: smallvec![], // ROOT version.
            uncommitted_ops: ops
        })
    }

    pub fn len(&self) -> usize { self.cg.len() }
    pub fn is_empty(&self) -> bool { self.cg.is_empty() }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.cg.get_or_create_agent_id(name)
    }

    fn inner_assign_local_op_span(&mut self, agent_id: AgentId, len: usize) -> DTRange {
        let span = self.cg.assign_op(&self.version, agent_id, len);
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

    pub(crate) fn push_local_op(&mut self, agent_id: AgentId, crdt_id: Time, contents: OpContents) -> DTRange {
        let len = contents.len();
        let time_span = self.inner_assign_local_op_span(agent_id, len);
        self.uncommitted_ops.ops.push(KVPair(time_span.start, Op { crdt_id, contents}));
        time_span
    }

    pub(crate) fn push_remote_op(&mut self, parents: &[Time], op_id: CRDTSpan, crdt_id: CRDTGuid, contents: OpContents) -> (DTRange, Time) {
        assert_eq!(op_id.len(), contents.len());

        // TODO: Filter op by anything we already know.
        let time_span = self.inner_assign_remote_op_span(parents, op_id);
        let crdt_id = self.cg.try_crdt_id_to_version(crdt_id).unwrap();

        self.uncommitted_ops.ops.push(KVPair(time_span.start, Op { crdt_id, contents}));

        (time_span, crdt_id)
    }

    // *** LWW / Map operations
    pub fn local_set_lww(&mut self, agent_id: AgentId, lww_id: Time, value: OpValue) -> Time {
        self.push_local_op(agent_id, lww_id, OpContents::RegisterSet(value))
            .start
    }

    pub fn local_set_map(&mut self, agent_id: AgentId, map_id: Time, key: &str, value: OpValue) -> Time {
        self.push_local_op(agent_id, map_id,
                           OpContents::MapSet(key.into(), value))
            .start
    }

    // *** Sets ***
    pub fn insert_into_set(&mut self, agent_id: AgentId, set_id: Time, kind: CRDTKind) -> Time {
        self.push_local_op(agent_id, set_id, OpContents::Set(SetOp::Insert(kind))).start
    }

    pub fn remove_from_set(&mut self, agent_id: AgentId, set_id: Time, item: Time) -> Time {
        self.push_local_op(agent_id, set_id, OpContents::Set(SetOp::Remove(item))).start
    }

    // *** Text ***
    pub(crate) fn modify_text(&mut self, agent_id: AgentId, crdt_id: Time, kind: ListOpKind, loc: RangeRev, content: Option<&str>) -> (DTRange, Op) {
        let len = loc.len();

        let content_pos = if let Some(c) = content {
            Some(self.uncommitted_ops.list_ctx.push_str(kind, c))
        } else { None };

        // let time_span = self.push_local_op(agent_id, crdt_id, OpContents::Text(
        //     ListOpMetrics { loc, kind, content_pos })
        // );

        let op = Op {
            crdt_id,
            contents: OpContents::Text(ListOpMetrics { loc, kind, content_pos })
        };

        let time_span = self.inner_assign_local_op_span(agent_id, len);
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
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::oplog::ROOT_MAP;
    use crate::{CRDTKind, OpContents, OpLog, OpValue, Primitive, SnapshotValue};

    #[test]
    fn foo() {
        let mut oplog = OpLog::new_mem();
        let seph = oplog.get_or_create_agent_id("seph");
        let set = oplog.local_set_map(seph, ROOT_MAP, "yoo", OpValue::NewCRDT(CRDTKind::Set));
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