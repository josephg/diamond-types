
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

use crate::causalgraph::agent_span::{AgentVersion, AgentSpan};
use crate::causalgraph::remote_ids::RemoteVersion;
use crate::rev_range::RangeRev;
use crate::rle::{RleKeyed, RleSpanHelpers};
use crate::unicount::count_chars;
use crate::wal::WALError;

#[derive(Debug, Eq, PartialEq, Clone)]
enum PathElement {
    Crdt(LV),
    MapValue(LV, SmartString),
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
            version: Frontier::root(),
            uncommitted_ops: ops
        })
    }

    pub fn len(&self) -> usize { self.cg.len() }
    pub fn is_empty(&self) -> bool { self.cg.is_empty() }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.cg.get_or_create_agent_id(name)
    }

    fn inner_assign_local_op_span(&mut self, agent_id: AgentId, len: usize) -> DTRange {
        let span = self.cg.assign_local_op(self.version.as_ref(), agent_id, len);
        self.version = Frontier::new_1(span.last());
        span
    }

    fn inner_assign_local_op(&mut self, agent_id: AgentId) -> LV {
        self.inner_assign_local_op_span(agent_id, 1).start
    }


    fn inner_assign_remote_op_span(&mut self, parents: &[LV], crdt_span: AgentSpan) -> DTRange {
        let time_span = self.cg.merge_and_assign_nonoverlapping(parents, crdt_span);
        self.version.advance_by_known_run(parents, time_span);
        time_span
    }

    fn inner_assign_remote_op(&mut self, parents: &[LV], id: AgentVersion) -> LV {
        self.inner_assign_remote_op_span(parents, id.into()).start
    }

    pub(crate) fn push_local_op(&mut self, agent_id: AgentId, crdt_id: LV, contents: OpContents) -> DTRange {
        let len = contents.len();
        let time_span = self.inner_assign_local_op_span(agent_id, len);
        self.uncommitted_ops.ops.push(KVPair(time_span.start, Op { target_id: crdt_id, contents}));
        time_span
    }

    /// This is different from the CausalGraph method because we need to handle the root CRDT
    /// object.
    fn try_agent_version_to_target(&self, id: AgentVersion) -> Option<LV> {
        if id == ROOT_CRDT_ID_AV { Some(ROOT_CRDT_ID) }
        else { self.cg.try_agent_version_to_lv(id) }
    }

    pub(crate) fn push_remote_op(&mut self, parents: &[LV], op_id: AgentSpan, crdt_av: AgentVersion, contents: OpContents) -> (DTRange, LV) {
        assert_eq!(op_id.len(), contents.len());
        if let OpContents::Text(_) = contents {
            panic!("Cannot push text operation using this method");
        }

        // TODO: Filter op by anything we already know.
        let time_span = self.inner_assign_remote_op_span(parents, op_id);
        let crdt_id = self.try_agent_version_to_target(crdt_av).unwrap();

        self.uncommitted_ops.ops.push(KVPair(time_span.start, Op { target_id: crdt_id, contents}));

        (time_span, crdt_id)
    }

    // *** LWW / Map operations
    pub fn local_mv_set(&mut self, agent_id: AgentId, lww_id: LV, value: CreateValue) -> LV {
        self.push_local_op(agent_id, lww_id, OpContents::RegisterSet(value))
            .start
    }

    pub fn local_map_set(&mut self, agent_id: AgentId, map_id: LV, key: &str, value: CreateValue) -> LV {
        self.push_local_op(agent_id, map_id,
                           OpContents::MapSet(key.into(), value))
            .start
    }

    pub fn local_map_delete(&mut self, agent_id: AgentId, map_id: LV, key: &str) -> LV {
        self.push_local_op(agent_id, map_id,
                           OpContents::MapDelete(key.into()))
            .start
    }

    // *** Sets ***
    pub fn insert_into_set(&mut self, agent_id: AgentId, set_id: LV, val: CreateValue) -> LV {
        self.push_local_op(agent_id, set_id, OpContents::Collection(CollectionOp::Insert(val))).start
    }

    pub fn remove_from_set(&mut self, agent_id: AgentId, set_id: LV, item: LV) -> LV {
        self.push_local_op(agent_id, set_id, OpContents::Collection(CollectionOp::Remove(item))).start
    }

    // *** Text ***
    pub(crate) fn modify_text(&mut self, agent_id: AgentId, crdt_id: LV, kind: ListOpKind, loc: RangeRev, content: Option<&str>) -> (DTRange, Op) {
        let len = loc.len();

        let content_pos = if let Some(c) = content {
            Some(self.uncommitted_ops.list_ctx.push_str(kind, c))
        } else { None };

        // let time_span = self.push_local_op(agent_id, crdt_id, OpContents::Text(
        //     ListOpMetrics { loc, kind, content_pos })
        // );

        let op = Op {
            target_id: crdt_id,
            contents: OpContents::Text(ListOpMetrics { loc, kind, content_pos })
        };

        let time_span = self.inner_assign_local_op_span(agent_id, len);
        self.uncommitted_ops.ops.push(KVPair(time_span.start, op.clone()));

        (time_span, op)
    }

    pub(crate) fn insert_into_text(&mut self, agent_id: AgentId, crdt_id: LV, pos: usize, ins_content: &str) -> (DTRange, Op) {
        let len = count_chars(ins_content);
        let pos_range = (pos..pos+len).into();
        self.modify_text(agent_id, crdt_id, ListOpKind::Ins, pos_range, Some(ins_content))
    }

    pub(crate) fn remove_from_text(&mut self, agent_id: AgentId, crdt_id: LV, range: RangeRev, content: Option<&str>) -> (DTRange, Op) {
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
    use crate::ROOT_CRDT_ID;
    use crate::{CRDTKind, OpContents, OpLog, CreateValue, Primitive, SnapshotValue};

    #[test]
    fn foo() {
        let mut oplog = OpLog::new_mem();
        let seph = oplog.get_or_create_agent_id("seph");
        let set = oplog.local_map_set(seph, ROOT_CRDT_ID, "yoo", CreateValue::NewCRDT(CRDTKind::Collection));
        let text = oplog.insert_into_set(seph, set, CreateValue::NewCRDT(CRDTKind::Text));
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