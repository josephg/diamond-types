use crate::*;
use crate::branch::DTValue;
use crate::frontier::local_version_eq;
use crate::list::operation::TextOperation;
use crate::oplog::ROOT_MAP;
use crate::causalgraph::remotespan::CRDTGuid;
use crate::wal::WALError;

#[derive(Debug)]
pub struct SimpleDatabase {
    oplog: OpLog,
    branch: Branch,
}

impl SimpleDatabase {
    pub fn new_mem() -> Self {
        Self {
            oplog: OpLog::new_mem(),
            branch: Branch::new()
        }
    }

    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, WALError> {
        Ok(Self {
            oplog: OpLog::open(path)?,
            branch: Branch::new()
        })
    }

    pub fn dbg_check(&self, deep: bool) {
        assert!(local_version_eq(&self.oplog.version, &self.branch.overlay_version));
        self.oplog.dbg_check(deep);
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.oplog.cg.get_or_create_agent_id(name)
    }

    pub fn get_recursive_at(&self, crdt_id: Time) -> Option<DTValue> {
        self.branch.get_recursive_at(crdt_id, &self.oplog.cg)
    }

    pub fn get_recursive(&self) -> Option<DTValue> {
        self.get_recursive_at(ROOT_MAP)
    }

    pub(crate) fn apply_remote_op(&mut self, parents: &[Time], op_id: CRDTSpan, crdt_id: CRDTGuid, contents: OpContents) -> DTRange {
        let (time, crdt_id) = self.oplog.push_remote_op(parents, op_id, crdt_id, contents.clone());
        self.branch.apply_remote_op(&self.oplog.cg, parents, time.start, &Op {
            target_id: crdt_id,
            contents
        }, &self.oplog.uncommitted_ops.list_ctx);

        time
    }

    // *** Modifying LWW Registers & Map values
    pub fn modify_map(&mut self, agent_id: AgentId, map_id: Time, key: &str, value: CreateValue) -> Time {
        let time = self.oplog.local_set_map(agent_id, map_id, key, value.clone());
        self.branch.modify_map_local(time, map_id, key, &value, &self.oplog.cg);

        time
    }

    pub fn modify_lww(&mut self, agent_id: AgentId, lww_id: Time, value: CreateValue) -> Time {
        let time = self.oplog.local_set_lww(agent_id, lww_id, value.clone());
        self.branch.modify_reg_local(time, lww_id, &value, &self.oplog.cg);
        time
    }

    // *** Sets ***
    pub(crate) fn modify_set(&mut self, agent_id: AgentId, set_id: Time, set_op: SetOp) -> Time {
        // TODO: Find a way to remove this clone.
        let time = self.oplog.push_local_op(agent_id, set_id, OpContents::Set(set_op.clone())).start;
        self.branch.modify_set_internal(time, set_id, &set_op);
        self.branch.set_time(time);
        time
    }

    pub fn set_insert(&mut self, agent_id: AgentId, set_id: Time, val: CreateValue) -> Time {
        self.modify_set(agent_id, set_id, SetOp::Insert(val))
    }

    pub fn set_remove(&mut self, agent_id: AgentId, set_id: Time, target: Time) -> Time {
        self.modify_set(agent_id, set_id, SetOp::Remove(target))
    }

    // *** Text ***
    // pub(crate) fn modify_text(&mut self, agent_id: AgentId, text_id: Time, pos: usize, content: Option<&str>) {
    //
    // }

    pub fn text_insert(&mut self, agent_id: AgentId, text_id: Time, pos: usize, ins_content: &str) {
        let (span, op) = self.oplog.insert_into_text(agent_id, text_id, pos, ins_content);
        self.branch.apply_local_op(span.start, &op, &self.oplog.uncommitted_ops.list_ctx, &self.oplog.cg);
    }
    pub fn text_remove(&mut self, agent_id: AgentId, text_id: Time, pos: DTRange) {
        let (span, op) = self.oplog.remove_from_text(agent_id, text_id, pos.into(), None);
        self.branch.apply_local_op(span.start, &op, &self.oplog.uncommitted_ops.list_ctx, &self.oplog.cg);
    }
}

#[cfg(test)]
mod test {
    use crate::branch::DTValue;
    use crate::{CRDTKind, CreateValue, OpContents};
    use crate::oplog::ROOT_MAP;
    use crate::Primitive::*;
    use crate::CreateValue::*;
    use crate::causalgraph::remotespan::{CRDT_DOC_ROOT, CRDTGuid};
    use crate::simpledb::SimpleDatabase;

    #[test]
    fn smoke() {
        let mut db = SimpleDatabase::new_mem();
        let seph = db.get_or_create_agent_id("seph");
        db.modify_map(seph, ROOT_MAP, "name", Primitive(Str("seph".into())));

        let inner = db.modify_map(seph, ROOT_MAP, "facts", NewCRDT(CRDTKind::Map));
        db.modify_map(seph, inner, "cool", Primitive(I64(1)));

        let inner_set = db.modify_map(seph, ROOT_MAP, "set stuff", NewCRDT(CRDTKind::Set));
        let inner_map = db.set_insert(seph, inner_set, CreateValue::NewCRDT(CRDTKind::Map));
        db.modify_map(seph, inner_map, "whoa", Primitive(I64(3214)));

        dbg!(db.get_recursive());

        dbg!(&db.branch.overlay_version);
        dbg!(&db.oplog.version);

        dbg!(&db);
        db.dbg_check(true);
    }

    #[test]
    fn concurrent_writes() {
        let mut db = SimpleDatabase::new_mem();
        let seph = db.get_or_create_agent_id("seph");
        let mike = db.get_or_create_agent_id("mike");

        let key = "yooo";

        let t = db.apply_remote_op(&[], CRDTGuid {
            agent: mike, seq: 0
        }.into(), CRDT_DOC_ROOT, OpContents::MapSet(
            key.into(), Primitive(I64(1))
        )).end - 1;

        db.apply_remote_op(&[], CRDTGuid {
            agent: seph, seq: 0
        }.into(), CRDT_DOC_ROOT, OpContents::MapSet(
            key.into(), Primitive(I64(2))
        ));

        db.apply_remote_op(&[t], CRDTGuid {
            agent: mike, seq: 1
        }.into(), CRDT_DOC_ROOT, OpContents::MapSet(
            key.into(), Primitive(I64(3))
        ));

        let map = db.get_recursive_at(ROOT_MAP)
            .unwrap().unwrap_map();
        let v = map.get(key).unwrap();

        assert_eq!(v.as_ref(), &DTValue::Primitive(I64(2)));
        // dbg!(db.get_recursive());
        // dbg!(&db);
        db.dbg_check(true);
    }

    #[test]
    fn text() {
        let mut db = SimpleDatabase::new_mem();
        let seph = db.get_or_create_agent_id("seph");

        let text = db.modify_map(seph, ROOT_MAP, "text", NewCRDT(CRDTKind::Text));
        db.text_insert(seph, text, 0, "hi there");
        db.text_remove(seph, text, (2..5).into());

        dbg!(db.get_recursive());
        db.dbg_check(true);

        dbg!(&db);
    }
}