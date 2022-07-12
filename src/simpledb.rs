use crate::*;
use crate::branch::DTValue;
use crate::frontier::local_version_eq;
use crate::list::operation::TextOperation;
use crate::oplog::ROOT_MAP;
use crate::remotespan::CRDTGuid;
use crate::storage::wal::WALError;

#[derive(Debug)]
pub struct SimpleDatabase {
    oplog: OpLog,
    branch: Branch,
}

impl SimpleDatabase {
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
        self.branch.get_recursive_at(crdt_id)
    }

    pub fn get_recursive(&self) -> Option<DTValue> {
        self.get_recursive_at(ROOT_MAP)
    }


    // *** Modifying LWW Registers & Map values
    pub fn map_lww_set_primitive(&mut self, agent_id: AgentId, map_id: Time, key: &str, value: Primitive) -> Time {
        let time = self.oplog.local_set_map(agent_id, map_id, key, value.clone());

        // self.branch.inner_set_map(time, map_id, key, Value::Primitive(value));
        self.branch.inner_register_set(time, map_id, Some(key), Some(Value::Primitive(value)));
        self.branch.set_time(time); // gross.

        time
    }

    pub fn lww_set_primitive(&mut self, agent_id: AgentId, lww_id: Time, value: Primitive) -> Time {
        let time = self.oplog.local_set_lww(agent_id, lww_id, value.clone());

        // self.branch.inner_set_lww(time, lww_id, Value::Primitive(value));
        self.branch.inner_register_set(time, lww_id, None, Some(Value::Primitive(value)));
        self.branch.set_time(time);

        time
    }

    pub fn create_inner(&mut self, agent_id: AgentId, crdt_id: Time, key: Option<&str>, kind: CRDTKind) -> Time {
        let time = self.oplog.local_create_inner(agent_id, crdt_id, key, kind);
        self.branch.create_inner(time, agent_id, crdt_id, key, kind);
        self.branch.set_time(time);
        time
    }

    pub(crate) fn remote_lww_set(&mut self, parents: &[Time], id: CRDTGuid, crdt_id: CRDTGuid, key: Option<&str>, value: Primitive) -> Time {
        let (time, crdt_id) = self.oplog.remote_set_lww(parents, id, crdt_id, key, value.clone());
        self.branch.remote_register_set(&self.oplog.cg, parents, time, crdt_id, key, Some(Value::Primitive(value)));

        time
    }

    // *** Sets ***
    pub(crate) fn modify_set(&mut self, agent_id: AgentId, set_id: Time, set_op: SetOp) -> Time {
        let time = self.oplog.modify_set(agent_id, set_id, set_op);
        self.branch.modify_set(time, set_id, &set_op);
        self.branch.set_time(time);
        time
    }

    pub fn set_insert(&mut self, agent_id: AgentId, set_id: Time, kind: CRDTKind) -> Time {
        self.modify_set(agent_id, set_id, SetOp::Insert(kind))
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
        self.branch.apply_local_op(span.start, &op, &self.oplog.uncommitted_ops.list_ctx);
    }
    pub fn text_remove(&mut self, agent_id: AgentId, text_id: Time, pos: DTRange) {
        let (span, op) = self.oplog.remove_from_text(agent_id, text_id, pos.into(), None);
        self.branch.apply_local_op(span.start, &op, &self.oplog.uncommitted_ops.list_ctx);
    }
}

#[cfg(test)]
mod test {
    use crate::remotespan::CRDT_DOC_ROOT;
    use super::*;

    #[test]
    fn smoke() {
        let mut db = SimpleDatabase::open("test").unwrap();
        let seph = db.get_or_create_agent_id("seph");
        db.map_lww_set_primitive(seph, ROOT_MAP, "name", Primitive::Str("seph".into()));

        let inner = db.create_inner(seph, ROOT_MAP, Some("facts"), CRDTKind::Map);
        db.map_lww_set_primitive(seph, inner, "cool", Primitive::I64(1));

        let inner_set = db.create_inner(seph, ROOT_MAP, Some("set stuff"), CRDTKind::Set);
        let inner_map = db.set_insert(seph, inner_set, CRDTKind::Map);
        db.map_lww_set_primitive(seph, inner_map, "whoa", Primitive::I64(3214));

        dbg!(db.get_recursive());

        dbg!(&db.branch.overlay_version);
        dbg!(&db.oplog.version);

        dbg!(&db);
        db.dbg_check(true);
    }

    #[test]
    fn concurrent_writes() {
        let mut db = SimpleDatabase::open("test").unwrap();
        let seph = db.get_or_create_agent_id("seph");
        let mike = db.get_or_create_agent_id("mike");

        let key = "yooo";

        db.remote_lww_set(&[], CRDTGuid {
            agent: mike,
            seq: 0
        }, CRDT_DOC_ROOT, Some(key), Primitive::I64(321));

        db.remote_lww_set(&[], CRDTGuid {
            agent: seph,
            seq: 0
        }, CRDT_DOC_ROOT, Some(key), Primitive::I64(123));

        db.remote_lww_set(&[], CRDTGuid {
            agent: mike,
            seq: 1
        }, CRDT_DOC_ROOT, Some(key), Primitive::I64(321));

        let map = db.get_recursive_at(ROOT_MAP)
            .unwrap().unwrap_map();
        let v = map.get(key).unwrap();

        assert_eq!(v.as_ref(), &DTValue::Primitive(Primitive::I64(123)));
        // dbg!(db.get_recursive());
        // dbg!(&db);
        db.dbg_check(true);

    }

    #[test]
    fn text() {
        let mut db = SimpleDatabase::open("test").unwrap();
        let seph = db.get_or_create_agent_id("seph");

        let text = db.create_inner(seph, ROOT_MAP, Some("text"), CRDTKind::Text);
        db.text_insert(seph, text, 0, "hi there");
        db.text_remove(seph, text, (2..5).into());

        dbg!(db.get_recursive());
        db.dbg_check(true);

        dbg!(&db);
    }
}