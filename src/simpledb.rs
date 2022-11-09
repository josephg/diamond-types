use crate::*;
use crate::branch::DTValue;
use crate::frontier::local_frontier_eq;
use crate::list::operation::TextOperation;
use crate::ROOT_CRDT_ID;
use crate::causalgraph::agent_span::{AgentVersion, AgentSpan};
use crate::wal::WALError;

#[derive(Debug)]
pub struct SimpleDatabase {
    pub(crate) oplog: OpLog,
    pub(crate) branch: Branch,
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
        assert!(local_frontier_eq(&self.oplog.version, &self.branch.version));
        self.oplog.dbg_check(deep);
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.oplog.cg.get_or_create_agent_id(name)
    }

    pub fn get_recursive_at(&self, crdt_id: LV) -> Option<DTValue> {
        self.branch.get_recursive_at(crdt_id, &self.oplog.cg)
    }

    pub fn get_recursive(&self) -> Option<DTValue> {
        self.get_recursive_at(ROOT_CRDT_ID)
    }

    pub(crate) fn apply_remote_op(&mut self, parents: &[LV], op_id: AgentSpan, crdt_id: AgentVersion, contents: OpContents) -> DTRange {
        let (time, crdt_id) = self.oplog.push_remote_op(parents, op_id, crdt_id, contents.clone());
        self.branch.apply_remote_op(&self.oplog.cg, parents, time.start, &Op {
            target_id: crdt_id,
            contents
        }, &self.oplog.uncommitted_ops.list_ctx);

        time
    }

    // *** Modifying LWW Registers & Map values
    pub fn map_set(&mut self, agent_id: AgentId, map_id: LV, key: &str, value: CreateValue) -> LV {
        // TODO: Remove this .clone().
        let time = self.oplog.local_map_set(agent_id, map_id, key, value.clone());
        self.branch.modify_map_local(time, map_id, key, Some(&value), &self.oplog.cg);

        time
    }

    pub fn map_delete(&mut self, agent_id: AgentId, map_id: LV, key: &str) -> LV {
        let time = self.oplog.local_map_delete(agent_id, map_id, key);
        self.branch.modify_map_local(time, map_id, key, None, &self.oplog.cg);
        time
    }

    pub fn mv_set(&mut self, agent_id: AgentId, mv_id: LV, value: CreateValue) -> LV {
        let time = self.oplog.local_mv_set(agent_id, mv_id, value.clone());
        self.branch.modify_reg_local(time, mv_id, &value, &self.oplog.cg);
        time
    }

    // *** Sets ***
    pub(crate) fn modify_collection(&mut self, agent_id: AgentId, set_id: LV, c_op: CollectionOp) -> LV {
        // TODO: Find a way to remove this clone.
        let time = self.oplog.push_local_op(agent_id, set_id, OpContents::Collection(c_op.clone())).start;
        self.branch.modify_set_internal(time, set_id, &c_op);
        self.branch.set_time(time);
        time
    }

    pub fn collection_insert(&mut self, agent_id: AgentId, set_id: LV, val: CreateValue) -> LV {
        self.modify_collection(agent_id, set_id, CollectionOp::Insert(val))
    }

    pub fn collection_remove(&mut self, agent_id: AgentId, set_id: LV, target: LV) -> LV {
        self.modify_collection(agent_id, set_id, CollectionOp::Remove(target))
    }

    // *** Text ***
    // pub(crate) fn modify_text(&mut self, agent_id: AgentId, text_id: Time, pos: usize, content: Option<&str>) {
    //
    // }

    pub fn text_insert(&mut self, agent_id: AgentId, text_id: LV, pos: usize, ins_content: &str) {
        let (span, op) = self.oplog.insert_into_text(agent_id, text_id, pos, ins_content);
        self.branch.apply_local_op(span.start, &op, &self.oplog.uncommitted_ops.list_ctx, &self.oplog.cg);
    }
    pub fn text_remove(&mut self, agent_id: AgentId, text_id: LV, pos: DTRange) {
        let (span, op) = self.oplog.remove_from_text(agent_id, text_id, pos.into(), None);
        self.branch.apply_local_op(span.start, &op, &self.oplog.uncommitted_ops.list_ctx, &self.oplog.cg);
    }
}

#[cfg(test)]
mod test {
    use crate::branch::DTValue;
    use crate::{CRDTKind, CreateValue, OpContents, ROOT_CRDT_ID_AV};
    use crate::ROOT_CRDT_ID;
    use crate::Primitive::*;
    use crate::CreateValue::*;
    use crate::causalgraph::agent_span::AgentVersion;
    use crate::simpledb::SimpleDatabase;

    #[test]
    fn smoke() {
        let mut db = SimpleDatabase::new_mem();
        let seph = db.get_or_create_agent_id("seph");
        db.map_set(seph, ROOT_CRDT_ID, "name", Primitive(Str("seph".into())));

        let inner = db.map_set(seph, ROOT_CRDT_ID, "facts", NewCRDT(CRDTKind::Map));
        db.map_set(seph, inner, "cool", Primitive(I64(1)));

        let inner_set = db.map_set(seph, ROOT_CRDT_ID, "set stuff", NewCRDT(CRDTKind::Collection));
        let inner_map = db.collection_insert(seph, inner_set, CreateValue::NewCRDT(CRDTKind::Map));
        db.map_set(seph, inner_map, "whoa", Primitive(I64(3214)));

        // db.map_delete(seph, inner, "cool");

        dbg!(db.get_recursive());

        dbg!(&db.branch.version);
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

        let t = db.apply_remote_op(&[],
                                   (mike, 0).into(), ROOT_CRDT_ID_AV, OpContents::MapSet(
            key.into(), Primitive(I64(1))
           )
        ).end - 1;

        db.apply_remote_op(&[],
                           (seph, 0).into(), ROOT_CRDT_ID_AV, OpContents::MapSet(
            key.into(), Primitive(I64(2))
            )
        );

        db.apply_remote_op(&[t],
                           (mike, 1).into(), ROOT_CRDT_ID_AV, OpContents::MapSet(
            key.into(), Primitive(I64(3))
            )
        );

        let map = db.get_recursive_at(ROOT_CRDT_ID)
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

        let text = db.map_set(seph, ROOT_CRDT_ID, "text", NewCRDT(CRDTKind::Text));
        db.text_insert(seph, text, 0, "hi there");
        db.text_remove(seph, text, (2..5).into());

        dbg!(db.get_recursive());
        db.dbg_check(true);

        dbg!(&db);
    }
}