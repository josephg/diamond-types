use crate::*;
use crate::branch::DTValue;
use crate::oplog::ROOT_MAP;
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

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.oplog.cg.get_or_create_agent_id(name)
    }

    pub fn set_map_primitive(&mut self, agent_id: AgentId, map_id: Time, key: &str, value: Primitive) -> Time {
        let time = self.oplog.set_map(agent_id, map_id, key, value.clone());

        self.branch.inner_set_map(time, agent_id, map_id, key, Value::Primitive(value));
        self.branch.set_time(time); // gross.

        time
    }

    pub fn set_lww_primitive(&mut self, agent_id: AgentId, lww_id: Time, value: Primitive) -> Time {
        let time = self.oplog.set_lww(agent_id, lww_id, value.clone());

        self.branch.inner_set_lww(time, agent_id, lww_id, Value::Primitive(value));
        self.branch.set_time(time);

        time
    }

    pub fn create_inner_map(&mut self, agent_id: AgentId, crdt_id: Time, key: Option<&str>) -> Time {
        let time = self.oplog.create_inner_map(agent_id, crdt_id, key);
        self.branch.create_inner_map(time, agent_id, crdt_id, key);
        time
    }

    pub fn get_recursive_at(&self, crdt_id: Time) -> Option<DTValue> {
        self.branch.get_recursive_at(crdt_id)
    }

    pub fn get_recursive(&self) -> Option<DTValue> {
        self.get_recursive_at(ROOT_MAP)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn foo() {
        let mut db = SimpleDatabase::open("test").unwrap();
        let seph = db.get_or_create_agent_id("seph");
        db.set_map_primitive(seph, ROOT_MAP, "name", Primitive::Str("seph".into()));

        dbg!(db.get_recursive());

        dbg!(&db.branch.overlay_version);
        dbg!(&db.oplog.version);

        dbg!(&db);
    }
}