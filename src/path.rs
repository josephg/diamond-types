use smallvec::SmallVec;
/// The path API provides a simple way to traverse in and modify values
use smartstring::alias::String as SmartString;
use crate::{AgentId, CRDTKind, LocalVersion, NewOpLog, CRDTItemId, Time, MapId};
use crate::new_oplog::{Primitive, ROOT_MAP, Value};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PathComponent<'a> {
    Inside,
    Key(&'a str),
}

// pub type PathRef<'a> = &'a [PathComponent];
// pub type Path = SmallVec<[PathComponent; 4]>;
pub type Path<'a> = &'a [PathComponent<'a>];
// pub type Path<'a> = &'a [&'a str];

impl NewOpLog {
    pub fn now(&self) -> LocalVersion {
        self.version.clone()
    }

    pub fn item_at_path_mut(&mut self, path: Path, version: &[Time]) -> Value {
        let mut current = Value::Map(ROOT_MAP);

        for p in path {
            match (current, p) {
                (Value::Primitive(_), _) => {
                    panic!("Cannot traverse inside primitive value");
                }
                (Value::Map(_), PathComponent::Inside) => {
                    panic!("Cannot traverse inside map");
                }
                (Value::Map(map_id), PathComponent::Key(k)) => {
                    // TODO: I don't like this k.into().
                    current = Value::InnerCRDT(self.get_or_create_map_child(map_id, (*k).into()));
                }
                (Value::InnerCRDT(item_id), PathComponent::Inside) => {
                    current = self.get_value_of_register(item_id, version).unwrap();
                }
                (Value::InnerCRDT(item_id), PathComponent::Key(k)) => {
                    let map_id = self.autoexpand_until_map(item_id, version).unwrap();
                    current = Value::InnerCRDT(self.get_or_create_map_child(map_id, (*k).into()));
                }
            }
        }
        current
    }

    // pub fn scope_at_path(&self, path: PathRef, version: &[Time]) -> Value {
    //     let mut current = Value::InnerCRDT(ROOT_SCOPE);
    //
    //     for p in path {
    //         if let Value::InnerCRDT(id) = current {
    //             let info = &self.scopes[id];
    //
    //             match (p, info.kind) {
    //                 (PathComponent::Inside, CRDTKind::LWWRegister) => {
    //                     current = self.get_value_of_register(id, version).unwrap();
    //                 },
    //                 (PathComponent::Key(key), CRDTKind::Map) => {
    //                     current = Value::InnerCRDT(self.get_map_child(id, key).unwrap())
    //                 },
    //                 (_, _) => {
    //                     panic!("Cannot traverse inside path component {:?} {:?}", p, current);
    //                 }
    //             }
    //
    //         } else {
    //             panic!("Cannot traverse inside primitive value");
    //         }
    //     }
    //
    //     current
    // }

    fn autoexpand_until_map(&self, mut item_id: CRDTItemId, version: &[Time]) -> Option<MapId> {
        loop {
            let info = &self.known_crdts[item_id];

            if info.kind == CRDTKind::LWWRegister {
                // Unwrap and loop
                let value = self.get_value_of_register(item_id, version)?;
                match value {
                    Value::Primitive(_) => {
                        return None;
                    }
                    Value::Map(map_id) => {
                        return Some(map_id);
                    }
                    Value::InnerCRDT(inner) => {
                        item_id = inner;
                        // And recurse.
                    }
                }
            } else {
                return None;
            }
        }
    }

    // pub(crate) fn append_create_inner_crdt(&mut self, agent_id: AgentId, parents: &[Time], parent_crdt_id: ScopeId, kind: CRDTKind) -> (Time, ScopeId) {
    // fn create_at_path(&mut self, agent_id: AgentId, parents: &[Time], path: Path, kind: CRDTKind)
    pub fn create_at_path(&mut self, agent_id: AgentId, path: Path, kind: CRDTKind) {
        let v = self.now(); // I hate this.
        let scope = self.item_at_path_mut(path, &v).unwrap_crdt();
        self.append_create_inner_crdt(agent_id, &v, scope, kind);
    }

    pub fn set_at_path(&mut self, agent_id: AgentId, path: Path, value: Primitive) {
        let v = self.now(); // :(
        let scope = self.item_at_path_mut(path, &v).unwrap_crdt();
        self.append_set(agent_id, &v, scope, value);
    }
}