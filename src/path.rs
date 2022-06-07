use smallvec::SmallVec;
/// The path API provides a simple way to traverse in and modify values
use smartstring::alias::String as SmartString;
use crate::{AgentId, CRDTKind, LocalVersion, NewOpLog, ScopeId, Time};
use crate::new_oplog::{Primitive, ROOT_SCOPE, Value};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PathComponent<'a> {
    Inside,
    Key(&'a str),
}

// pub type PathRef<'a> = &'a [PathComponent];
// pub type Path = SmallVec<[PathComponent; 4]>;
pub type Path<'a> = &'a [PathComponent<'a>];

impl NewOpLog {
    pub fn now(&self) -> LocalVersion {
        self.version.clone()
    }

    pub fn scope_at_path_mut(&mut self, path: Path, version: &[Time]) -> Value {
        let mut current = Value::InnerCRDT(ROOT_SCOPE);

        for p in path {
            if let Value::InnerCRDT(id) = current {
                let info = &self.scopes[id];

                match (p, info.kind) {
                    (PathComponent::Inside, CRDTKind::LWWRegister) => {
                        current = self.get_value_of_register(id, version).unwrap();
                    },
                    (PathComponent::Key(key), CRDTKind::Map) => {
                        current = Value::InnerCRDT(self.get_or_create_map_child(id, (*key).into()))
                    },
                    (PathComponent::Key(key), CRDTKind::LWWRegister) => {
                        let inner = self.autoexpand_until_type(id, CRDTKind::Map, version)
                            .expect("Cannot expand key for non-map type");
                        current = Value::InnerCRDT(self.get_or_create_map_child(inner, (*key).into()))
                    },
                    (_, _) => {
                        panic!("Cannot traverse inside path component {:?} {:?}", p, current);
                    }
                }

            } else {
                panic!("Cannot traverse inside primitive value");
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

    fn autoexpand_until_type(&self, mut scope_id: ScopeId, target_kind: CRDTKind, version: &[Time]) -> Option<ScopeId> {
        loop {
            let info = &self.scopes[scope_id];

            if info.kind == target_kind {
                // Found it.
                return Some(scope_id);
            } else if info.kind == CRDTKind::LWWRegister {
                // Unwrap and loop
                scope_id = self.get_value_of_register(scope_id, version)?.scope()?;
            } else {
                return None;
            }
        }
    }

    // pub(crate) fn append_create_inner_crdt(&mut self, agent_id: AgentId, parents: &[Time], parent_crdt_id: ScopeId, kind: CRDTKind) -> (Time, ScopeId) {
    // fn create_at_path(&mut self, agent_id: AgentId, parents: &[Time], path: Path, kind: CRDTKind)
    pub fn create_at_path(&mut self, agent_id: AgentId, path: Path, kind: CRDTKind) {
        let v = self.now(); // I hate this.
        let scope = self.scope_at_path_mut(path, &v).unwrap_scope();
        self.append_create_inner_crdt(agent_id, &v, scope, kind);
    }

    pub fn set_at_path(&mut self, agent_id: AgentId, path: Path, value: Primitive) {
        let v = self.now(); // :(
        let scope = self.scope_at_path_mut(path, &v).unwrap_scope();
        self.append_set(agent_id, &v, scope, value);
    }
}