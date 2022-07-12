use smallvec::SmallVec;
/// The path API provides a simple way to traverse in and modify values
use smartstring::alias::String as SmartString;
use crate::{Branch, CRDTKind, LocalVersion, OpLog, Time, Value};
use crate::oplog::ROOT_MAP;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PathComponent<'a> {
    Inside,
    Key(&'a str),
}

// pub type PathRef<'a> = &'a [PathComponent];
// pub type Path = SmallVec<[PathComponent; 4]>;
pub type Path<'a> = &'a [PathComponent<'a>];
// pub type Path<'a> = &'a [&'a str];

impl Branch {
    // pub fn item_at_path_mut(&mut self, path: Path) -> Value {
    //     let mut current_kind = CRDTKind::Map;
    //     let mut current_value = Value::InnerCRDT(ROOT_MAP);
    //     let mut current_key: Option<SmartString> = None;
    //
    //     for p in path {
    //         match (current_kind, p) {
    //             // (Value::Primitive(_), _) => {
    //             //     panic!("Cannot traverse inside primitive value");
    //             // }
    //             // (Value::Map(_), PathComponent::Inside) => {
    //             //     panic!("Cannot traverse inside map");
    //             // }
    //             (CRDTKind::Map, PathComponent::Key(k)) => {
    //                 self.get_map_value(current_id, *k);
    //                 current_key = Some(k.into());
    //                 // current = Value::InnerCRDT(self.get_or_create_map_child(map_id, (*k).into()));
    //             }
    //             // (Value::InnerCRDT(item_id), PathComponent::Inside) => {
    //             //     current = self.get_value_of_register(item_id, version).unwrap();
    //             // }
    //             // (Value::InnerCRDT(item_id), PathComponent::Key(k)) => {
    //             //     let map_id = self.autoexpand_until_map(item_id, version).unwrap();
    //             //     current = Value::InnerCRDT(self.get_or_create_map_child(map_id, (*k).into()));
    //             // }
    //         }
    //     }
    //     current
    // }

    // fn autoexpand_until_map(&self, mut item_id: CRDTItemId, version: &[Time]) -> Option<MapId> {
    //     loop {
    //         let info = &self.known_crdts[item_id];
    //
    //         if info.kind == CRDTKind::LWWRegister {
    //             // Unwrap and loop
    //             let value = self.get_value_of_register(item_id, version)?;
    //             match value {
    //                 Value::Primitive(_) => {
    //                     return None;
    //                 }
    //                 Value::Map(map_id) => {
    //                     return Some(map_id);
    //                 }
    //                 Value::InnerCRDT(inner) => {
    //                     item_id = inner;
    //                     // And recurse.
    //                 }
    //             }
    //         } else {
    //             return None;
    //         }
    //     }
    // }
    //
    // // pub(crate) fn append_create_inner_crdt(&mut self, agent_id: AgentId, parents: &[Time], parent_crdt_id: ScopeId, kind: CRDTKind) -> (Time, ScopeId) {
    // // fn create_at_path(&mut self, agent_id: AgentId, parents: &[Time], path: Path, kind: CRDTKind)
    // pub fn create_crdt_at_path(&mut self, agent_id: AgentId, path: Path, kind: CRDTKind) -> Time {
    //     let v = self.now(); // I hate this.
    //     let scope = self.item_at_path_mut(path, &v).unwrap_crdt();
    //     self.append_create_inner_crdt(agent_id, &v, scope, kind).0
    // }
    //
    // pub fn create_map_at_path(&mut self, agent_id: AgentId, path: Path) -> Time {
    //     let v = self.now(); // :/
    //     let scope = self.item_at_path_mut(path, &v).unwrap_crdt();
    //     self.append_set_new_map(agent_id, &v, scope).0
    // }
    //
    // pub fn set_at_path(&mut self, agent_id: AgentId, path: Path, value: Primitive) -> Time {
    //     let v = self.now(); // :(
    //     let scope = self.item_at_path_mut(path, &v).unwrap_crdt();
    //     self.append_set(agent_id, &v, scope, value)
    // }
}