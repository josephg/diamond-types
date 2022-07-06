// use std::collections::BTreeMap;
// use crate::{LocalVersion, NewOpLog, Time, MapId};
// use crate::new_oplog::{Primitive, ROOT_MAP, Value};
// use smartstring::alias::String as SmartString;
//
// #[derive(Debug, Clone, Eq, PartialEq)]
// pub enum DTValue {
//     Primitive(Primitive),
//     // Register(Box<DTValue>),
//     Map(BTreeMap<SmartString, Box<DTValue>>),
// }
//
// #[derive(Debug, Clone, Eq, PartialEq)]
// pub struct NewBranch {
//     /// The version the branch is currently at. This is used to track which changes the branch has
//     /// or has not locally merged.
//     ///
//     /// This field is public for convenience, but you should never modify it directly. Instead use
//     /// the associated functions on Branch.
//     version: LocalVersion,
//
//     /// The document's content. Always a Map at the top level.
//     content: DTValue,
// }
//
// impl NewOpLog {
//     fn checkout_map(&self, map_id: MapId, version: &[Time]) -> Option<BTreeMap<SmartString, Box<DTValue>>> {
//         let map = &self.maps[map_id];
//         // if map.created_at
//         if !self.cg.history.version_contains_time(version, map.created_at) {
//             return None;
//         }
//
//         let mut result =
//             map.children.iter().filter_map(|(key, item_id)| {
//                 let inner = self.checkout_crdt(*item_id, version)?;
//                 Some((key.clone(), Box::new(inner)))
//             }).collect();
//
//         Some(result)
//     }
//
//     fn checkout_crdt(&self, item_id: CRDTItemId, version: &[Time]) -> Option<DTValue> {
//         // Recursive is probably the best option here?
//         let info = &self.known_crdts[item_id];
//         match info.kind {
//             CRDTKind::LWWRegister => {
//                 let val = self.get_value_of_register(item_id, version)?;
//                 match val {
//                     Value::Primitive(p) => Some(DTValue::Primitive(p)),
//                     Value::Map(map_id) => {
//                         self.checkout_map(map_id, version)
//                             .map(DTValue::Map)
//                     }
//                     Value::InnerCRDT(id) => {
//                         // let inner = self.checkout_scope(id, version).unwrap();
//                         // // DTValue::Register(Box::new(inner))
//                         // Some(inner)
//                         self.checkout_crdt(id, version)
//                     }
//                 }
//             }
//             CRDTKind::Text => {
//                 unimplemented!()
//             }
//         }
//     }
//
//     pub fn checkout(&self, version: &[Time]) -> Option<BTreeMap<SmartString, Box<DTValue>>> {
//         self.checkout_map(ROOT_MAP, version)
//     }
// }
//
// #[cfg(test)]
// mod test {
//     use crate::{CRDTKind, NewOpLog};
//     use crate::new_oplog::{ROOT_MAP, Value};
//     use smartstring::alias::String as SmartString;
//     use crate::new_oplog::Primitive::Str;
//     use crate::path::PathComponent;
//
//     #[test]
//     fn checkout_inner_map() {
//         let mut oplog = NewOpLog::new();
//         // dbg!(oplog.checkout(&oplog.version));
//
//         let seph = oplog.get_or_create_agent_id("seph");
//         let map_id = ROOT_MAP;
//         // dbg!(oplog.checkout(&oplog.version));
//
//         let title_id = oplog.get_or_create_map_child(map_id, "title".into());
//         oplog.append_set(seph, &oplog.version.clone(), title_id, Str("Cool title bruh".into()));
//
//         let author_id = oplog.get_or_create_map_child(map_id, "author".into());
//         let author_map = oplog.append_set_new_map(seph, &oplog.version.clone(), author_id).1;
//
//         let email_id = oplog.get_or_create_map_child(author_map, "email".into());
//         oplog.append_set(seph, &oplog.version.clone(), email_id, Str("me@josephg.com".into()));
//
//         // oplog.append_set(seph, &oplog.version.clone(), author_id, Value::);
//
//
//
//         dbg!(oplog.checkout(&oplog.version));
//
//
//         // dbg!(oplog.get_value_of_register(ROOT_CRDT_ID, &oplog.version.clone()));
//         // dbg!(&oplog);
//         oplog.dbg_check(true);
//     }
//
//     #[test]
//     fn checkout_inner_map_path() {
//         use PathComponent::*;
//         use CRDTKind::*;
//
//         let mut oplog = NewOpLog::new();
//         let seph = oplog.get_or_create_agent_id("seph");
//
//         oplog.set_at_path(seph, &[Key("title")], Str("Cool title bruh".into()));
//
//         oplog.create_map_at_path(seph, &[Key("author")]);
//         oplog.set_at_path(seph, &[Key("author"), Key("name")], Str("Seph".into()));
//
//         dbg!(oplog.checkout(&oplog.version));
//
//         oplog.dbg_check(true);
//     }
//
//     // #[test]
//     // fn crdt_gets_overwritten() {
//     //     use PathComponent::*;
//     //     use CRDTKind::*;
//     //
//     //     let mut oplog = NewOpLog::new();
//     //     let seph = oplog.get_or_create_agent_id("seph");
//     //
//     //     oplog.create_at_path(seph, &[], Map);
//     //     oplog.create_at_path(seph, &[], Map);
//     //
//     //     dbg!(oplog.checkout(&oplog.version));
//     //
//     //     oplog.dbg_check(true);
//     //     dbg!(&oplog);
//     // }
// }
//
//
//
//
//
//
