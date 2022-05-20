use std::collections::BTreeMap;
use crate::LocalVersion;
use crate::new_oplog::{Primitive, Value};


// #[derive(Debug, Clone, Eq, PartialEq)]
// pub enum DTValue {
//     Primitive(Primitive),
//     Map(BTreeMap<>
// }
//
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
//     /// The document's content
//     content: Value,
// }
//
