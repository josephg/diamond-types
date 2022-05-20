use std::collections::BTreeMap;
use crate::{ScopeId, LocalVersion, NewOpLog, Time, CRDTKind};
use crate::new_oplog::{Primitive, ROOT_SCOPE, Value};
use smartstring::alias::String as SmartString;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DTValue {
    Primitive(Primitive),
    // Register(Box<DTValue>),
    Map(BTreeMap<SmartString, Box<DTValue>>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewBranch {
    /// The version the branch is currently at. This is used to track which changes the branch has
    /// or has not locally merged.
    ///
    /// This field is public for convenience, but you should never modify it directly. Instead use
    /// the associated functions on Branch.
    version: LocalVersion,

    /// The document's content
    content: DTValue,
}

impl NewOpLog {
    fn checkout_scope(&self, scope_id: ScopeId, version: &[Time]) -> Option<DTValue> {
        // Recursive is probably the best option here?

        let info = &self.scopes[scope_id];
        Some(match info.kind {
            CRDTKind::LWWRegister => {
                let val = self.get_value_of_register(scope_id, version)?;
                match val {
                    Value::Primitive(p) => DTValue::Primitive(p),
                    Value::InnerCRDT(id) => {
                        // let inner = self.checkout_scope(id, version).unwrap();
                        // // DTValue::Register(Box::new(inner))
                        // Some(inner)
                        self.checkout_scope(id, version)?
                    }
                }
            }
            CRDTKind::Map => {
                DTValue::Map(info.map_children.as_ref().unwrap().iter().filter_map(|(key, value)| {
                    let inner = self.checkout_scope(*value, version)?;
                    Some((key.clone(), Box::new(inner)))
                }).collect())
            }
            CRDTKind::Text => {
                unimplemented!()
            }
        })
    }

    pub fn checkout(&self, version: &[Time]) -> Option<DTValue> {
        self.checkout_scope(ROOT_SCOPE, version)
    }
}

#[cfg(test)]
mod test {
    use crate::{CRDTKind, NewOpLog};
    use crate::new_oplog::{ROOT_SCOPE, Value};
    use smartstring::alias::String as SmartString;
    use crate::new_oplog::Primitive::Str;

    #[test]
    fn checkout_inner_map() {
        let mut oplog = NewOpLog::new();
        // dbg!(oplog.checkout(&oplog.version));

        let seph = oplog.get_or_create_agent_id("seph");
        let map_id = oplog.append_create_inner_crdt(seph, &[], ROOT_SCOPE, CRDTKind::Map).1;
        // dbg!(oplog.checkout(&oplog.version));

        let title_id = oplog.get_or_create_map_child(map_id, "title".into());
        oplog.append_set(seph, &oplog.version.clone(), title_id, Str("Cool title bruh".into()));

        let author_id = oplog.get_or_create_map_child(map_id, "author".into());
        let author_map = oplog.append_create_inner_crdt(seph, &oplog.version.clone(), author_id, CRDTKind::Map).1;

        let email_id = oplog.get_or_create_map_child(author_map, "email".into());
        oplog.append_set(seph, &oplog.version.clone(), email_id, Str("me@josephg.com".into()));

        // oplog.append_set(seph, &oplog.version.clone(), author_id, Value::);



        dbg!(oplog.checkout(&oplog.version));


        // dbg!(oplog.get_value_of_register(ROOT_CRDT_ID, &oplog.version.clone()));
        // dbg!(&oplog);
        oplog.dbg_check(true);
    }
}


















