use std::collections::BTreeMap;
use crate::{Branch, CRDTKind, LV, Primitive, RegisterValue, ROOT_CRDT_ID};
use smartstring::alias::String as SmartString;

#[derive(Debug, Clone)]
pub enum SimpleVal {
    Text(String),
    Map(BTreeMap<SmartString, Box<SimpleVal>>),
    Primitive(Primitive),
}

impl Branch {
    fn simple_val_at(&self, key: LV, kind: CRDTKind) -> SimpleVal {
        match kind {
            CRDTKind::Map => {
                let mut map = BTreeMap::new();
                for (key, state) in self.maps.get(&key).unwrap() {
                    // TODO: Rewrite this as an iterator map then collect().
                    map.insert(key.clone(), Box::new(match &state.value {
                        RegisterValue::Primitive(primitive) => {
                            SimpleVal::Primitive(primitive.clone())
                        }
                        RegisterValue::OwnedCRDT(inner_kind, inner_key) => {
                            self.simple_val_at(*inner_key, *inner_kind)
                        }
                    }));
                }
                SimpleVal::Map(map)
            }
            CRDTKind::Register => {
                // TODO
                SimpleVal::Primitive(Primitive::Nil)
            }
            CRDTKind::Collection => {
                // todo!();
                SimpleVal::Primitive(Primitive::Nil)
            }
            CRDTKind::Text => {
                SimpleVal::Text(self.texts.get(&key).unwrap().to_string())
            }
        }
    }

    pub fn simple_val(&self) -> SimpleVal {
        self.simple_val_at(ROOT_CRDT_ID, CRDTKind::Map)
    }
}