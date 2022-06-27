use bumpalo::Bump;
use bumpalo::collections::vec::Vec as BumpVec;
use crate::new_oplog::{Primitive, Value};
use crate::{CRDTKind, NewOpLog};
use num_enum::TryFromPrimitive;
use crate::encoding::tools::{push_str, push_u32, push_u64};
use crate::encoding::varint::num_encode_zigzag_i64;

#[derive(Debug, PartialEq, Eq, Copy, Clone, TryFromPrimitive)]
#[repr(u32)]
enum ValueType {
    // TODO: Assign numbers!
    PrimFalse,
    PrimTrue,

    PrimSInt,
    PrimUInt,

    PrimFloat,
    PrimDouble,

    PrimStr,

    LWWRegister,
    MVRegister,
    Map,
    Set,
    Text,
    // TODO: Arbitrary shifty list
}

pub fn encode_op_contents<'a, 'b: 'a, I: Iterator<Item=&'b Value>>(bump: &'a Bump, iter: I, oplog: &NewOpLog) -> BumpVec<'a, u8> {
    let mut result = BumpVec::new_in(bump);

    for val in iter {
        match val {
            Value::Primitive(Primitive::I64(n)) => {
                push_u32(&mut result, ValueType::PrimSInt as u32);
                push_u64(&mut result, num_encode_zigzag_i64(*n));
            }
            Value::Primitive(Primitive::Str(s)) => {
                push_u32(&mut result, ValueType::PrimStr as u32);
                push_str(&mut result, s);
            }
            Value::Map(_) => {
                push_u32(&mut result, ValueType::Map as u32);
            }
            Value::InnerCRDT(crdt_id) => {
                let kind = oplog.known_crdts[*crdt_id].kind;
                let kind_value = match kind {
                    CRDTKind::LWWRegister => ValueType::LWWRegister,
                    CRDTKind::Text => ValueType::Text,
                };
                push_u32(&mut result, kind_value as u32);
            }
        }
    }

    result
}
