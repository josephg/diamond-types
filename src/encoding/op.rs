use bumpalo::Bump;
use bumpalo::collections::vec::Vec as BumpVec;
use rle::Searchable;
use crate::encoding::Merger;
use crate::{CausalGraph, KVPair, ListOperationCtx, ListOpMetrics, Op, OpContents, CreateValue, CollectionOp, LV};
use crate::encoding::bufparser::BufParser;
use crate::encoding::map::{ReadMap, WriteMap};
use crate::encoding::parseerror::ParseError;
use crate::encoding::tools::{push_str, push_u32, push_u64, push_usize};
use crate::encoding::varint::{mix_bit_u32, mix_bit_usize, num_encode_zigzag_i64, strip_bit_u32, strip_bit_u32_2, strip_bit_usize_2};
use crate::list::operation::ListOpKind;
use crate::oplog::ROOT_MAP;
use crate::rle::RleSpanHelpers;

fn write_time(result: &mut BumpVec<u8>, time: LV, ref_time: LV, write_map: &WriteMap, cg: &CausalGraph) {
    debug_assert!(ref_time >= time);

    // This code is adapted from parents encoding.
    // TODO: Generalize this to reuse it in both places.

    let mut write_parent_diff = |mut n: usize, is_foreign: bool, is_known: bool| {
        if is_foreign {
            n = mix_bit_usize(n, is_known);
        }
        n = mix_bit_usize(n, is_foreign);
        push_usize(result, n);
    };

    // Parents are either local or foreign. Local changes are changes we've written
    // (already) to the file. And foreign changes are changes that point outside the
    // local part of the DAG we're sending.
    //
    // Most parents will be local.

    if let Some((map, offset)) = write_map.txn_map.find_with_offset(time) {
        // Local change
        let mapped_parent = map.1.start + offset;
        write_parent_diff(ref_time - mapped_parent, false, true);
    } else {
        // Foreign change
        let item = cg.lv_to_agent_version(time);

        match write_map.map_maybe_root(&cg.client_data, item.0) {
            Ok(mapped_agent) => {
                write_parent_diff(mapped_agent as usize, true, true);
            }
            Err(name) => {
                write_parent_diff(0, true, false);
                push_str(result, name);
            }
        }
        push_usize(result, item.1);
    }
}

// fn read_time(reader: &mut BufParser, read_map: &mut ReadMap, cg: &CausalGraph, next_time: Time) -> Result<Time, ParseError> {
//     // TODO: Unify this with parents.read_parents_raw
//
//     // Parents bits:
//     // is_foreign
//     // is_known (only if is_foreign)
//     // diff (only if is_known)
//
//     let is_foreign = strip_bit_usize_2(&mut n);
//     let is_known = if is_foreign {
//         strip_bit_usize_2(&mut n)
//     } else { true };
//
//     Ok(if !is_foreign {
//         let diff = n;
//         // Local parents (parents inside this chunk of data) are stored using their local (file)
//         // time offset.
//         let file_time = next_time - diff;
//         let (entry, offset) = read_map.txn_map.find_with_offset(file_time).unwrap();
//         entry.1.at_offset(offset)
//     } else {
//         let agent = if !is_known {
//             if n != 0 { return Err(ParseError::GenericInvalidData); }
//             let agent_name = reader.next_str()?;
//             let agent = cg.get_agent_id(agent_name)
//                 .ok_or(ParseError::DataMissing)?;
//             // if persist {
//             //     read_map.agent_map.push((agent, 0));
//             // }
//             agent
//         } else {
//             // The remaining data is the mapped agent. We need to un-map it!
//             let mapped_agent = n;
//
//             if mapped_agent == 0 {
//                 // The parents list is empty (ie, our parent is ROOT). We're done here!
//                 if has_more { return Err(ParseError::GenericInvalidData); }
//                 break;
//             } else {
//                 read_map.agent_map[mapped_agent - 1].0
//             }
//         };
//
//         let seq = reader.next_usize()?;
//         // dbg!((agent, seq));
//         cg.try_crdt_id_to_version(CRDTGuid { agent, seq })
//             .ok_or(ParseError::InvalidLength)?
//     })
// }

fn write_create_value(result: &mut BumpVec<u8>, value: &CreateValue) {
    use crate::Primitive::*;

    match value {
        CreateValue::Primitive(Nil) => {
            push_u32(result, 0);
        }
        CreateValue::Primitive(Bool(b)) => {
            push_u32(result, 1);
            push_u32(result, if *b { 1 } else { 0 });
        }
        CreateValue::Primitive(I64(num)) => {
            push_u32(result, 2);
            push_u64(result, num_encode_zigzag_i64(*num));
        }
        CreateValue::Primitive(Str(str)) => {
            push_u32(result, 4);
            push_str(result, str);
        }
        CreateValue::Primitive(InvalidUninitialized) => { panic!("Invalid set") }
        CreateValue::NewCRDT(kind) => {
            let mut n = *kind as u32;
            n = mix_bit_u32(n, true); // NewCRDT vs Primitive.
            push_u32(result, n);
        }
    }
}

fn op_type(c: &OpContents) -> u32 {
    match c {
        OpContents::RegisterSet(_) => 1,
        OpContents::MapSet(_, _) => 2,
        OpContents::MapDelete(_) => 3,
        OpContents::Collection(CollectionOp::Insert(_)) => 4,
        OpContents::Collection(CollectionOp::Remove(_)) => 5,
        OpContents::Text(ListOpMetrics { kind: ListOpKind::Ins, ..}) => 6,
        OpContents::Text(ListOpMetrics { kind: ListOpKind::Del, ..}) => 7,
    }
}

fn write_op(result: &mut BumpVec<u8>, content_out: &mut BumpVec<u8>,
            expect_time: LV, last_crdt_id: LV, pair: &KVPair<Op>,
            list_ctx: &ListOperationCtx, write_map: &WriteMap, cg: &CausalGraph)
{
    let KVPair(time, op) = pair;
    debug_assert!(*time >= expect_time);

    let encode_crdt_id = last_crdt_id != op.target_id;
    let encode_time_skip = *time != expect_time;

    let mut n = op_type(&op.contents);
    n = mix_bit_u32(n, encode_crdt_id);
    n = mix_bit_u32(n, encode_time_skip);

    push_u32(result, n);

    if encode_time_skip {
        push_usize(result, *time - expect_time);
    }

    if encode_crdt_id {
        write_time(result, op.target_id, *time, write_map, cg);
    }

    match &op.contents {
        OpContents::RegisterSet(value) => {
            write_create_value(result, value);
        }
        OpContents::MapSet(key, value) => {
            // TODO: Add some sort of encoding for repeated keys.
            push_str(result, key);
            write_create_value(result, value);
        }
        OpContents::MapDelete(key) => {
            // TODO: Here as well.
            push_str(result, key);
        }
        OpContents::Collection(CollectionOp::Insert(value)) => {
            // push_u32(result, *kind as u32);
            write_create_value(result, value);
        }
        OpContents::Collection(CollectionOp::Remove(target)) => {
            write_time(result, *target, *time, write_map, cg);
        }
        OpContents::Text(text_metrics) => {
            todo!();
            // list_ctx.get_str(text_metrics.kind, text_metrics.content_pos)
        }
    }
}

pub(crate) fn write_ops<'a, I: Iterator<Item = KVPair<Op>>>(bump: &'a Bump, iter: I, first_time: LV, write_map: &WriteMap, ctx: &ListOperationCtx, cg: &CausalGraph) -> BumpVec<'a, u8> {
    let mut result = BumpVec::new_in(bump);
    let mut content_out = BumpVec::new_in(bump);
    let mut last_crdt_id = ROOT_MAP;

    // I'm not sure if this complexity is worth it yet. The idea is that we might be missing
    // information about specific operations - and if we are, we'll have jumps in the ops iterator.
    let mut expected_time = first_time;

    // Could use Merger here but I don't think it would help at all.
    for op in iter {
        write_op(&mut result, &mut content_out, expected_time, last_crdt_id, &op,
                 ctx, write_map, cg);
        last_crdt_id = op.1.target_id;
        expected_time = op.end();
    }

    result
}

// fn read_op(reader: &mut BufParser, next_time: &mut Time) -> Result<KVPair<Op>, ParseError> {
//     let mut n = reader.next_u32()?;
//     let has_time_skip = strip_bit_u32_2(&mut n);
//     let has_crdt_id = strip_bit_u32_2(&mut n);
//     let op_type = n;
//
//     if has_time_skip {
//         // push_usize(result, *time - expect_time);
//         *next_time += reader.next_usize()?;
//     }
//
//     if has_crdt_id {
//         // write_time(result, op.target_id, *time, write_map, cg);
//     }
//
//     // let mut n = op_type(&op.contents);
//     // n = mix_bit_u32(n, encode_crdt_id);
//     // n = mix_bit_u32(n, encode_time_skip);
//     //
//     // push_u32(result, n);
//
// }

// pub(crate) fn read_ops(reader: &mut BufParser, mut next_time: Time) -> Result<(), ParseError> {
//     while !reader.is_empty() {
//         let op = read_op(reader, &mut next_time)?;
//         dbg!(op);
//     }
//
//     Ok(())
// }
