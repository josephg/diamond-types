use bumpalo::Bump;
use bumpalo::collections::vec::Vec as BumpVec;
use rle::Searchable;
use crate::encoding::Merger;
use crate::{CausalGraph, KVPair, ListOperationCtx, ListOpMetrics, Op, OpContents, CreateValue, CollectionOp, LV};
use crate::encoding::bufparser::BufParser;
use crate::encoding::map::{ReadMap, WriteMap};
use crate::encoding::parseerror::ParseError;
use crate::encoding::tools::{ExtendFromSlice, push_str, push_u32, push_u64, push_usize};
use crate::encoding::varint::{mix_bit_u32, mix_bit_u64, mix_bit_usize, num_encode_zigzag_i64, strip_bit_u32, strip_bit_u32_2, strip_bit_usize_2};
use crate::list::operation::ListOpKind;
use crate::ROOT_CRDT_ID;
use crate::rle::RleSpanHelpers;

fn write_time<R: ExtendFromSlice>(result: &mut R, time: LV, ref_time: LV, persist: bool, write_map: &mut WriteMap, cg: &CausalGraph) {
    debug_assert!(ref_time >= time);

    // This code is adapted from parents encoding. I've tried really hard to share code between this
    // function and write_parents, but the combined result seems consistently more complex than
    // simply duplicating this method.

    // There are 3 kinds of values we store:
    // - Local versions - which reference times in the output write map.
    // - Foreign versions - which reference times not in the output write map
    //   - And they either have an as-of-yet unnamed agent name (in which case this is included)
    //   - Or the agent name is known, and named.

    // Parents are either local or foreign. Local changes are changes we've written
    // (already) to the file. And foreign changes are changes that point outside the
    // local part of the DAG we're sending.
    //
    // Most parents will be local.

    let mut write_n = |mut n: u64, is_foreign: bool| {
        n = mix_bit_u64(n, is_foreign);
        push_u64(result, n);
    };

    if let Some((map, offset)) = write_map.txn_map.find_with_offset(time) {
        // Local change
        let mapped_parent = map.1.start + offset;
        write_n((ref_time - mapped_parent) as u64, false);
    } else {
        // Foreign change
        let item = cg.agent_assignment.local_to_agent_version(time);

        // Foreign items are encoded as:
        // - unknown agent: 0, followed by agent name.
        // - known agents are stored as (mapped + 1).

        match write_map.map_mut(&cg.agent_assignment.client_data, item.0, persist) {
            Ok(mapped_agent) => {
                write_n(mapped_agent as u64 + 1, true);
            }
            Err(name) => {
                write_n(0, true);
                push_str(result, name);
            }
        }

        // And write the sequence number.
        push_usize(result, item.1);
    }
}

fn write_create_value<R: ExtendFromSlice>(result: &mut R, value: &CreateValue) {
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

fn write_op<R: ExtendFromSlice>(result: &mut R, _content_out: &mut R,
            expect_time: LV, last_crdt_id: LV, pair: &KVPair<Op>,
            _list_ctx: &ListOperationCtx, write_map: &mut WriteMap, cg: &CausalGraph)
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
        write_time(result, op.target_id, *time, true, write_map, cg);
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
            write_time(result, *target, *time, true, write_map, cg);
        }
        OpContents::Text(_text_metrics) => {
            todo!();
            // list_ctx.get_str(text_metrics.kind, text_metrics.content_pos)
        }
    }
}

pub(crate) fn write_ops<'a, I: Iterator<Item = KVPair<Op>>>(bump: &'a Bump, iter: I, first_time: LV, write_map: &mut WriteMap, ctx: &ListOperationCtx, cg: &CausalGraph) -> BumpVec<'a, u8> {
    let mut result = BumpVec::new_in(bump);
    let mut content_out = BumpVec::new_in(bump);
    let mut last_crdt_id = ROOT_CRDT_ID;

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
