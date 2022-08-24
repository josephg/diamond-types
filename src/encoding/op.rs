use bumpalo::Bump;
use bumpalo::collections::vec::Vec as BumpVec;
use crate::encoding::Merger;
use crate::{CausalGraph, KVPair, ListOperationCtx, ListOpMetrics, Op, OpContents, CreateValue, SetOp, Time};
use crate::encoding::map::WriteMap;
use crate::encoding::tools::{push_str, push_u32, push_u64, push_usize};
use crate::encoding::varint::{mix_bit_u32, mix_bit_usize, num_encode_zigzag_i64};
use crate::list::operation::ListOpKind;
use crate::oplog::ROOT_MAP;
use crate::rle::RleSpanHelpers;

// fn write_time(result: &mut BumpVec<u8>, time: Time, ref_time: Time, persist: bool, agent_map: &mut AgentMappingEnc, txn_map: &TxnMap, cg: &CausalGraph) {
fn write_time(result: &mut BumpVec<u8>, time: Time, ref_time: Time, write_map: &WriteMap, cg: &CausalGraph)
{
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
        let item = cg.version_to_crdt_id(time);

        match write_map.map_maybe_root(&cg.client_data, item.agent) {
            Ok(mapped_agent) => {
                write_parent_diff(mapped_agent as usize, true, true);
            }
            Err(name) => {
                write_parent_diff(0, true, false);
                push_str(result, name);
            }
        }
        push_usize(result, item.seq);
    }
}

fn write_create_value(result: &mut BumpVec<u8>, value: &CreateValue) {
    use crate::Primitive::*;

    match value {
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
        OpContents::Set(SetOp::Insert(_)) => 3,
        OpContents::Set(SetOp::Remove(_)) => 4,
        OpContents::Text(ListOpMetrics { kind: ListOpKind::Ins, ..}) => 5,
        OpContents::Text(ListOpMetrics { kind: ListOpKind::Del, ..}) => 6,
    }
}

fn write_op(result: &mut BumpVec<u8>, content_out: &mut BumpVec<u8>,
            expect_time: Time, last_crdt_id: Time, pair: &KVPair<Op>,
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
        OpContents::Set(SetOp::Insert(value)) => {
            // push_u32(result, *kind as u32);
            write_create_value(result, value);
        }
        OpContents::Set(SetOp::Remove(target)) => {
            write_time(result, *target, *time, write_map, cg);
        }
        OpContents::Text(text_metrics) => {
            todo!();
            // list_ctx.get_str(text_metrics.kind, text_metrics.content_pos)
        }
    }
}

pub(crate) fn write_ops<'a, I: Iterator<Item = KVPair<Op>>>(bump: &'a Bump, iter: I, first_time: Time, write_map: &WriteMap, ctx: &ListOperationCtx, cg: &CausalGraph) -> BumpVec<'a, u8> {
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


