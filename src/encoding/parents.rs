use bumpalo::Bump;
use rle::HasLength;
use crate::encoding::tools::{push_str, push_usize};
use crate::encoding::varint::*;
use crate::history::MinimalHistoryEntry;
use crate::remotespan::CRDTGuid;
use crate::{AgentId, DTRange, KVPair, NewOpLog, RleVec};
use crate::encoding::agent_assignment::AgentMapping;
use crate::encoding::Merger;
use bumpalo::collections::vec::Vec as BumpVec;

/// Map from local oplog versions -> file versions. Each entry is KVPair(local start, file range).
pub(crate) type TxnMap = RleVec::<KVPair<DTRange>>;

pub(crate) fn write_txn_parents(result: &mut BumpVec<u8>, mut tag: bool, txn: &MinimalHistoryEntry,
                     txn_map: &mut TxnMap, agent_map: &mut AgentMapping, persist: bool, oplog: &NewOpLog,
) {
    let len = txn.len();

    let next_output_time = txn_map.last().map_or(0, |last| last.1.end);
    let output_range = (next_output_time .. next_output_time + len).into();

    // NOTE: we're using .insert instead of .push here so the txn_map stays in the expected order!
    if persist {
        txn_map.insert(KVPair(txn.span.start, output_range));
    }
    // txn_map.push(KVPair(txn.span.start, output_range));

    // And the parents.
    if txn.parents.is_empty() {
        // Parenting off the root is special-cased, because its rare in practice (well,
        // usually exactly 1 item will have the parents as root). We'll write a single dummy
        // value with foreign 0 here, because we (unfortunately) need to mark the list is
        // empty.

        // let mapped_agent = 0, has_more = false, is_foreign = true, first = true -> val = 2.
        // push_usize(result, 2);
        push_usize(result, if tag { 2 } else { 1 });
    } else {
        let mut iter = txn.parents.iter().peekable();
        // let mut first = true;
        while let Some(&p) = iter.next() {
            let has_more = iter.peek().is_some();

            let mut write_parent_diff = |mut n: usize, is_foreign: bool, is_known: bool| {
                n = mix_bit_usize(n, has_more);
                if is_foreign {
                    n = mix_bit_usize(n, is_known);
                }
                n = mix_bit_usize(n, is_foreign);
                if std::mem::take(&mut tag) {
                    n = mix_bit_usize(n, false);
                }
                push_usize(result, n);
            };

            // Parents are either local or foreign. Local changes are changes we've written
            // (already) to the file. And foreign changes are changes that point outside the
            // local part of the DAG we're sending.
            //
            // Most parents will be local.

            if let Some((map, offset)) = txn_map.find_with_offset(p) {
                // Local change!
                // TODO: There's a sort of bug here. Local parents should (probably?) be sorted
                // in the file, but this mapping doesn't guarantee that. Currently I'm
                // re-sorting after reading - which is necessary for external parents anyway.
                // But allowing unsorted local parents is vaguely upsetting.
                let mapped_parent = map.1.start + offset;

                write_parent_diff(output_range.start - mapped_parent, false, true);
            } else {
                // Foreign change
                // println!("Region does not contain parent for {}", p);

                let item = oplog.version_to_crdt_id(p);
                let mapped_agent = agent_map.map_maybe_root(&oplog.client_data, item.agent, persist);

                // There are probably more compact ways to do this, but the txn data set is
                // usually quite small anyway, even in large histories. And most parents objects
                // will be in the set anyway. So I'm not too concerned about a few extra bytes
                // here.
                //
                // I'm adding 1 to the mapped agent to make room for ROOT. This is quite dirty!
                match mapped_agent {
                    Ok(mapped_agent) => {
                        // If the parent is ROOT, the parents is empty - which is handled above.
                        debug_assert!(mapped_agent >= 1);
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
    }
}

pub fn encode_parents<'a, I: Iterator<Item=MinimalHistoryEntry>>(bump: &'a Bump, iter: I, map: &mut AgentMapping, oplog: &NewOpLog) -> BumpVec<'a, u8> {
    let mut txn_map = TxnMap::new();
    let mut next_output_time = 0;
    let mut result = BumpVec::new_in(bump);

    Merger::new(|txn: MinimalHistoryEntry, map: &mut AgentMapping| {
        // next_output_time,
        write_txn_parents(&mut result, false, &txn, &mut txn_map, map, true, oplog);
        next_output_time += txn.len();
    }).flush_iter2(iter, map);

    result
}