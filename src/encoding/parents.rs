use bumpalo::Bump;
use rle::{HasLength, Searchable};
use crate::encoding::tools::{push_str, push_u32, push_usize};
use crate::encoding::varint::*;
use crate::causalgraph::parents::ParentsEntrySimple;
use crate::remotespan::CRDTGuid;
use crate::{AgentId, CausalGraph, DTRange, KVPair, LocalVersion, OpLog, RleVec, Time};
use crate::encoding::Merger;
use bumpalo::collections::vec::Vec as BumpVec;
use smallvec::SmallVec;
use crate::encoding::bufparser::BufParser;
use crate::encoding::parseerror::ParseError;
use crate::encoding::map::{WriteMap, ReadMap};
use crate::frontier::clean_version;



pub(crate) fn write_parents_raw(result: &mut BumpVec<u8>, parents: &[Time], next_output_time: Time, persist: bool, write_map: &mut WriteMap, cg: &CausalGraph) {
    // And the parents.
    if parents.is_empty() {
        // Parenting off the root is special-cased, because its rare in practice (well,
        // usually exactly 1 item will have the parents as root). We'll write a single dummy
        // value with foreign 0 here, because we (unfortunately) need to mark the list is
        // empty.

        // push_usize(result, 2);
        // let mapped_agent = 0, has_more = false, is_foreign = true, is_known = true, first = true -> val = 3.
        push_u32(result, 3);
    } else {
        let mut iter = parents.iter().peekable();
        // let mut first = true;
        while let Some(&p) = iter.next() {
            let has_more = iter.peek().is_some();

            // TODO: Rewrite to share write_time from op encoding.

            let mut write_parent_diff = |mut n: usize, is_foreign: bool, is_known: bool| {
                n = mix_bit_usize(n, has_more);
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

            if let Some((map, offset)) = write_map.txn_map.find_with_offset(p) {
                // Local change!
                // TODO: There's a sort of bug here. Local parents should (probably?) be sorted
                // in the file, but this mapping doesn't guarantee that. Currently I'm
                // re-sorting after reading - which is necessary for external parents anyway.
                // But allowing unsorted local parents is vaguely upsetting.
                let mapped_parent = map.1.start + offset;

                write_parent_diff(next_output_time - mapped_parent, false, true);
            } else {
                // Foreign change
                // println!("Region does not contain parent for {}", p);

                let item = cg.version_to_crdt_id(p);
                let mapped_agent = write_map.map_maybe_root_mut(&cg.client_data, item.agent, persist);

                // There are probably more compact ways to do this, but the txn data set is
                // usually quite small anyway, even in large histories. And most parents objects
                // will be in the set anyway. So I'm not too concerned about a few extra bytes
                // here.
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

// *** Read path ***

pub(crate) fn read_parents_raw(reader: &mut BufParser, persist: bool, cg: &mut CausalGraph, next_time: Time, read_map: &mut ReadMap) -> Result<LocalVersion, ParseError> {
    let mut parents = SmallVec::<[usize; 2]>::new();

    loop {
        let mut n = reader.next_usize()?;

        // Parents bits:
        // is_foreign
        // is_known (only if is_foreign)
        // has_more
        // diff (only if is_known)

        let is_foreign = strip_bit_usize_2(&mut n);
        let is_known = if is_foreign {
            strip_bit_usize_2(&mut n)
        } else { true };
        let has_more = strip_bit_usize_2(&mut n);

        let parent = if !is_foreign {
            let diff = n;
            // Local parents (parents inside this chunk of data) are stored using their local (file)
            // time offset.
            let file_time = next_time - diff;
            let (entry, offset) = read_map.txn_map.find_with_offset(file_time).unwrap();
            entry.1.at_offset(offset)
        } else {
            let agent = if !is_known {
                if n != 0 { return Err(ParseError::GenericInvalidData); }
                let agent_name = reader.next_str()?;
                let agent = cg.get_or_create_agent_id(agent_name);
                if persist {
                    read_map.agent_map.push((agent, 0));
                }
                agent
            } else {
                // The remaining data is the mapped agent. We need to un-map it!
                let mapped_agent = n;

                if mapped_agent == 0 {
                    // The parents list is empty (ie, our parent is ROOT). We're done here!
                    if has_more { return Err(ParseError::GenericInvalidData); }
                    break;
                } else {
                    read_map.agent_map[mapped_agent - 1].0
                }
            };

            let seq = reader.next_usize()?;
            // dbg!((agent, seq));
            cg.try_crdt_id_to_version(CRDTGuid { agent, seq })
                .ok_or(ParseError::InvalidLength)?
        };

        parents.push(parent);
        // debug_assert!(frontier_is_sorted(&parents));

        if !has_more { break; }
    }

    // The parents list could legitimately end up out of order due to foreign items being imported
    // in a different order from the original local order.
    //
    // This is fine - we can just re-sort.
    clean_version(&mut parents);

    Ok(parents)
}
