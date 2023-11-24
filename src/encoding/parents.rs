use rle::Searchable;
use crate::encoding::tools::{ExtendFromSlice, push_str, try_push_str};
use crate::encoding::varint::*;
use crate::{Frontier, LV};
// use bumpalo::collections::vec::Vec as BumpVec;
use smallvec::SmallVec;
use crate::causalgraph::agent_assignment::AgentAssignment;
use crate::encoding::bufparser::BufParser;
use crate::encoding::parseerror::ParseError;
use crate::encoding::map::{ReadMap, WriteMap};
use crate::frontier::sort_frontier;

pub(crate) fn write_parents_raw<R: ExtendFromSlice>(result: &mut R, parents: &[LV], next_output_time: LV, persist: bool, write_map: &mut WriteMap, aa: &AgentAssignment) {
    // println!("Write parents {:?} next_output_time {next_output_time}", parents);
    // For parents we need to differentiate a few different cases:
    // - The parents list is empty. This means the item is the first operation in its history.
    // - If that isn't true, each item in parents is either:
    //   1. A "local change" (part of this write map). We store the offset in the file / data set.
    //   2. A "foreign change" with a known user agent. Mark the mapped user agent and the seq.
    //   3. A foreign change with an unknown user agent. Write the agent string and offset.

    // To express this, each item has 3 bits mixed in:
    // 1. has_more (are there more entries in this list)
    // 2. is_known (only if is_foreign). Is the agent known? If this is false, the number will
    //    be followed by an agent name string.
    // 3. is_foreign. Is the version part of the write_map? If so, we encode a delta. If false,
    //    we encode the remote (mapped agent, seq) pair.

    // An empty list (ROOT item) is encoded as (false, true, true) with mapped agent ID 0.
    // A local item is encoded as (has_more, _, false).
    // A foreign change with known agent is (has_more, true, true) with mapped agent ID 1+mapped.
    // A foreign change with unknown agent is (has_more, false, true) followed by the agent string.

    #[inline]
    fn write_parent_diff<R: ExtendFromSlice>(result: &mut R, mut n: usize, has_more: bool, is_foreign: bool) {
        // dbg!(n, is_foreign, is_known);
        n = mix_bit_usize(n, has_more);
        n = mix_bit_usize(n, is_foreign);
        push_usize(result, n);
    }

    if parents.is_empty() {
        // Parenting off the root is special-cased, because its rare in practice (well,
        // usually exactly 1 item will have the parents as root). We'll write a single dummy
        // value with foreign 0 here, because we (unfortunately) need to mark the list is
        // empty.
        write_parent_diff(result, 0, false, true);
    } else {
        let mut iter = parents.iter().peekable();
        // let mut first = true;
        while let Some(&p) = iter.next() {
            debug_assert_ne!(p, usize::MAX);

            let has_more = iter.peek().is_some();

            // TODO: Rewrite to share write_time from op encoding.

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

                write_parent_diff(result, next_output_time - mapped_parent, has_more, false);
            } else {
                // Foreign change
                // println!("Region does not contain parent for {}", p);

                let item = aa.local_to_agent_version(p);
                // println!("Writing foreign parent item {:?}", item);
                // I'm using maybe_root here not because item.0 is ever ROOT, but because we're
                // special casing mapped agent 0 to express an empty parents list above.
                let mapped_agent = write_map.map_mut(&aa.client_data, item.0, persist);

                // There are probably more compact ways to do this, but the txn data set is
                // usually quite small anyway, even in large histories. And most parents objects
                // will be in the set anyway. So I'm not too concerned about a few extra bytes
                // here.
                match mapped_agent {
                    Ok(mapped_agent) => {
                        // If the parent is ROOT, the parents is empty - which is handled above.
                        write_parent_diff(result, mapped_agent as usize + 2, has_more, true);
                    }
                    Err(name) => {
                        write_parent_diff(result, 1, has_more, true);
                        push_str(result, name);
                    }
                }
                // And the sequence number.
                push_usize(result, item.1);
            }
        }
    }
}

// *** Read path ***

pub(crate) fn read_parents_raw(reader: &mut BufParser, persist: bool, aa: &mut AgentAssignment, next_time: LV, read_map: &mut ReadMap) -> Result<Frontier, ParseError> {
    // println!("read parents raw {}", reader.len());
    let mut parents = SmallVec::<[LV; 2]>::new();

    loop {
        let mut n = reader.next_usize()?;
        let is_foreign = strip_bit_usize_2(&mut n);
        let has_more = strip_bit_usize_2(&mut n);

        let parent = if !is_foreign {
            let diff = n;
            // Local parents (parents inside this chunk of data) are stored using their local (file)
            // time offset.
            let file_time = next_time - diff;
            let (entry, offset) = read_map.txn_map.find_with_offset(file_time).unwrap();
            entry.1.at_offset(offset)
        } else {
            let agent = match n {
                0 => {
                    // 0 is a dummy item for empty parent lists (ie, ROOT items).
                    if has_more { return Err(ParseError::GenericInvalidData); }
                    break;
                },
                1 => {
                    // This is a foreign (unknown) item.
                    let agent_name = reader.next_str()?;
                    let agent = aa.get_or_create_agent_id(agent_name);
                    if persist {
                        read_map.agent_map.push((agent, 0));
                    }
                    agent
                }
                n => {
                    // n references a mapped agent.
                    let mapped_agent = n - 2;
                    read_map.agent_map[mapped_agent].0
                }
            };

            let seq = reader.next_usize()?;
            aa.try_agent_version_to_lv((agent, seq))
                .ok_or(ParseError::DataMissing)? // missing expected (agent, seq).
        };

        parents.push(parent);
        // debug_assert!(frontier_is_sorted(&parents));

        if !has_more { break; }
    }

    // The parents list could legitimately end up out of order due to foreign items being imported
    // in a different order from the original local order.
    //
    // This is fine - we can just re-sort.
    sort_frontier(&mut parents);

    Ok(Frontier(parents))
}

#[cfg(test)]
mod test {
    use crate::causalgraph::agent_assignment::AgentAssignment;
    use crate::encoding::bufparser::BufParser;
    use crate::encoding::map::{ReadMap, WriteMap};
    use crate::encoding::parents::{read_parents_raw, write_parents_raw};
    use crate::rle::KVPair;

    #[test]
    fn round_trip_items() {
        // We'll write each of the variants.
        let mut result = vec![];
        let mut write_map = WriteMap::new();
        let mut aa = AgentAssignment::new();
        let seph = aa.get_or_create_agent_id("seph");
        aa.assign_lv_to_client_next_seq(seph, (0..10).into());
        // Item 1: A ROOT item:
        write_parents_raw(&mut result, &[], 0, true, &mut write_map, &aa);

        // Item 2: An item with a foreign agent name. (Actually the 6 here has a known agent name):
        write_parents_raw(&mut result, &[5, 6], 10, true, &mut write_map, &aa);

        // Item 3: An item with a known agent name:
        write_parents_raw(&mut result, &[0, 1], 20, true, &mut write_map, &aa);

        // Item 4: A local item:
        write_map.insert_known((0..10).into(), 0);
        write_parents_raw(&mut result, &[3, 8], 30, true, &mut write_map, &aa);

        let mut aa_out = AgentAssignment::new();
        let george = aa_out.get_or_create_agent_id("george");
        aa_out.assign_lv_to_client_next_seq(george, (0..100).into());

        let mut read_map = ReadMap::new();

        let mut reader = BufParser(&result);

        // 1. ROOT
        let frontier = read_parents_raw(&mut reader, true, &mut aa_out, 0, &mut read_map).unwrap();
        assert!(frontier.is_root());

        // 2. Foreign agent
        let seph = aa_out.get_or_create_agent_id("seph");
        aa_out.assign_lv_to_client_next_seq(seph, (100..110).into());
        let frontier = read_parents_raw(&mut reader, true, &mut aa_out, 10, &mut read_map).unwrap();
        assert_eq!(frontier.as_ref(), &[105, 106]);

        // 3. Known agent (local changes):
        let frontier = read_parents_raw(&mut reader, true, &mut aa_out, 20, &mut read_map).unwrap();
        assert_eq!(frontier.as_ref(), &[100, 101]);

        // 4. Local changes
        read_map.txn_map.push(KVPair(0, (100..110).into()));
        let frontier = read_parents_raw(&mut reader, true, &mut aa_out, 30, &mut read_map).unwrap();
        assert_eq!(frontier.as_ref(), &[103, 108]);
    }
}