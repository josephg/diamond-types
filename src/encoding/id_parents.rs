use rle::{HasLength, MergableSpan};
use crate::{CausalGraph, CRDTSpan, DTRange, KVPair, LocalVersion, ROOT_AGENT, Time};
use crate::causalgraph::ClientData;
use crate::encoding::agent_assignment::{AgentMappingDec, AgentMappingEnc, AgentStrToId, isize_try_add};
use crate::encoding::parents::{read_parents_raw, TxnMap, write_parents_raw};
use crate::encoding::tools::{push_str, push_u32, push_u64, push_usize};
use crate::encoding::varint::{mix_bit_u32, mix_bit_usize, num_encode_zigzag_i64, strip_bit_usize2};
use bumpalo::collections::vec::Vec as BumpVec;
use smallvec::smallvec;
use crate::encoding::bufparser::BufParser;
use crate::encoding::parseerror::ParseError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IDParents {
    pub start: Time,
    pub parents: LocalVersion,
    pub span: CRDTSpan,
}

impl Default for IDParents {
    fn default() -> Self {
        IDParents {
            start: 0,
            parents: Default::default(),
            span: CRDTSpan {
                agent: 0,
                seq_range: (0..0).into()
            }
        }
    }
}

impl HasLength for IDParents {
    fn len(&self) -> usize {
        self.span.len()
    }
}

impl MergableSpan for IDParents {
    fn can_append(&self, other: &Self) -> bool {
        let end = self.start + self.len();
        (end == other.start)
            && other.parents_are_trivial()
            && self.span.can_append(&other.span)
    }

    fn append(&mut self, other: Self) {
        self.span.append(other.span)
        // Other parents don't matter.
    }
}

impl IDParents {
    fn parents_are_trivial(&self) -> bool {
        self.parents.len() == 1
            && self.parents[0] == self.start - 1
    }

    pub fn time_span(&self) -> DTRange {
        (self.start..self.start + self.len()).into()
    }

    pub fn clear(&mut self) {
        self.span.seq_range.clear()
    }
}


pub(crate) fn write_id_parents(result: &mut BumpVec<u8>, data: &IDParents,
                               txn_map: &mut TxnMap, agent_map: &mut AgentMappingEnc,
                               persist: bool, cg: &CausalGraph) {
    assert_ne!(data.span.agent, ROOT_AGENT, "Cannot assign operations to ROOT");
    let len = data.len();

    // We only write the parents info if parents is non-trivial.
    let write_parents = !data.parents_are_trivial();

    // Its rare, but possible for the agent assignment sequence to jump around a little.
    // This can happen when:
    // - The sequence numbers are shared with other documents, and hence the seqs are sparse
    // - Or the same agent made concurrent changes to multiple branches. The operations may
    //   be reordered to any order which obeys the time dag's partial order.

    let mapped_agent = agent_map.map_no_root(&cg.client_data, data.span.agent, persist);
    let delta = agent_map.seq_delta(data.span.agent, data.span.seq_range, persist);

    // I tried adding an extra bit field to mark len != 1 - so we can skip encoding the
    // length. But in all the data sets I've looked at, len is so rarely 1 that it increased
    // filesize.
    let has_jump = delta != 0;

    let mut write_n = |mapped_agent: u32, is_known: bool| {
        let mut n = mix_bit_u32(mapped_agent, has_jump);
        n = mix_bit_u32(n, is_known);
        n = mix_bit_u32(n, write_parents);
        push_u32(result, n);
    };

    match mapped_agent {
        Ok(mapped_agent) => {
            // Agent is already known in the file. Just use its mapped ID.
            write_n(mapped_agent as u32, true);
        }
        Err(name) => {
            write_n(0, false);
            push_str(result, name);
        }
    }

    push_usize(result, len);

    if has_jump {
        push_u64(result, num_encode_zigzag_i64(delta as i64));
    }


    // And parents stuff.

    let next_output_time = txn_map.last().map_or(0, |last| last.1.end);
    let output_range = (next_output_time .. next_output_time + len).into();


    // NOTE: we're using .insert instead of .push here so the txn_map stays in the expected order!
    if persist {
        txn_map.insert(KVPair(data.start, output_range));
    }

    // Write the parents, if it makes sense to do so.
    if write_parents {
        write_parents_raw(result, &data.parents, next_output_time, persist, agent_map, txn_map, cg);
    }
}

pub(crate) fn read_id_p(reader: &mut BufParser, persist: bool, cg: &mut CausalGraph, next_time: Time, agent_map: &mut AgentMappingDec) -> Result<IDParents, ParseError> {
    // First we have agent assignment, then optional parents.
    debug_assert_eq!(next_time, cg.len());

    // Bits are:
    // has_parents
    // is_known
    // delta != 0 (has_jump)
    // (mapped agent)

    // dbg!(reader.0);
    let mut n = reader.next_usize()?;

    let has_parents = strip_bit_usize2(&mut n);
    let is_known = strip_bit_usize2(&mut n);
    let has_jump = strip_bit_usize2(&mut n);
    let mapped_agent = n;

    let (agent, last_seq, idx) = if !is_known {
        if mapped_agent != 0 { return Err(ParseError::GenericInvalidData); }
        let agent_name = reader.next_str()?;
        let agent = cg.get_or_create_agent_id(agent_name);
        let idx = agent_map.len();
        if persist {
            agent_map.push((agent, 0));
        }
        (agent, 0, idx)
    } else {
        let entry = agent_map[mapped_agent];
        (entry.0, entry.1, mapped_agent)
    };

    let len = reader.next_usize()?;

    let jump = if has_jump {
        reader.next_zigzag_isize()?
    } else { 0 };

    let start = isize_try_add(last_seq, jump)
        .ok_or(ParseError::GenericInvalidData)?;
    let end = start + len;

    if persist {
        agent_map[idx].1 = end;
    }

    let parents = if has_parents {
        read_parents_raw(reader, persist, cg, next_time, agent_map)?
    } else {
        smallvec![next_time - 1]
    };

    Ok(IDParents {
        start: next_time,
        parents,
        span: CRDTSpan {
            agent,
            seq_range: (start..end).into(),
        }
    })
}