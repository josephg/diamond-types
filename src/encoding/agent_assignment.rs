use std::mem::replace;
use bumpalo::Bump;
use rle::{HasLength, MergableSpan};
use crate::{AgentId, CausalGraph, CRDTSpan, DTRange, ROOT_AGENT};
use crate::encoding::Merger;
use crate::encoding::bufparser::BufParser;
use crate::encoding::tools::{push_str, push_u32, push_u64, push_usize};
use crate::encoding::varint::*;
use bumpalo::collections::vec::Vec as BumpVec;
use bumpalo::{vec as bumpvec};
use crate::causalgraph::ClientData;
use crate::encoding::parseerror::ParseError;

#[derive(Debug, Clone)]
pub struct AgentMappingEnc {
    /// Map from oplog's agent ID to the agent id in the file. Paired with the last assigned agent
    /// ID, to support agent IDs bouncing around.
    map: Vec<(Option<AgentId>, usize)>,
    next_mapped_agent: AgentId,
    // output: BumpVec<'a, u8>,
}

impl AgentMappingEnc {
    pub(crate) fn new(client_data: &[ClientData]) -> Self {
        Self {
            map: vec![(None, 0); client_data.len()],
            next_mapped_agent: 0,
            // output: BumpVec::new_in(bump)
        }
    }

    fn ensure_capacity(&mut self, cap: usize) {
        // There's probably nicer ways to implement this.
        if cap > self.map.len() {
            self.map.resize(cap, (None, 0));
        }
    }

    pub(crate) fn populate_from_dec(&mut self, dec: &AgentMappingDec) {
        self.next_mapped_agent = dec.len() as AgentId;
        for (mapped_agent, (agent, last)) in dec.iter().enumerate() {
            self.ensure_capacity(*agent as usize + 1);
            self.map[*agent as usize] = (Some(mapped_agent as AgentId), *last);
        }
    }

    pub(crate) fn map_no_root<'c>(&mut self, client_data: &'c [ClientData], agent: AgentId, persist: bool) -> Result<AgentId, &'c str> {
        debug_assert_ne!(agent, ROOT_AGENT);

        let agent = agent as usize;
        self.ensure_capacity(agent + 1);

        self.map[agent].0.ok_or_else(|| {
            // We'll quietly map it internally, but still return None because the caller needs to
            // know to write the name itself to the file.
            let mapped = self.next_mapped_agent;

            if persist {
                self.map[agent] = (Some(mapped), 0);
                // println!("Mapped agent {} -> {}", oplog.client_data[agent].name, mapped);
                self.next_mapped_agent += 1;
            }

            client_data[agent].name.as_str()
        })
    }

    fn seq_delta(&mut self, agent: AgentId, span: DTRange, persist: bool) -> isize {
        let agent = agent as usize;
        self.ensure_capacity(agent + 1);

        let item = &mut self.map[agent].1;
        let old_seq = *item;

        if persist {
            *item = span.end;
        }

        isize_diff(span.start, old_seq)
    }

    pub(crate) fn map_maybe_root<'c>(&mut self, client_data: &'c [ClientData], agent: AgentId, persist: bool) -> Result<AgentId, &'c str> {
        if agent == ROOT_AGENT { Ok(0) }
        else { self.map_no_root(client_data, agent, persist).map(|a| a + 1) }
    }
}

pub(crate) fn write_agent_assignment_span(result: &mut BumpVec<u8>, mut tag: Option<bool>, span: CRDTSpan,
                                          agent_map: &mut AgentMappingEnc, persist: bool, client_data: &[ClientData]) {
    // let s = result.len();

    // Its rare, but possible for the agent assignment sequence to jump around a little.
    // This can happen when:
    // - The sequence numbers are shared with other documents, and hence the seqs are sparse
    // - Or the same agent made concurrent changes to multiple branches. The operations may
    //   be reordered to any order which obeys the time dag's partial order.
    assert_ne!(span.agent, ROOT_AGENT, "Cannot assign operations to ROOT");

    // debug_assert!((span.agent as usize) < last_seq_for_agent.len());
    // Adding 1 here to make room for ROOT.
    let mapped_agent = agent_map.map_no_root(client_data, span.agent, persist);
    let delta = agent_map.seq_delta(span.agent, span.seq_range, persist);

    // I tried adding an extra bit field to mark len != 1 - so we can skip encoding the
    // length. But in all the data sets I've looked at, len is so rarely 1 that it increased
    // filesize.
    let has_jump = delta != 0;

    let mut write_n = |mapped_agent: u32, is_known: bool| {
        let mut n = mix_bit_u32(mapped_agent, has_jump);
        n = mix_bit_u32(n, is_known);
        if let Some(tag) = tag.take() {
            n = mix_bit_u32(n, tag);
        }
        push_u32(result, n);
        // pos += encode_u32(n, &mut buf);
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

    // pos += encode_usize(span.len(), &mut buf[pos..]);
    push_usize(result, span.len());

    if has_jump {
        push_u64(result, num_encode_zigzag_i64(delta as i64));
    }

    // dbg!(&result[s..]);
}

pub(crate) fn encode_agent_assignment<'a, I: Iterator<Item=CRDTSpan>>(bump: &'a Bump, iter: I, client_data: &[ClientData], map: &mut AgentMappingEnc) -> BumpVec<'a, u8> {
    // let mut last_seq_for_agent: LastSeqForAgent = bumpvec![in bump; 0; client_data.len()];
    let mut result = BumpVec::new_in(bump);

    Merger::new(|span: CRDTSpan, map: &mut AgentMappingEnc| {
        write_agent_assignment_span(&mut result, None, span, map, true, client_data);
    }).flush_iter2(iter, map);

    result
}

pub fn isize_diff(x: usize, y: usize) -> isize {
    // This looks awkward, but the optimizer reduces this to a simple `sub`:
    // https://rust.godbolt.org/z/Ta617dWsK
    let result = (x as i128) - (y as i128);

    debug_assert!(result <= isize::MAX as i128);
    debug_assert!(result >= isize::MIN as i128);

    result as isize
}

pub fn isize_try_add(x: usize, y: isize) -> Option<usize> {
    let result = (x as i128) + (y as i128);

    if result < 0 || result > usize::MAX as i128 { None }
    else { Some(result as usize) }
}

/// Map from file's mapped ID -> internal ID, and the last seq we've seen.
pub type AgentMappingDec = Vec<(AgentId, usize)>;

pub(crate) trait AgentStrToId {
    fn get_or_create_agent_id(&mut self, name: &str) -> AgentId;
}

impl AgentStrToId for CausalGraph {
    fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.get_or_create_agent_id(name)
    }
}

fn push_and_ref<V>(vec: &mut Vec<V>, new_val: V) -> &mut V {
    let len = vec.len();
    vec.push(new_val);
    unsafe {
        vec.get_unchecked_mut(len)
    }
}

pub(crate) fn read_agent_assignment<M: AgentStrToId>(reader: &mut BufParser, tagged: bool, persist: bool, cg: &mut M, map: &mut AgentMappingDec) -> Result<CRDTSpan, ParseError> {
    // fn read_next_agent_assignment(&mut self, map: &mut [(AgentId, usize)]) -> Result<Option<CRDTSpan>, ParseError> {
    // Agent assignments are almost always (but not always) linear. They can have gaps, and
    // they can be reordered if the same agent ID is used to contribute to multiple branches.
    //
    // I'm still not sure if this is a good idea.

    // if reader.is_empty() { return Ok(None); }
    // if reader.is_empty() { return Err(ParseError::UnexpectedEOF); }

    // Bits are:
    // optional tag
    // is_known
    // delta != 0 (has_jump)
    // (mapped agent)

    // dbg!(reader.0);
    let mut n = reader.next_usize()?;
    if tagged {
        // Ditch the tag.
        strip_bit_usize2(&mut n);
    }

    let is_known = strip_bit_usize2(&mut n);
    let has_jump = strip_bit_usize2(&mut n);
    let mapped_agent = n;

    let (agent, last_seq, idx) = if !is_known {
        if mapped_agent != 0 { return Err(ParseError::GenericInvalidData); }
        let agent_name = reader.next_str()?;
        let agent = cg.get_or_create_agent_id(agent_name);
        let idx = map.len();
        if persist {
            map.push((agent, 0));
        }
        (agent, 0, idx)
    } else {
        let entry = map[mapped_agent];
        (entry.0, entry.1, mapped_agent)
    };

    let len = reader.next_usize()?;

    let jump = if has_jump {
        reader.next_zigzag_isize()?
    } else { 0 };
    dbg!(jump);

    let start = isize_try_add(last_seq, jump)
        .ok_or(ParseError::GenericInvalidData)?;
    let end = start + len;

    if persist {
        map[idx].1 = end;
    }

    Ok(CRDTSpan {
        agent,
        seq_range: (start..end).into(),
    })
}
