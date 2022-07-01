use std::mem::replace;
use bumpalo::Bump;
use rle::{HasLength, MergableSpan};
use crate::{AgentId, ClientData, CRDTSpan, DTRange, NewOpLog, ROOT_AGENT};
use crate::encoding::Merger;
use crate::encoding::bufparser::BufParser;
use crate::encoding::tools::{push_str, push_u32, push_u64, push_usize};
use crate::encoding::varint::*;
use bumpalo::collections::vec::Vec as BumpVec;
use bumpalo::{vec as bumpvec};

#[derive(Debug, Clone)]
pub struct AgentMapping {
    /// Map from oplog's agent ID to the agent id in the file. Paired with the last assigned agent
    /// ID, to support agent IDs bouncing around.
    map: Vec<Option<AgentId>>,
    next_mapped_agent: AgentId,
    // output: BumpVec<'a, u8>,
}

impl AgentMapping {
    pub(crate) fn new(client_data: &[ClientData]) -> Self {
        Self {
            map: vec![None; client_data.len()],
            next_mapped_agent: 0,
            // output: BumpVec::new_in(bump)
        }
    }

    pub(crate) fn map_no_root<'c>(&mut self, client_data: &'c [ClientData], agent: AgentId, persist: bool) -> Result<AgentId, &'c str> {
        debug_assert_ne!(agent, ROOT_AGENT);

        let agent = agent as usize;

        // More agents might get added later.
        while agent >= self.map.len() {
            self.map.push(None);
        }

        self.map[agent].ok_or_else(|| {
            // We'll quietly map it internally, but still return None because the caller needs to
            // know to write the name itself to the file.
            let mapped = self.next_mapped_agent;

            if persist {
                self.map[agent] = Some(mapped);
                // println!("Mapped agent {} -> {}", oplog.client_data[agent].name, mapped);
                self.next_mapped_agent += 1;
            }

            client_data[agent].name.as_str()
        })
    }

    pub(crate) fn map_maybe_root<'c>(&mut self, client_data: &'c [ClientData], agent: AgentId, persist: bool) -> Result<AgentId, &'c str> {
        if agent == ROOT_AGENT { Ok(0) }
        else { self.map_no_root(client_data, agent, persist).map(|a| a + 1) }
    }
}

// pub(crate) type LastSeqForAgent<'a> = BumpVec<'a, usize>;
pub(crate) type LastSeqForAgent = Vec<usize>;

pub(crate) fn write_agent_assignment_span(result: &mut BumpVec<u8>, tag: bool, span: CRDTSpan,
                                          agent_map: &mut AgentMapping, last_seq_for_agent: &mut LastSeqForAgent,
                                          persist: bool, client_data: &[ClientData]) {
    // Its rare, but possible for the agent assignment sequence to jump around a little.
    // This can happen when:
    // - The sequence numbers are shared with other documents, and hence the seqs are sparse
    // - Or the same agent made concurrent changes to multiple branches. The operations may
    //   be reordered to any order which obeys the time dag's partial order.
    assert_ne!(span.agent, ROOT_AGENT, "Cannot assign operations to ROOT");

    while (span.agent as usize) >= last_seq_for_agent.len() {
        // I'm sure there's a prettier way to do this but eh. Calling extend(repeat(0).take(...))
        // is longer and generates more output code.
        last_seq_for_agent.push(0);
    }

    // debug_assert!((span.agent as usize) < last_seq_for_agent.len());
    let last_seq = replace(
        &mut last_seq_for_agent[span.agent as usize],
        span.seq_range.end
    );
    // Adding 1 here to make room for ROOT.
    let mapped_agent = agent_map.map_no_root(client_data, span.agent, persist);

    // I tried adding an extra bit field to mark len != 1 - so we can skip encoding the
    // length. But in all the data sets I've looked at, len is so rarely 1 that it increased
    // filesize.
    let delta = isize_diff(last_seq, span.seq_range.start);
    // let has_jump = self.last_seq != item.seq_range.start;

    let mut write_n = |mapped_agent: u32, is_known: bool| {
        let mut n = mix_bit_u32(mapped_agent, delta != 0);
        n = mix_bit_u32(n, is_known);
        if tag {
            n = mix_bit_u32(n, true);
        }
        push_u32(result, n);
        // pos += encode_u32(n, &mut buf);
    };

    match mapped_agent {
        Ok(mapped_agent) => {
            // Agent is already known in the file. Just use its mapped ID.
            write_n(mapped_agent as u32, false);
        }
        Err(name) => {
            write_n(0, true);
            push_str(result, name);
        }
    }

    // pos += encode_usize(span.len(), &mut buf[pos..]);
    push_usize(result, span.len());

    if delta != 0 {
        push_u64(result, num_encode_zigzag_i64(delta as i64));
    }
}

pub(crate) fn encode_agent_assignment<'a, I: Iterator<Item=CRDTSpan>>(bump: &'a Bump, iter: I, client_data: &[ClientData], map: &mut AgentMapping) -> BumpVec<'a, u8> {
    // let mut last_seq_for_agent: LastSeqForAgent = bumpvec![in bump; 0; client_data.len()];
    let mut last_seq_for_agent: LastSeqForAgent = vec![0; client_data.len()];
    let mut result = BumpVec::new_in(bump);

    Merger::new(|span: CRDTSpan, map: &mut AgentMapping| {
        write_agent_assignment_span(&mut result, false, span, map, &mut last_seq_for_agent, true, client_data);
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

// impl RlePackReadCursor for AgentAssignmentCursor {
//     type Item = CRDTSpan;
//
//     fn read(&mut self, reader: &mut BufReader) -> Result<Option<Self::Item>, ParseError> {
//         // fn read_next_agent_assignment(&mut self, map: &mut [(AgentId, usize)]) -> Result<Option<CRDTSpan>, ParseError> {
//         // Agent assignments are almost always (but not always) linear. They can have gaps, and
//         // they can be reordered if the same agent ID is used to contribute to multiple branches.
//         //
//         // I'm still not sure if this is a good idea.
//
//         if reader.is_empty() { return Ok(None); }
//
//         let mut n = reader.next_usize()?;
//         let has_jump = strip_bit_usize2(&mut n);
//         let len = reader.next_usize()?;
//
//         let jump = if has_jump {
//             reader.next_zigzag_isize()?
//         } else { 0 };
//
//         // The agent mapping uses 0 to refer to ROOT, but no actual operations can be assigned to
//         // the root agent.
//         // if n == 0 {
//         //     return Err(ParseError::InvalidLength);
//         // }
//
//         // let inner_agent = n - 1;
//         let inner_agent = n;
//         if inner_agent >= map.len() {
//             return Err(ParseError::InvalidLength);
//         }
//
//         let entry = &mut map[inner_agent];
//         let agent = entry.0;
//
//         // TODO: Error if this overflows.
//         let start = (entry.1 as isize + jump) as usize;
//         let end = start + len;
//         entry.1 = end;
//
//         Ok(Some(CRDTSpan {
//             agent,
//             seq_range: (start..end).into(),
//         }))
//     }
// }
