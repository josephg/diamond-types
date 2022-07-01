use std::mem::replace;
use bumpalo::Bump;
use rle::{HasLength, MergableSpan};
use crate::{AgentId, ClientData, CRDTSpan, DTRange, NewOpLog, ROOT_AGENT};
use crate::encoding::Merger;
use crate::encoding::bufparser::BufParser;
use crate::encoding::tools::push_str;
use crate::encoding::varint::*;
use bumpalo::collections::vec::Vec as BumpVec;
use bumpalo::{vec as bumpvec};

#[derive(Debug, Clone)]
pub struct AgentMapping<'a> {
    /// Map from oplog's agent ID to the agent id in the file. Paired with the last assigned agent
    /// ID, to support agent IDs bouncing around.
    map: Vec<Option<AgentId>>,
    next_mapped_agent: AgentId,
    output: BumpVec<'a, u8>,
}

impl<'a> AgentMapping<'a> {
    pub(crate) fn new(bump: &'a Bump, client_data: &[ClientData]) -> Self {
        Self {
            map: vec![None; client_data.len()],
            next_mapped_agent: 0,
            output: BumpVec::new_in(bump)
        }
    }

    pub(crate) fn map_no_root(&mut self, client_data: &[ClientData], agent: AgentId) -> AgentId {
        debug_assert_ne!(agent, ROOT_AGENT);

        let agent = agent as usize;

        self.map[agent].unwrap_or_else(|| {
            let mapped = self.next_mapped_agent;
            self.map[agent] = Some(mapped);
            push_str(&mut self.output, client_data[agent].name.as_str());
            // println!("Mapped agent {} -> {}", oplog.client_data[agent].name, mapped);
            self.next_mapped_agent += 1;
            mapped
        })
    }

    pub(crate) fn map_maybe_root(&mut self, client_data: &[ClientData], agent: AgentId) -> AgentId {
        if agent == ROOT_AGENT { 0 }
        else { self.map_no_root(client_data, agent) + 1 }
    }

    pub fn into_output(self) -> BumpVec<'a, u8> {
        self.output
    }
}

pub fn encode_agent_assignment<'a, I: Iterator<Item=CRDTSpan>>(bump: &'a Bump, iter: I, oplog: &NewOpLog, map: &mut AgentMapping) -> BumpVec<'a, u8> {
    let mut last_seq_for_agent = bumpvec![in bump; 0; oplog.client_data.len()];
    let mut result = BumpVec::new_in(bump);

    Merger::new(|span: CRDTSpan, map: &mut AgentMapping| {
        // Its rare, but possible for the agent assignment sequence to jump around a little.
        // This can happen when:
        // - The sequence numbers are shared with other documents, and hence the seqs are sparse
        // - Or the same agent made concurrent changes to multiple branches. The operations may
        //   be reordered to any order which obeys the time dag's partial order.
        let (last_seq, mapped_agent) = if span.agent == ROOT_AGENT {
            // ROOT -> agent 0.
            (0, 0)
        } else {
            debug_assert!((span.agent as usize) < last_seq_for_agent.len());
            let last_seq = replace(
                &mut last_seq_for_agent[span.agent as usize],
                span.seq_range.end
            );
            // Adding 1 here to make room for ROOT.
            let agent = map.map_no_root(&oplog.client_data, span.agent) + 1;

            (last_seq, agent)
        };

        let mut buf = [0u8; 25];
        let mut pos = 0;

        // I tried adding an extra bit field to mark len != 1 - so we can skip encoding the
        // length. But in all the data sets I've looked at, len is so rarely 1 that it increased
        // filesize.
        let delta = isize_diff(last_seq, span.seq_range.start);
        // let has_jump = self.last_seq != item.seq_range.start;

        let n = mix_bit_u32(mapped_agent, delta != 0);
        pos += encode_u32(n, &mut buf);
        pos += encode_usize(span.len(), &mut buf[pos..]);

        if delta != 0 {
            pos += encode_i64(delta as i64, &mut buf[pos..]);
        }

        result.extend_from_slice(&buf[..pos]);
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
