use rle::{HasLength, MergableSpan};
use crate::{AgentId, CRDTSpan, ROOT_AGENT};
use crate::encoding::{ParseError, RlePackCursor};
use crate::encoding::bufreader::BufReader;
use crate::encoding::varint::*;

#[derive(Debug, Clone)]
pub(super) struct AgentAssignmentCursor {
    // Its rare, but possible for the agent assignment sequence to jump around a little.
    // This can happen when:
    // - The sequence numbers are shared with other documents, and hence the seqs are sparse
    // - Or the same agent made concurrent changes to multiple branches. The operations may
    //   be reordered to any order which obeys the time dag's partial order.
    //
    // We track each agent separately, so the file size is smaller.
    last_seq_for_agent: Vec<usize>,
}

impl AgentAssignmentCursor {
    pub fn new(num_agents: usize) -> Self {
        AgentAssignmentCursor {
            last_seq_for_agent: vec![0; num_agents]
        }
    }
}

pub fn isize_diff(x: usize, y: usize) -> isize {
    // This looks awkward, but the optimizer reduces this to a simple `sub`:
    // https://rust.godbolt.org/z/Ta617dWsK
    let result = (x as i128) - (y as i128);

    debug_assert!(result <= isize::MAX as i128);
    debug_assert!(result >= isize::MIN as i128);

    result as isize
}

impl RlePackCursor for AgentAssignmentCursor {
    type Item = CRDTSpan;

    fn write_and_advance(&mut self, item: &CRDTSpan, dest: &mut Vec<u8>) {
        assert_ne!(item.agent, ROOT_AGENT);

        let agent = item.agent as usize;
        // if agent >= self.last_seq_for_agent.len() {
        //     self.last_seq_for_agent.resize_with(agent + 1, || 0);
        // }

        let mut buf = [0u8; 25];
        let mut pos = 0;

        // I tried adding an extra bit field to mark len != 1 - so we can skip encoding the
        // length. But in all the data sets I've looked at, len is so rarely 1 that it increased
        // filesize.
        let delta = isize_diff(self.last_seq_for_agent[agent], item.seq_range.start);
        // let has_jump = self.last_seq != item.seq_range.start;

        // dbg!(run);
        let n = mix_bit_u32(item.agent, delta != 0);
        pos += encode_u32(n, &mut buf);
        pos += encode_usize(item.len(), &mut buf[pos..]);

        if delta != 0 {
            pos += encode_i64(delta as i64, &mut buf[pos..]);
        }

        dest.extend_from_slice(&buf[..pos]);

        self.last_seq_for_agent[agent] = item.seq_range.end;
    }

    fn read(&mut self, reader: &mut BufReader) -> Result<Option<Self::Item>, ParseError> {
        // fn read_next_agent_assignment(&mut self, map: &mut [(AgentId, usize)]) -> Result<Option<CRDTSpan>, ParseError> {
        // Agent assignments are almost always (but not always) linear. They can have gaps, and
        // they can be reordered if the same agent ID is used to contribute to multiple branches.
        //
        // I'm still not sure if this is a good idea.

        if reader.is_empty() { return Ok(None); }

        let mut n = reader.next_usize()?;
        let has_jump = strip_bit_usize2(&mut n);
        let len = reader.next_usize()?;

        let jump = if has_jump {
            reader.next_zigzag_isize()?
        } else { 0 };

        // The agent mapping uses 0 to refer to ROOT, but no actual operations can be assigned to
        // the root agent.
        // if n == 0 {
        //     return Err(ParseError::InvalidLength);
        // }

        // let inner_agent = n - 1;
        let inner_agent = n;
        if inner_agent >= map.len() {
            return Err(ParseError::InvalidLength);
        }

        let entry = &mut map[inner_agent];
        let agent = entry.0;

        // TODO: Error if this overflows.
        let start = (entry.1 as isize + jump) as usize;
        let end = start + len;
        entry.1 = end;

        Ok(Some(CRDTSpan {
            agent,
            seq_range: (start..end).into(),
        }))
    }
}
