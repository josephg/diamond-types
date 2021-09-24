use diamond_core::CRDTId;
use rle::SplitableSpan;

use content_tree::ContentLength;
use rle::Searchable;

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct CRDTSpan {
    // TODO: Consider changing to agent / range of sequences.
    pub loc: CRDTId,
    pub len: u32,
}

impl Searchable for CRDTSpan {
    type Item = CRDTId;

    fn contains(&self, loc: CRDTId) -> Option<usize> {
        // let r = self.loc.seq .. self.loc.seq + (self.len.abs() as usize);
        // self.loc.agent == loc.agent && entry.get_seq_range().contains(&loc.seq)
        if self.loc.agent == loc.agent
            && loc.seq >= self.loc.seq
            && loc.seq < self.loc.seq + self.len {
            Some((loc.seq - self.loc.seq) as usize)
        } else { None }
    }

    fn at_offset(&self, offset: usize) -> CRDTId {
        assert!(offset < self.len());
        CRDTId {
            agent: self.loc.agent,
            seq: self.loc.seq + offset as u32
        }
    }
}

impl ContentLength for CRDTSpan {
    fn content_len(&self) -> usize {
        self.len as _
    }

    fn content_len_at_offset(&self, offset: usize) -> usize {
        offset
    }
}

impl SplitableSpan for CRDTSpan {
    /// this length refers to the length that we'll use when we call truncate(). So this does count
    /// deletes.
    fn len(&self) -> usize {
        self.len as _
    }

    fn truncate(&mut self, at: usize) -> Self {
        let at = at as u32;
        debug_assert!(at < self.len);

        let other = CRDTSpan {
            loc: CRDTId {
                agent: self.loc.agent,
                seq: self.loc.seq + at,
            },
            len: self.len - at
        };

        self.len = at;

        other
    }

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let at = at as u32;
        let other = CRDTSpan {
            loc: self.loc,
            len: at
        };
        self.loc.seq += at;
        self.len -= at;
        other
    }

    fn can_append(&self, other: &Self) -> bool {
        other.loc.agent == self.loc.agent
            && other.loc.seq == self.loc.seq + self.len
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        self.loc.seq = other.loc.seq;
        self.len += other.len;
    }
}
