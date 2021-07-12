use crate::common::{CRDTLocation, CLIENT_INVALID, IndexGet};
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;

// TODO: Consider renaming this "RangeEntry" or something.
pub trait EntryTraits: SplitableSpan + Copy + Debug + PartialEq + Eq + Sized + Default {
    type Item: Copy + Debug;

    // This is strictly unnecessary given truncate(), but it makes some code cleaner.
    fn truncate_keeping_right(&mut self, at: usize) -> Self;

    /// Checks if the entry contains the specified item. If it does, returns the offset into the
    /// item.
    fn contains(&self, loc: Self::Item) -> Option<usize>;
    fn is_valid(&self) -> bool;

    // I'd use Index for this but the index trait returns a reference.
    // fn at_offset(&self, offset: usize) -> Self::Item;
    fn at_offset(&self, offset: usize) -> Self::Item;
}

pub trait EntryWithContent {
    /// User specific content length. Used by range_tree for character counts.
    fn content_len(&self) -> usize;
}

impl<T: EntryTraits> IndexGet<usize> for T {
    type Output = T::Item;

    fn index_get(&self, index: usize) -> Self::Output {
        self.at_offset(index)
    }
}

pub trait CRDTItem {
    fn is_insert(&self) -> bool;
    fn is_delete(&self) -> bool {
        !self.is_insert()
    }
    fn mark_deleted(&mut self);
}

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct CRDTSpan {
    pub loc: CRDTLocation,
    pub len: u32,
}

impl EntryTraits for CRDTSpan {
    type Item = CRDTLocation;

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

    fn contains(&self, loc: CRDTLocation) -> Option<usize> {
        // let r = self.loc.seq .. self.loc.seq + (self.len.abs() as usize);
        // self.loc.agent == loc.agent && entry.get_seq_range().contains(&loc.seq)
        if self.loc.agent == loc.agent
            && loc.seq >= self.loc.seq
            && loc.seq < self.loc.seq + self.len {
            Some((loc.seq - self.loc.seq) as usize)
        } else { None }
    }

    fn is_valid(&self) -> bool {
        self.loc.agent != CLIENT_INVALID
    }

    fn at_offset(&self, offset: usize) -> CRDTLocation {
        assert!(offset < self.len());
        CRDTLocation {
            agent: self.loc.agent,
            seq: self.loc.seq + offset as u32
        }
    }
}

impl EntryWithContent for CRDTSpan {
    fn content_len(&self) -> usize {
        self.len as _
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
            loc: CRDTLocation {
                agent: self.loc.agent,
                seq: self.loc.seq + at,
            },
            len: self.len - at
        };

        self.len = at;

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
