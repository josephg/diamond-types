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
pub struct Entry {
    pub loc: CRDTLocation,
    pub len: i32, // negative if the chunk was deleted. Never 0 - TODO: could use NonZeroI32
}

impl EntryTraits for Entry {
    type Item = CRDTLocation;

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let other = Entry {
            loc: self.loc,
            len: at as i32 * self.len.signum()
        };
        self.loc.seq += at as u32;
        self.len += if self.len < 0 { at as i32 } else { -(at as i32) };
        other
    }

    fn contains(&self, loc: CRDTLocation) -> Option<usize> {
        // let r = self.loc.seq .. self.loc.seq + (self.len.abs() as usize);
        // self.loc.agent == loc.agent && entry.get_seq_range().contains(&loc.seq)
        if self.loc.agent == loc.agent
            && loc.seq >= self.loc.seq
            && loc.seq < self.loc.seq + self.len() as u32 {
            Some((loc.seq - self.loc.seq) as usize)
        } else { None }
    }

    fn is_valid(&self) -> bool {
        self.loc.agent != CLIENT_INVALID
    }

    // fn at_offset(&self, offset: usize) -> Self::Item {
    fn at_offset(&self, offset: usize) -> CRDTLocation {
        assert!(offset < self.len());
        CRDTLocation {
            agent: self.loc.agent,
            // seq: if self.len < 0 { self.loc.seq - self.len } else { self.loc.seq + self.len }
            // So gross.
            seq: (self.loc.seq as i32 + (offset as i32 * self.len.signum())) as u32
        }
    }
}

impl EntryWithContent for Entry {
    fn content_len(&self) -> usize {
        if self.len < 0 { 0 } else { self.len as _ }
    }
}

impl SplitableSpan for Entry {
    /// this length refers to the length that we'll use when we call truncate(). So this does count
    /// deletes.
    fn len(&self) -> usize {
        self.len.abs() as _
    }

    fn truncate(&mut self, at: usize) -> Self {
        debug_assert!(at < self.len());

        let sign = self.len.signum();

        let other = Entry {
            loc: CRDTLocation {
                agent: self.loc.agent,
                seq: self.loc.seq + at as u32,
            },
            len: self.len - (at as i32) * sign
        };

        self.len = at as i32 * sign;

        other
    }

    fn can_append(&self, other: &Self) -> bool {
        other.loc.agent == self.loc.agent
            && self.is_insert() == other.is_insert()
            && other.loc.seq == self.loc.seq + self.len() as u32
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        self.loc.seq = other.loc.seq;
        self.len += other.len;
    }
}


impl CRDTItem for Entry {
    fn is_insert(&self) -> bool {
        debug_assert!(self.len != 0);
        self.len > 0
    }

    fn mark_deleted(&mut self) {
        debug_assert!(self.is_insert());
        self.len = -self.len
    }
}