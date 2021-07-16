use crate::splitable_span::SplitableSpan;

/// Sometimes the same item is removed by multiple peers. This is really rare, but necessary to
/// track for correctness when we're activating and deactivating entries.

/// "Double" delete entries can track any number of duplicate deletes to the same entry.

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct DoubleDelete {
    pub len: u32,
    pub excess_deletes: u32, // u16 would do but it doesn't matter - we'll pad out anyway.
}

impl SplitableSpan for DoubleDelete {
    fn len(&self) -> usize {
        self.len as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let trimmed = DoubleDelete {
            // order: self.order + at as _,
            len: self.len - at as u32,
            excess_deletes: self.excess_deletes
        };
        self.len = at as u32;
        trimmed
    }

    fn can_append(&self, other: &Self) -> bool {
        other.excess_deletes == self.excess_deletes
    }

    fn append(&mut self, other: Self) { self.len += other.len; }
    fn prepend(&mut self, other: Self) { self.len += other.len; }
}