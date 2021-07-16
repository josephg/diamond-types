use crate::splitable_span::SplitableSpan;
use crate::rle::{Rle, KVPair};
use crate::list::Order;
// use crate::range_tree::EntryTraits;

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

impl Rle<KVPair<DoubleDelete>> {
    pub fn increment_delete_range(&mut self, base: Order, len: u32) {
        debug_assert!(len > 0);
        let mut next_entry = KVPair(base, DoubleDelete { len, excess_deletes: 1 });

        let start = self.search(base);
        let mut idx = start.unwrap_or_else(|idx| idx);
        loop {
            // There's essentially 2 cases here: We're in a gap, or we landed in an existing entry.
            // The gap case is most common. We'll handle the gap, then flow to handle the modify
            // case each iteration.

            if idx == self.0.len() || self.0[idx].0 > base {
                // We're in a gap. Insert as much as we can here.
                let mut this_entry = next_entry;
                let done_here = if idx < self.0.len() && next_entry.end() > self.0[idx].0 {
                    // The gap isn't big enough.
                    next_entry = this_entry.truncate((self.0[idx].0 - this_entry.0) as usize);
                    false
                } else {
                    // Plenty of room.
                    true
                };

                if idx >= 1 && self.0[idx - 1].can_append(&this_entry) {
                    &mut self.0[idx - 1].append(this_entry);
                } else {
                    // Insert here.
                    self.0.insert(idx, this_entry);
                    idx += 1;
                }

                if done_here { break; }
            }

            // Ok we still have stuff to increment, and we're inside an entry now.
            let entry = &mut self.0[idx];
            debug_assert!(entry.0 <= next_entry.0);
            debug_assert!(next_entry.0 < entry.end());

            if entry.0 < next_entry.0 {
                // Split into 2 entries. This approach will result in more memcpys but it shouldn't
                // matter much in practice.
                let remainder = entry.truncate((next_entry.0 - entry.0) as usize);
                idx += 1;
                self.0.insert(idx, remainder);
            }

            let entry = &mut self.0[idx];
            debug_assert!(entry.0 == next_entry.0);

            if entry.len() <= next_entry.len() {
                entry.1.excess_deletes += 1;
                next_entry.0 += entry.1.len;
                next_entry.1.len -= entry.1.len;
                if next_entry.1.len == 0 { break; }
                idx += 1;
            } else {
                // entry.len > next_entry.len. Split entry into 2 parts, increment excess_deletes
                // and we're done.
                let remainder = entry.truncate(next_entry.len());
                entry.1.excess_deletes += 1;
                self.0.insert(idx + 1, remainder);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::rle::{Rle, KVPair};
    use crate::list::double_delete::DoubleDelete;

    #[test]
    fn inc_delete_range() {
        // This isn't completely exhaustive, but its pretty robust.
        // It'd be nice to do coverage reporting on this.
        let mut deletes: Rle<KVPair<DoubleDelete>> = Rle::new();
        deletes.increment_delete_range(5, 3);
        assert_eq!(deletes.0, vec![KVPair(5, DoubleDelete { len: 3, excess_deletes: 1 })]);
        deletes.increment_delete_range(5, 3);
        assert_eq!(deletes.0, vec![KVPair(5, DoubleDelete { len: 3, excess_deletes: 2 })]);
        deletes.increment_delete_range(4, 2);
        assert_eq!(deletes.0, vec![
            KVPair(4, DoubleDelete { len: 1, excess_deletes: 1 }),
            KVPair(5, DoubleDelete { len: 1, excess_deletes: 3 }),
            KVPair(6, DoubleDelete { len: 2, excess_deletes: 2 }),
        ]);
        deletes.increment_delete_range(7, 3);
        assert_eq!(deletes.0, vec![
            KVPair(4, DoubleDelete { len: 1, excess_deletes: 1 }),
            KVPair(5, DoubleDelete { len: 1, excess_deletes: 3 }),
            KVPair(6, DoubleDelete { len: 1, excess_deletes: 2 }),
            KVPair(7, DoubleDelete { len: 1, excess_deletes: 3 }),
            KVPair(8, DoubleDelete { len: 2, excess_deletes: 1 }),
        ]);

        // dbg!(&deletes);
    }
}