use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanHelpers};
use crate::rle::{RleVec, KVPair, RleSpanHelpers};
use crate::list::LV;
use crate::order::TimeSpan;

/// Sometimes the same item is removed by multiple peers. This is really rare, but necessary to
/// track for correctness when we're activating and deactivating entries.
///
/// "Double" delete entries can track any number of duplicate deletes to the same entry.
#[derive(Default, Copy, Clone, Debug, Eq, PartialEq)]
pub struct DoubleDelete {
    pub len: u32,
    pub excess_deletes: u32, // u16 would do but it doesn't matter - we'll pad out anyway.
}

impl HasLength for DoubleDelete {
    fn len(&self) -> usize {
        self.len as usize
    }
}
impl SplitableSpanHelpers for DoubleDelete {
    fn truncate_h(&mut self, at: usize) -> Self {
        let trimmed = DoubleDelete {
            // order: self.order + at as _,
            len: self.len - at as u32,
            excess_deletes: self.excess_deletes
        };
        self.len = at as u32;
        trimmed
    }
}
impl MergableSpan for DoubleDelete {
    fn can_append(&self, other: &Self) -> bool {
        other.excess_deletes == self.excess_deletes
    }

    fn append(&mut self, other: Self) { self.len += other.len; }
    fn prepend(&mut self, other: Self) { self.len += other.len; }
}

impl RleVec<KVPair<DoubleDelete>> {
    // TODO: Consider changing all these methods to take an OrderSpan instead.

    /// Internal function to add / subtract from a range of double deleted entries.
    /// Returns the number of items modified.
    ///
    /// Idx must be the index where the target item should be, as is returned by search.
    pub(crate) fn modify_delete_range_idx(&mut self, base: LV, len: u32, mut idx: usize, update_by: i32, max_value: u32) -> u32 {
        debug_assert!(len > 0);
        debug_assert_ne!(update_by, 0);
        debug_assert_eq!(update_by.abs(), 1);

        let mut modified = 0;

        let mut next_span = TimeSpan { start: base, len };
        // let mut next_entry = KVPair(base, DoubleDelete { len, excess_deletes: 1 });

        loop {
            debug_assert!(next_span.len > 0);

            // There's essentially 2 cases here: We're in a gap, or we landed in an existing entry.
            // The gap case is most common when incrementing, but when decrementing we'll *only*
            // land on entries. We'll handle the gap, then flow to handle the modify case each
            // iteration.
            if idx == self.0.len() || self.0[idx].0 > next_span.start {
                if update_by < 0 { break; }
                // We're in a gap.
                // let mut this_span = next_entry;
                let (done_here, this_entry) = if idx < self.0.len() && next_span.end() > self.0[idx].0 {
                    // The gap isn't big enough.
                    let this_span = next_span.truncate_keeping_right((self.0[idx].0 - next_span.start) as usize);
                    (false, KVPair(this_span.start, DoubleDelete {
                        len: this_span.len,
                        excess_deletes: update_by as u32
                    }))
                } else {
                    // Plenty of room.
                    (true, KVPair(next_span.start, DoubleDelete {
                        len: next_span.len,
                        excess_deletes: update_by as u32
                    }))
                };

                modified += this_entry.1.len;

                if idx >= 1 && self.0[idx - 1].can_append(&this_entry) {
                    self.0[idx - 1].append(this_entry);
                } else {
                    // Insert here.
                    self.0.insert(idx, this_entry);
                    idx += 1;
                }

                if done_here { break; }
            }

            // Ok we still have stuff to increment, and we're inside an entry now.
            let entry = &mut self.0[idx];
            debug_assert!(entry.0 <= next_span.start);
            debug_assert!(next_span.start < entry.end() as u32);

            if entry.0 < next_span.start {
                // Split into 2 entries. This approach will result in more memcpys but it shouldn't
                // matter much in practice.
                let remainder = entry.truncate((next_span.start - entry.0) as usize);
                idx += 1;
                self.0.insert(idx, remainder);
            }

            let entry = &mut self.0[idx];
            debug_assert!(entry.0 == next_span.start);

            // Note that we're leaving in entries with excess_deletes of 0. The reason for this is
            // that decrement_delete_range is used when bouncing between versions. Usually we'll
            // come right back to the branch in which the item was deleted twice, and in that case
            // its more efficient not to need to slide entries around all over the place.

            // Logic only correct because |update_by| == 1.
            if update_by < 0 && entry.1.excess_deletes == 0 || update_by > 0 && entry.1.excess_deletes == max_value {
                // We can't decrement an entry with 0 or past max_value. We're done here.
                break;
            }

            if entry.len() <= next_span.len() {
                entry.1.excess_deletes = entry.1.excess_deletes.wrapping_add(update_by as u32);
                next_span.truncate_keeping_right(entry.1.len as usize);
                modified += entry.1.len;
                if next_span.len == 0 { break; }
                idx += 1;
            } else {
                // entry.len > next_entry.len. Split entry into 2 parts, increment excess_deletes
                // and we're done.
                let remainder = entry.truncate(next_span.len());
                entry.1.excess_deletes = entry.1.excess_deletes.wrapping_add(update_by as u32);
                modified += entry.1.len;
                self.0.insert(idx + 1, remainder);
                break;
            }
        }

        modified
    }

    pub(crate) fn modify_delete_range(&mut self, base: LV, len: u32, update_by: i32, max_value: u32) -> u32 {
        let start = self.find_index(base);
        let idx = start.unwrap_or_else(|idx| idx);
        self.modify_delete_range_idx(base, len, idx, update_by, max_value)
    }

    pub fn increment_delete_range(&mut self, base: LV, len: u32) {
        self.modify_delete_range(base, len, 1, u32::MAX);
    }

    pub fn increment_delete_range_to(&mut self, base: LV, max_len: u32, max_value: u32) -> u32 {
        self.modify_delete_range(base, max_len, 1, max_value)
    }

    pub fn decrement_delete_range(&mut self, base: LV, max_len: u32) -> u32 {
        self.modify_delete_range(base, max_len, -1, u32::MAX)
    }

    /// Find the range of items which have (implied or explicit) 0 double deletes
    pub(crate) fn find_zero_range(&self, base: LV, max_len: u32) -> u32 {
        // let mut span = OrderSpan { order: base, len: max_len };

        for idx in self.find_index(base).unwrap_or_else(|idx| idx)..self.0.len() {
            let e = &self.0[idx];
            debug_assert_ne!(e.1.len, 0);

            if e.0 >= base + max_len {
                return max_len;
            } else if e.1.excess_deletes != 0 {
                // The element overlaps and its non-zero.
                return if e.0 <= base { 0 } else { e.0 - base }
            }
        }
        max_len
    }
}

// Note this code is more heavily tested because its rarely called in practice. Rare bugs are worst
// bugs.
#[cfg(test)]
mod tests {
    use crate::rle::{RleVec, KVPair};
    use crate::list::double_delete::DoubleDelete;
    use rle::test_splitable_methods_valid;

    #[test]
    fn double_delete_entry_valid() {
        test_splitable_methods_valid(DoubleDelete {
            len: 10,
            excess_deletes: 2
        });
    }

    #[test]
    fn inc_delete_range() {
        // This isn't completely exhaustive, but its pretty robust.
        // It'd be nice to do coverage reporting on this.
        let mut deletes: RleVec<KVPair<DoubleDelete>> = RleVec::new();
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

    #[test]
    fn delete_regression() {
        // Regression. This bug was found by the fuzzer.
        let mut deletes: RleVec<KVPair<DoubleDelete>> = RleVec::new();
        deletes.increment_delete_range(5, 2);
        deletes.increment_delete_range(5, 1);
        deletes.increment_delete_range(5, 2);

        assert_eq!(deletes.0, vec![
            KVPair(5, DoubleDelete { len: 1, excess_deletes: 3 }),
            KVPair(6, DoubleDelete { len: 1, excess_deletes: 2 }),
        ]);

        // dbg!(&deletes);
    }

    #[test]
    fn dec_delete_range() {
        // This is mostly the same code is inc_delete_range so we don't need too much testing.
        let mut deletes: RleVec<KVPair<DoubleDelete>> = RleVec::new();
        assert_eq!(deletes.decrement_delete_range(5, 10), 0);

        deletes.increment_delete_range(5, 3);
        assert_eq!(deletes.decrement_delete_range(5, 5), 3);
        assert_eq!(deletes.0, vec![KVPair(5, DoubleDelete { len: 3, excess_deletes: 0 })]);


        deletes.increment_delete_range(6, 3);
        // dbg!(&deletes);
        assert_eq!(deletes.0, vec![
            KVPair(5, DoubleDelete { len: 1, excess_deletes: 0 }),
            KVPair(6, DoubleDelete { len: 3, excess_deletes: 1 }),
        ]);
        assert_eq!(deletes.decrement_delete_range(5, 3), 0);
        assert_eq!(deletes.decrement_delete_range(7, 3), 2);
    }

    #[test]
    fn zero_range() {
        let mut deletes: RleVec<KVPair<DoubleDelete>> = RleVec::new();
        assert_eq!(deletes.find_zero_range(10, 100), 100);

        deletes.increment_delete_range(5, 3);
        assert_eq!(deletes.find_zero_range(10, 100), 100);
        assert_eq!(deletes.find_zero_range(8, 100), 100);
        assert_eq!(deletes.find_zero_range(7, 100), 0);
        assert_eq!(deletes.find_zero_range(5, 100), 0);
        assert_eq!(deletes.find_zero_range(0, 100), 5);

        deletes.decrement_delete_range(5, 3);
        assert_eq!(deletes.find_zero_range(0, 100), 100);
        assert_eq!(deletes.find_zero_range(5, 100), 100);
        assert_eq!(deletes.find_zero_range(8, 100), 100);
        assert_eq!(deletes.find_zero_range(10, 100), 100);

        deletes.increment_delete_range(7, 3);
        assert_eq!(deletes.find_zero_range(0, 100), 7);
        assert_eq!(deletes.find_zero_range(5, 100), 2);
        assert_eq!(deletes.find_zero_range(7, 100), 0);
        assert_eq!(deletes.find_zero_range(8, 100), 0);
        assert_eq!(deletes.find_zero_range(9, 100), 0);
        assert_eq!(deletes.find_zero_range(10, 100), 100);
    }
}