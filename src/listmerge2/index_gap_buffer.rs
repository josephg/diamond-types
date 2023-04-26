use std::cmp::Ordering;
use std::ops::Range;
use rle::{HasLength, MergableSpan, merge_items, MergeIter, SplitableSpan};
use crate::frontier::{debug_assert_sorted, is_sorted_slice};
use crate::list::op_iter::OpMetricsIter;
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::operation::ListOpKind;
use crate::listmerge2::action_plan::{MergePlan, MergePlanAction};
use crate::listmerge2::Index;
use crate::listmerge2::yjsspan::{YjsSpan, SpanState, YjsSpanWithState};
use crate::rle::{KVPair, RleVec};


#[derive(Default, Debug, Clone, Copy)]
struct IndexInfo {
    active: bool,
    before_gap_len: usize,
}

#[derive(Debug, Clone)]
struct IndexGapBuffer {
    // Gap buffer size = items.len().
    // == indexes[xx].len() at all times.
    items: Vec<YjsSpan>,
    gap_start_idx: usize,
    gap_end_idx: usize,

    states: Vec<SpanState>, // internally a 2d array of [index * len + item] -> state.
    // start_end_len: Vec<(usize, usize)>, // Index -> (length of gap_start, length of gap_end) for this index.
    index_info: Vec<IndexInfo>, // Index -> length of gap_start for this index.
}

fn next_contains<I: Eq + Copy>(i: &mut usize, needle: I, haystack: &[I]) -> bool {
    // TODO: I think this loop could correctly be an if() statement in cases where we check every
    // item - and I think that is guaranteed in all cases in this code.
    while *i < haystack.len() {
        if haystack[*i] == needle { return true; }
        *i += 1;
    }
    false
}

impl IndexGapBuffer {
    // This returns a gap buffer which doesn't have any starting entries
    fn new_raw(num_indexes: usize, starting_buf_size: usize) -> Self {
        assert!(num_indexes >= 1);
        assert!(starting_buf_size >= 2);

        Self {
            items: vec![YjsSpan::default(); starting_buf_size],
            states: vec![SpanState::NotInsertedYet; starting_buf_size * num_indexes],
            gap_start_idx: 0,
            gap_end_idx: starting_buf_size,
            index_info: vec![IndexInfo::default(); num_indexes],
        }
    }

    fn new_internal(num_indexes: usize, starting_buf_size: usize) -> Self {
        let mut result = Self::new_raw(num_indexes, starting_buf_size);

        let starting_item = YjsSpan::new_undiff_max();

        // But with modifications:
        // - We always start with the first index active
        result.index_info[0] = IndexInfo {
            active: true,
            before_gap_len: starting_item.len(),
        };

        // - And because we might be merging into an existing document, start with a single
        //   underwater item which is in the inserted state, to represent any existing content.
        result.items[0] = starting_item;
        result.states[0] = SpanState::Inserted; // [0] is the start of index 0.
        result.gap_start_idx = 1;

        result
    }

    fn new_with_num_indexes(num_indexes: usize) -> Self {
        Self::new_internal(num_indexes, 16)
        // Self::new_internal(num_indexes, 256)
    }

    // Basically just for testing methods
    fn active_items(&self) -> usize {
        self.items.len() - (self.gap_end_idx - self.gap_start_idx)
    }

    // fn item_len(&self) -> usize {
    //     self.items.len()
    // }

    fn dbg_check(&self) {
        let buffer_size = self.items.len();
        let num_indexes = self.index_info.len();

        assert_eq!(self.states.len(), buffer_size * num_indexes);
        for (i, states) in self.states.chunks_exact(buffer_size).enumerate() {

            let start_actual_len: usize = self.items[..self.gap_start_idx].iter()
                .zip(states[..self.gap_start_idx].iter().copied())
                .map(|(span, state)| span.content_len_with_state(state))
                .sum();

            assert_eq!(self.index_info[i].before_gap_len, start_actual_len);

            // let end_actual_len: usize = self.items[self.gap_end..].iter()
            //     .zip(states[self.gap_end..].iter().copied())
            //     .map(|(span, state)| span.content_len_with_state(state))
            //     .sum();
            //
            // assert_eq!(self.start_end_len[i].1, end_actual_len);
        }
        // assert_eq!(self.start_end_len.len(), num_indexes);
    }

    /// Returns the total "inserted length" for a given index.
    fn count_len(&self, index: Index) -> usize {
        // We don't actually store the length of the tail, since its not that useful.
        debug_assert!(self.index_info[index].active);

        let index_start = self.start_state_idx(index);
        let tail_len: usize = self.items[self.gap_end_idx..].iter()
            .zip(self.states[index_start + self.gap_end_idx..index_start + self.items.len()].iter().copied())
            .map(|(item, state)| item.content_len_with_state(state))
            .sum();

        self.index_info[index].before_gap_len + tail_len
    }

    fn num_indexes(&self) -> Index { self.index_info.len() }

    /// Returns the amount the list grew by.
    fn grow(&mut self) -> usize {
        let old_size = self.items.len();
        let new_size = old_size * 2; // Double it!

        self.items.resize(new_size, YjsSpan::default());
        // Move items after the gap to the end of the vec.

        let new_gap_end = new_size - old_size + self.gap_end_idx;

        self.items.copy_within(self.gap_end_idx..old_size, new_gap_end);

        let num_indexes = self.num_indexes();
        self.states.resize(new_size * num_indexes, SpanState::NotInsertedYet);
        for (i, info) in self.index_info.iter().enumerate().rev() {
            if info.active {
                let old_start = old_size * i;
                let new_start = new_size * i;
                self.states.copy_within(old_start + self.gap_end_idx..old_start + old_size, new_start + new_gap_end);
                self.states.copy_within(old_start..old_start + self.gap_start_idx, new_start);
            }
        }
        // for i in (0..num_indexes).rev() {
        //     if self.index_info[i].active {
        //         let old_start = old_size * i;
        //         let new_start = new_size * i;
        //         self.states.copy_within(old_start + self.gap_end_idx..old_start + old_size, new_start + new_gap_end);
        //         self.states.copy_within(old_start..old_start + self.gap_start_idx, new_start);
        //     }
        // }
        self.gap_end_idx = new_gap_end;

        // We doubled - so we grew by old_size.
        old_size
    }

    // Move the gap and split the item at new_gap_i / offset.
    fn move_gap_and_split(&mut self, split_i: usize, offset: usize) {
        debug_assert!(split_i < self.gap_start_idx || split_i >= self.gap_end_idx);

        let split_item = &mut self.items[split_i];
        if offset == 0 {
            self.move_gap(split_i);
            return;
        }
        if offset == split_item.len() {
            self.move_gap(split_i + 1);
            return;
        }

        let remainder = split_item.truncate(offset);

        if split_i < self.gap_start_idx {
            // Gap is moving left / items are moving right.

            // The remainder is being moved from the left side of the gap to the right side.
            let items_len = self.items.len();
            let r_len = remainder.len();

            let mut base = split_i;
            for info in self.index_info.iter_mut() {
                if info.active && self.states[base] == SpanState::Inserted {
                    info.before_gap_len -= r_len;
                }
                base += items_len;
            }
        }

        self.move_gap(split_i + 1);

        // Will this little short circuit actually happen much in practice?
        if self.gap_end_idx < self.items.len() && remainder.can_append(&self.items[self.gap_end_idx]) {
            self.items[self.gap_end_idx].prepend(remainder);
        } else {
            // We need to reinsert the remainder regardless. For that we need room:
            if self.gap_size() == 0 { self.grow(); }

            debug_assert!(self.gap_end_idx >= 1); // Guaranteed after the call to grow().
            self.gap_end_idx -= 1;
            self.items[self.gap_end_idx] = remainder;

            // And set the state.
            let mut base = 0;
            let num_items = self.items.len();
            for info in self.index_info.iter() {
                // Copy the state from the split item's original position.
                if info.active {
                    self.states[base + self.gap_end_idx] = self.states[base + self.gap_start_idx - 1];
                }
                base += num_items;
            }
        }
    }

    fn states_match(&self, mut a: usize, mut b: usize) -> bool {
        debug_assert_ne!(a, b);

        let num_items = self.items.len();
        for info in self.index_info.iter() {
            if info.active {
                if self.states[a] != self.states[b] { return false; }
            }
            a += num_items;
            b += num_items;
        }
        true
    }

    fn simplify_gap_to_front(&mut self, end_idx: usize) {
        if self.gap_start_idx == 0 { return; }

        let target_i = self.gap_start_idx - 1;

        let mut moved_len = 0;

        while self.gap_end_idx < self.items.len() && self.gap_end_idx < end_idx {
            let src_i = self.gap_end_idx;

            // We can merge them if they can append and if the states in all active indexes match.

            if !self.items[target_i].can_append(&self.items[src_i])
                || !self.states_match(src_i, target_i)
            {
                break;
            }

            // Ok, we can append.
            let src = self.items[src_i];
            moved_len += src.len();
            self.items[target_i].append(src);
            self.gap_end_idx += 1;
        }

        if moved_len > 0 {
            let mut base = target_i;
            let num_items = self.items.len();
            for info in self.index_info.iter_mut() {
                if info.active {
                    if self.states[base] == SpanState::Inserted {
                        info.before_gap_len += moved_len;
                    }
                }
                base += num_items;
            }
        }
    }

    fn simplify_gap_to_tail(&mut self, min_idx: usize) {
        if self.gap_end_idx >= self.items.len() { return; }

        let target_i = self.gap_end_idx;
        let mut moved_len = 0;

        while self.gap_start_idx > min_idx {
            let src_i = self.gap_start_idx - 1;

            // We can merge them if they can append and if the states in all active indexes match.
            if !self.items[src_i].can_append(&self.items[target_i])
                || !self.states_match(src_i, target_i)
            {
                break;
            }

            // Ok, we can append.
            let src = self.items[src_i];
            moved_len += src.len();
            self.items[target_i].prepend(src);
            self.gap_start_idx -= 1;
        }

        if moved_len > 0 {
            let mut base = target_i;
            let num_items = self.items.len();
            for info in self.index_info.iter_mut() {
                if info.active {
                    if self.states[base] == SpanState::Inserted {
                        info.before_gap_len -= moved_len;
                    }
                }
                base += num_items;
            }
        }
    }

    fn move_gap(&mut self, new_gap_i: usize) {
        // TODO: This method currently doesn't merge adjacent items when the gap closes. Consider
        // adding that!
        let count_moved_size = |items: &[YjsSpan], states: &[SpanState], moved_range: Range<usize>, base: usize| -> (usize, Range<usize>) {
            let r2 = moved_range.start+base..moved_range.end+base;
            let mut moved_size = 0;
            for (span, state) in items[moved_range.clone()].iter()
                .zip(states[r2.clone()].iter().copied())
            {
                if state == SpanState::Inserted {
                    moved_size += span.len();
                }
            }
            (moved_size, r2)
        };

        match new_gap_i.cmp(&self.gap_start_idx) {
            Ordering::Equal => {} // Nothing to do!
            Ordering::Less => {
                // Move the gap left, and items right.
                self.simplify_gap_to_tail(new_gap_i);
                let moved_range = new_gap_i..self.gap_start_idx;
                if moved_range.is_empty() { return; }
                let new_gap_end = self.gap_end_idx - moved_range.len();

                // The annoying part: Updating all the indexes and states.
                let mut base = 0;
                let items_len = self.items.len();
                for index_info in self.index_info.iter_mut() {
                    if !index_info.active { continue; }
                    let (moved_size, r2) = count_moved_size(&self.items, &self.states, moved_range.clone(), base);
                    // dbg!(moved_size);

                    self.states.copy_within(r2, new_gap_end+base);
                    index_info.before_gap_len -= moved_size;
                    base += items_len;
                }

                // The easy part - move the actual items.
                self.items.copy_within(moved_range.clone(), new_gap_end);

                self.gap_start_idx = new_gap_i;
                self.gap_end_idx = new_gap_end;
            }
            Ordering::Greater => {
                // Move the gap right, the items left.
                self.simplify_gap_to_front(new_gap_i);
                let moved_range = self.gap_end_idx..new_gap_i;
                if moved_range.is_empty() { return; } // Nothing to do!

                // Update the indexes and states. Code adapted from above.
                let mut base = 0;
                let items_len = self.items.len();
                for index_info in self.index_info.iter_mut() {
                    if !index_info.active { continue; }
                    let (moved_size, r2) = count_moved_size(&self.items, &self.states, moved_range.clone(), base);
                    self.states.copy_within(r2, self.gap_start_idx+base);
                    index_info.before_gap_len += moved_size;
                    base += items_len;
                }

                // Move the items themselves.
                self.items.copy_within(moved_range.clone(), self.gap_start_idx);

                self.gap_start_idx += moved_range.len();
                self.gap_end_idx = new_gap_i;
            }
        }
    }

    fn gap_size(&self) -> usize {
        self.gap_end_idx - self.gap_start_idx
    }

    fn all_indexes_insert(&self, i: usize) -> bool {
        for (index, info) in self.index_info.iter().enumerate() {
            if !info.active { continue; }
            if self.states[self.state_idx_at(index, i)] != SpanState::Inserted { return false; }
        }
        true
    }

    fn insert(&mut self, new_item: YjsSpan, pos: usize, update_index: Index, other_indexes: &[Index]) {
        let (i, offset) = self.find(update_index, pos, true);
        // dbg!(pos, (i, offset));
        self.insert_internal(new_item, i, offset, update_index, other_indexes);
    }

    // State for the new item is implicitly Inserted.
    fn insert_internal(&mut self, new_item: YjsSpan, mut i: usize, mut offset: usize, update_index: Index, other_indexes: &[Index]) {
        // Indexes must be sorted.
        debug_assert!(is_sorted_slice::<true, _>(other_indexes));
        debug_assert!(!other_indexes.contains(&update_index));

        // assert!(i < self.item_len() || i == self.item_len() && offset == 0);

        // Could roll back inserts with offset=0 to the start of the next item, but I don't think
        // the logic below would use that hinge anyway.
        assert!(i < self.items.len());
        // i == 0 if we're empty.
        let is_valid_pos = i < self.gap_start_idx || i >= self.gap_end_idx;
        assert!(i == 0 || is_valid_pos);

        // let all_inserts = |i: usize| -> bool {
        //     for (index, info) in self.index_info.iter().enumerate() {
        //         if !info.active { continue; }
        //         if self.states[self.state_idx_at(index, i)] != SpanState::Inserted { return false; }
        //     }
        //     true
        // };

        let new_item_len = new_item.len();

        // if i == 0 && self.gap_start_idx == 0 {
        //     // We're presumably inserting into an empty list. I could make the code below handle
        //     // this case, but its easier to just special case it here and return.
        //     self.items[0] = new_item;
        //     self.set_item_state_inserted(0, update_index, other_indexes);
        //     self.gap_start_idx += 1;
        //     self.add_to_gap_len(i, update_index, other_indexes, new_item_len);
        //     return;
        // }

        if is_valid_pos {
            let item = &self.items[i];

            if new_item.is_undiff() && item.is_undiff() && self.all_indexes_insert(i) {
                // No sweat.
                self.items[i].append(new_item);
                self.add_to_gap_len(i, update_index, other_indexes, new_item_len);
                return;
            }

            if offset == item.len() {
                // Try to simply append the new item to the end of the existing item.
                if item.can_append(&new_item) && self.all_indexes_insert(i) {
                    // Easy case. Just append the item to the end of the existing item here. Don't even
                    // sweat dawg.
                    self.items[i].append(new_item);
                    self.add_to_gap_len(i, update_index, other_indexes, new_item_len);
                    return;
                }

                // Ok, we can't append. Roll to next.
                i += 1;
                offset = 0;
            }

            if offset == 0 {
                // Try to prepend the new content to the next item.

                // TODO: I have a feeling this code will never be executed, because we insert spans
                // in chronological order. Check and if I'm right, remove this code entirely.
                let next_item_idx = if i == self.gap_start_idx { self.gap_end_idx } else { i };

                if next_item_idx < self.items.len() && new_item.can_append(&self.items[next_item_idx]) && self.all_indexes_insert(next_item_idx) {
                    self.items[next_item_idx].prepend(new_item);
                    self.add_to_gap_len(i, update_index, other_indexes, new_item_len);
                    return;
                }
            }
        } else {
            debug_assert_eq!(offset, 0);
        }

        // If we split an item, we'll grow the size by 2 slots. ([aa] -> [a,B,a]).
        let (remainder, gap_needed) = if offset > 0 {
            debug_assert!(i < self.items.len() && (i < self.gap_start_idx || i >= self.gap_end_idx));
            let rem = self.items[i].truncate(offset);
            let rem_idx = i;

            // I'd zero offset, but it isn't used after this point anyway and the compiler generates
            // warnings if I assign to it.
            //offset = 0;

            // Sooo, this is weird but - we're trimming this item's length here, and we'll reinsert
            // it before the gap down below. There's only 2 cases:
            // 1. Its before the gap now (and it stays there)
            // 2. Its after the gap now, and it moves before the gap. The before_length will
            //    need to be increased.

            // I could unconditionally reduce the before_gap length now and then add it back after,
            // but its more efficient to just do all that here.
            if rem_idx >= self.gap_end_idx {
                let mut ii = rem_idx;
                let items_len = self.items.len();
                let rem_len = rem.len();

                for index in 0..self.index_info.len() {
                    if self.states[ii] == SpanState::Inserted {
                        self.index_info[index].before_gap_len += rem_len;
                    }

                    ii += items_len;
                }
            }

            i += 1;
            (Some((rem, rem_idx)), 2)
        } else {
            (None, 1)
        };

        let grew_by = if self.gap_size() < gap_needed {
            let grew_by = self.grow();

            // The grow() method doubles the size, and we start at a reasonable number (not 1). So
            // the size should always be fine after we've grown the list.
            debug_assert!(self.gap_size() >= gap_needed);

            if i >= self.gap_end_idx { i += grew_by; }
            grew_by
        } else { 0 };

        if i != self.gap_start_idx {
            self.move_gap(i);
            // If i previously pointed to an item after the gap, it will end up pointing to
            // self.gap_end_idx. Easier to just consistently fill the gap at this point from the
            // left.
            i = self.gap_start_idx;
        }

        assert!(i + 1 < self.items.len()); // Saves the compiler from needing bounds checks below.

        self.items[i] = new_item;
        self.set_item_state_inserted(i, update_index, other_indexes);
        self.gap_start_idx += 1;
        self.add_to_gap_len(i, update_index, other_indexes, new_item_len);

        if let Some((rem, mut rem_idx)) = remainder {
            if rem_idx >= self.gap_start_idx { rem_idx += grew_by; }
            self.items[i + 1] = rem;
            self.copy_item_state(rem_idx, i + 1);
            self.gap_start_idx += 1;
        }
    }

    fn mark_deleted_front(&mut self, i: usize, offset: usize, del_len: usize, index: Index, other_indexes: &[Index]) {
        debug_assert!(i < self.gap_start_idx);
        debug_assert_sorted(other_indexes);

        debug_assert!(offset < self.items[i].len());
        // if offset == self.items[i].len() {
        //     offset = 0;
        //     i += 1;
        // }

        let items_len = self.items.len();
        let mut base = i;
        let n = self.gap_start_idx - i;
        let mut other_index_i = 0;

        if offset > 0 {
            // We need to split this item - since the second half will be marked deleted and the
            // first half will stay with its current state.
            if self.gap_size() == 0 { self.grow(); }

            let remainder = self.items[i].truncate(offset);
            self.items.copy_within(i+1..self.gap_start_idx, i + 2);
            self.items[i + 1] = remainder;

            // And duplicate the state.
            for (idx, info) in self.index_info.iter_mut().enumerate().rev() {
                if info.active {
                    if idx == index || next_contains(&mut other_index_i, idx, other_indexes) {
                        // We can't mark things as deleted if they're in the NotInsertedYet state.
                        // Everything in the range should already be deleted (so no change) or inserted.
                        debug_assert!(self.states[base..base + n]
                            .iter().copied()
                            .all(|s| s != SpanState::NotInsertedYet));

                        self.states[base + 1..base + n + 1].fill(SpanState::Deleted);
                        info.before_gap_len -= del_len;
                    } else {
                        self.states.copy_within(base..base + n, base + 1);
                    }
                }
                base += items_len;
            }

            self.gap_start_idx += 1;
        } else {
            // Since offset is 0, we just mark the items as deleted.
            for (idx, info) in self.index_info.iter_mut().enumerate().rev() {
                if info.active {
                    if idx == index || next_contains(&mut other_index_i, idx, other_indexes) {
                        debug_assert!(self.states[base..base + n]
                            .iter().copied()
                            .all(|s| s != SpanState::NotInsertedYet));

                        self.states[base..base + n].fill(SpanState::Deleted);
                        info.before_gap_len -= del_len;
                    }
                }
                base += items_len;
            }
        }
    }

    fn mark_deleted_tail(&mut self, i: usize, offset: usize, index: Index, other_indexes: &[Index]) {
        // Crimes! This is a modified copy of mark_deleted_front.
        debug_assert!(i >= self.gap_end_idx);
        debug_assert_sorted(other_indexes);
        debug_assert!(offset < self.items[i].len());

        let items_len = self.items.len();
        let mut base = self.gap_end_idx;
        let mut other_index_i = 0;

        if offset > 0 {
            // We need to split this item - since the *first* half will be marked deleted and the
            // second half will stay with its current state.
            if self.gap_size() == 0 { self.grow(); }

            let remainder = self.items[i].truncate_keeping_right(offset);
            self.items.copy_within(self.gap_end_idx..i, self.gap_end_idx - 1);
            self.items[i - 1] = remainder;

            // So item i stays as-is (though its truncated), and item i-1 is the last item which
            // is marked as deleted.

            // And duplicate the state.
            let n = i - self.gap_end_idx;
            for (idx, info) in self.index_info.iter_mut().enumerate().rev() {
                if info.active {
                    if idx == index || next_contains(&mut other_index_i, idx, other_indexes) {
                        // If we're marking things as deleted, they have to first be in the inserted
                        // state!
                        debug_assert!(self.states[base..base + n]
                            .iter().copied()
                            .all(|s| s != SpanState::NotInsertedYet));

                        self.states[base - 1..base + n].fill(SpanState::Deleted);
                    } else {
                        self.states.copy_within(base..base + n + 1, base - 1);
                    }
                }
                base += items_len;
            }

            self.gap_end_idx -= 1;
        } else {
            // Since offset is 0, we just mark the items as deleted.
            let n = i - self.gap_end_idx + 1;
            for (idx, info) in self.index_info.iter_mut().enumerate().rev() {
                if info.active {
                    if idx == index || next_contains(&mut other_index_i, idx, other_indexes) {
                        // If we're marking things as deleted, they have to first be in the inserted
                        // state!
                        debug_assert!(self.states[base..base + n]
                            .iter().copied()
                            .all(|s| s != SpanState::NotInsertedYet));

                        self.states[base..base + n].fill(SpanState::Deleted);
                    }
                }
                base += items_len;
            }
        }
    }

    fn mark_deleted(&mut self, pos: usize, del_len: usize, index: Index, other_indexes: &[Index]) {
        assert!(self.index_info[index].active);

        // First find the deleted item and figure out the range of items which will be deleted.

        let (start_i, start_offset) = self.find(index, pos, false);
        let len_remaining = self.items[start_i].len() - start_offset;
        // if len_remaining >= del_count {
        //     // Only this item is modified. Special case when there's no concurrency.
        //     todo!()
        // }

        // Now find the end. To avoid rescanning too much, I'll use the currently found item as an
        // anchor if we can. (And because deletes are often short).
        let end = pos + del_len;
        let gap_pos = self.index_info[index].before_gap_len;

        if end <= gap_pos {
            // Scan from i.
            let (end_i, end_offset) = if len_remaining >= del_len {
                (start_i, start_offset + del_len)
            } else {
                self.find_front(index, del_len + start_offset, true, start_i)
            };

            // The whole edit is in the front.
            self.move_gap_and_split(end_i, end_offset);
            self.mark_deleted_front(start_i, start_offset, del_len, index, other_indexes);
        } else if start_i >= self.gap_end_idx {
            let (end_i, end_offset) = if len_remaining >= del_len {
                (start_i, start_offset + del_len)
            } else {
                self.find_tail(index, del_len + start_offset, true, start_i)
            };

            // The whole edit is in the tail.
            self.move_gap_and_split(start_i, start_offset);
            self.mark_deleted_tail(end_i, end_offset, index, other_indexes);
        } else {
            // The deleted range is partially in the front and partially in the tail.
            let (end_i, end_offset) = self.find_tail(index, end - gap_pos, true, self.gap_start_idx);

            // I could do the start and end in either order here; but doing the end first means
            // if the list grows I don't need need to adjust any indexes.
            self.mark_deleted_tail(end_i, end_offset, index, other_indexes);
            self.mark_deleted_front(start_i, start_offset, gap_pos - pos, index, other_indexes);
        }
    }

    fn copy_item_state(&mut self, from_i: usize, to_i: usize) {
        for index in 0..self.index_info.len() {
            let base = self.start_state_idx(index);
            self.states[base + to_i] = self.states[base + from_i];
        }
    }

    fn add_to_gap_len(&mut self, i: usize, update_index: Index, other_indexes: &[Index], item_len: usize) {
        if i < self.gap_start_idx {
            self.index_info[update_index].before_gap_len += item_len;
            for &index in other_indexes {
                self.index_info[index].before_gap_len += item_len;
            }
        }
    }

    fn sub_from_gap_len(&mut self, i: usize, update_index: Index, other_indexes: &[Index], item_len: usize) {
        if i < self.gap_start_idx {
            self.index_info[update_index].before_gap_len -= item_len;
            for &index in other_indexes {
                if self.states[self.state_idx_at(index, i)] == SpanState::Inserted {
                    self.index_info[index].before_gap_len -= item_len;
                }
            }
        }
    }

    // TODO: Refactor / adapt this when I implement deleting stuff.
    fn set_item_state_inserted(&mut self, mut i: usize, update_index: Index, other_indexes: &[Index]) {
        // Indexes must be sorted.
        debug_assert!(is_sorted_slice::<true, _>(other_indexes));
        debug_assert!(!other_indexes.contains(&update_index));

        let mut other_i = 0;
        let items_len = self.items.len();
        for index in 0..self.index_info.len() {
            self.states[i] = if index == update_index || next_contains(&mut other_i, index, other_indexes) {
                SpanState::Inserted
            } else {
                SpanState::NotInsertedYet
            };

            i += items_len;
        }

        // And update the before-gap length.
        // if i < self.gap_start_idx {
        //     self.index_info[update_index].before_gap_len += item_len;
        //     for &index in update_other_indexes {
        //         self.index_info[index].before_gap_len += item_len;
        //     }
        // }
    }

    fn start_state_idx(&self, index: Index) -> usize {
        self.items.len() * index
    }
    fn state_idx_at(&self, index: Index, i: usize) -> usize {
        self.items.len() * index + i
    }


    // TODO: Refactor these two methods to reuse code.
    /// returns (index, offset).
    fn find_front(&self, index: Index, mut count: usize, prefer_first: bool, from_i: usize) -> (usize, usize) {
        // We're looking for the item in the first half of the gap buffer.
        if prefer_first && count == 0 { return (from_i, 0); }
        let index_start = self.start_state_idx(index);

        // TODO: This scans from the start, but the desired position will often be closer to
        // the gap position rather than close to the start of the whole thing. Rewrite this to
        // scan backwards from the gap based on a heuristic.
        // let mut pos = 0;
        for (i, (span, state)) in self.items[from_i..self.gap_start_idx].iter()
            .zip(self.states[index_start + from_i..index_start + self.gap_start_idx].iter().copied())
            .enumerate()
        {
            if state != SpanState::Inserted { continue; }

            let len = span.len();
            if len > count || (prefer_first && len == count) {
                return (from_i + i, count);
            }
            count -= span.len();
        }

        if !prefer_first && count == 0 { return (self.gap_start_idx, 0); }
        panic!("Content position overflowed gap_pos")
    }

    /// returns (index, offset).
    fn find_tail(&self, index: Index, mut count: usize, prefer_first: bool, from_i: usize) -> (usize, usize) {
        let index_start = self.start_state_idx(index);

        for (i, (span, state)) in self.items[from_i..].iter()
            .zip(self.states[index_start + from_i..index_start + self.items.len()].iter().copied())
            .enumerate()
        {
            // TODO: This loop body copied from above. Refactor to avoid that!
            if state != SpanState::Inserted { continue; }

            let len = span.len();
            if len > count || (prefer_first && len == count) {
                return (from_i + i, count);
            }
            count -= span.len();
        }

        if !prefer_first && count == 0 {
            // Essentially we've been asked for the index of the *next* item, but no such
            // position exists. Bail!
            panic!("Requested last item and prefer_first is false.");
        }
        panic!("Requested content position is past end of list");
    }


    /// If there's a run of items with content length 0, if prefer_first is true we'll return
    /// the first position which matches the content length. Otherwise we'll return the last
    /// content position which matches.
    fn find(&self, index: Index, content_pos: usize, prefer_first: bool) -> (usize, usize) {
        debug_assert!(self.index_info[index].active);
        let gap_pos = self.index_info[index].before_gap_len;

        return if content_pos < gap_pos || (prefer_first && content_pos == gap_pos) {
            // We're looking for the item in the first half of the gap buffer.
            // TODO: Consider scanning backwards when we can.
            self.find_front(index, content_pos, prefer_first, 0)
        } else {
            self.find_tail(index, content_pos - gap_pos, prefer_first, self.gap_end_idx)
        }
    }

    fn print_states(&self) {
        for (index, states) in self.states.chunks_exact(self.items.len()).enumerate() {
            print!("{index}: ");
            for (s, state) in states.iter().copied().enumerate() {
                let in_gap = s >= self.gap_start_idx && s < self.gap_end_idx;
                print!("{}", match (in_gap, state) {
                    (true, _) => '_',
                    (false, SpanState::NotInsertedYet) => 'n',
                    (false, SpanState::Inserted) => 'I',
                    (false, SpanState::Deleted) => 'D',
                });
            }
            println!();
        }
    }
}

#[derive(Debug, Clone)]
struct GapBufferReader<'a> {
    buffer: &'a IndexGapBuffer,
    index: Index,
    i: usize,
}

impl<'a> Iterator for GapBufferReader<'a> {
    type Item = (YjsSpan, SpanState);

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.buffer.items.len() { return None; }

        if self.i == self.buffer.gap_start_idx {
            self.i = self.buffer.gap_end_idx;
            if self.i >= self.buffer.items.len() { return None; }
        }

        let item = self.buffer.items[self.i];
        let idx = self.buffer.state_idx_at(self.index, self.i);
        let state = self.buffer.states[idx];
        self.i += 1;
        Some((item, state))
    }
}

impl<'a> GapBufferReader<'a> {
    fn new(buffer: &'a IndexGapBuffer, index: Index, i: usize) -> Self {
        let info = &buffer.index_info[index];
        assert!(info.active);

        Self { buffer, index, i }
    }
}

impl IndexGapBuffer {
    pub fn iter(&self, index: Index) -> GapBufferReader<'_> {
        assert!(index < self.index_info.len());
        debug_assert!(self.index_info[index].active, "Cannot iterate an inactive index");
        GapBufferReader::new(self, index, 0)
    }

    pub fn iter_merged(&self, index: Index) -> impl Iterator<Item = YjsSpanWithState> + '_ {
        merge_items(self.iter(index).map(|(span, state)| YjsSpanWithState(span, state)))
    }
}

fn run_plan(plan: &MergePlan, op_metrics: &RleVec<KVPair<ListOpMetrics>>, ctx: &ListOperationCtx) {
    if plan.actions.is_empty() { return; } // Nothing to do anyway.
    if plan.indexes_used == 1 {
        // In this case, we don't actually need to set up the gap buffer. We can just copy input ->
        // output.
        eprintln!("TODO: Update me to do less work!");
    }

    let mut buffer = IndexGapBuffer::new_with_num_indexes(plan.indexes_used);
    for action in plan.actions.iter() {
        match action {
            MergePlanAction::Apply(apply_action) => {
                for metrics in OpMetricsIter::new(op_metrics, ctx, apply_action.span) {
                    match metrics.1.kind {
                        ListOpKind::Ins => {

                        }
                        ListOpKind::Del => {
                            // For delete operations, we're marking the state certain number of ops as
                            // deleted
                        }
                    }
                    todo!()
                }
            }
            MergePlanAction::ClearInsertedItems => {
                for (i, s) in buffer.index_info.iter_mut().enumerate() {
                    // Not sure if this check is needed. The only active index should be index 0.
                    assert_eq!(s.active, i == 0);
                }

                buffer.items[0] = YjsSpan::new_undiff_max();
                buffer.states[0] = SpanState::Inserted; // [0] is the start of index 0.
                buffer.gap_start_idx = 1;
                buffer.gap_end_idx = buffer.items.len();
            }
            MergePlanAction::ForkIndex { src, dest } => {
                assert!(buffer.index_info[*src].active);
                buffer.index_info[*dest] = buffer.index_info[*src];

                // TODO: Benchmark this with std::ptr::copy_nonoverlapping.
                let src_start = buffer.start_state_idx(*src);
                let dest_start = buffer.start_state_idx(*dest);
                buffer.states.copy_within(
                    src_start..src_start+buffer.gap_start_idx,
                    dest_start
                );
                buffer.states.copy_within(
                    src_start+buffer.gap_end_idx..src_start+buffer.items.len(),
                    dest_start+buffer.gap_end_idx
                );
            }
            MergePlanAction::DropIndex(index) => {
                buffer.index_info[*index].active = false;
                // We don't need to clear it or anything. Just leave whatever junk was in there
                // before.
            }
            MergePlanAction::MaxIndex(index, from) => {
                assert!(buffer.index_info[*index].active);
                let mut before_gap_len = buffer.index_info[*index].before_gap_len;

                // TODO: We can reverse the loop order. Might be worth benchmarking or spending some
                // time with godbolt to figure out which is faster.
                let start_idx = buffer.start_state_idx(*index);

                // First do the start (the bit before the gap):
                for i in 0..buffer.gap_start_idx {
                    let ii = i + start_idx;
                    for &src_idx in from {
                        let from_idx = buffer.state_idx_at(src_idx, i);
                        let old_state = buffer.states[ii];
                        let merge_state = buffer.states[from_idx];
                        match (old_state, merge_state) {
                            (a, b) if a == b => { continue; },
                            (SpanState::NotInsertedYet, SpanState::Inserted) => {
                                before_gap_len += buffer.items[i].len();
                            },
                            (SpanState::NotInsertedYet, SpanState::Deleted) => {},
                            (SpanState::Inserted, SpanState::Deleted) => {
                                before_gap_len -= buffer.items[i].len();
                            },
                            _ => { continue; },
                        }
                        debug_assert!(merge_state > old_state);
                        buffer.states[ii] = merge_state;
                    }
                }

                buffer.index_info[*index].before_gap_len = before_gap_len;

                // Then do the bit after the gap. There's probably a way to merge these loops
                // together rather than copy+pasting+changing. But there's 2 reasons I'm not doing
                // that:
                // 1. They're a bit different because we don't need to update the index size in each
                //    case.
                // 2. This is hot code. We want the compiler to unroll these loops as much as it can
                //    anyway, so reusing code here is a non-goal.
                for i in buffer.gap_end_idx..buffer.items.len() {
                    let ii = i + start_idx;
                    for &src_idx in from {
                        let from_idx = buffer.state_idx_at(src_idx, i);
                        let old_state = buffer.states[ii];
                        let merge_state = buffer.states[from_idx];
                        if merge_state > old_state {
                            buffer.states[ii] = merge_state;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::SmallRng;
    use rand::{Rng, SeedableRng};
    use super::*;

    #[test]
    fn smoke_test() {
        let mut g = IndexGapBuffer::new_with_num_indexes(1);
        g.dbg_check();
        g.insert(YjsSpan {
            id: (0..10).into(),
            origin_left: 0,
            origin_right: 0,
        }, 0, 0, &[]);
        g.dbg_check();
        g.insert(YjsSpan {
            id: (10..20).into(),
            origin_left: 0,
            origin_right: 0,
        }, 5, 0, &[]);
        // }, 0, 20, 0, &[]);
        dbg!(&g);
        g.dbg_check();
    }

    #[test]
    fn resize() {
        // let mut g = IndexGapBuffer::new_internal(1, 2);
        // g.dbg_check();
        // g.insert(YjsSpan { id: 1.into(), origin_left: 0, origin_right: 0 }, 0, 1, 0, &[]);
        // g.dbg_check();

        let mut g = IndexGapBuffer::new_internal(1, 2);
        g.insert_internal(YjsSpan { id: 1.into(), origin_left: 0, origin_right: 0 }, 0, 0, 0, &[]);
        g.insert_internal(YjsSpan { id: 2.into(), origin_left: 0, origin_right: 0 }, 0, 0, 0, &[]);
        g.dbg_check();
        g.print_states();
        dbg!(&g);
    }

    #[test]
    fn merge_undiff() {
        let mut g = IndexGapBuffer::new_internal(1, 4);
        g.insert_internal(YjsSpan::new_undiff(1), 0, 1, 0, &[]);
        // g.insert(YjsSpan { id: 2.into(), origin_left: 0, origin_right: 0 }, 0, 0, 0, &[]);
        g.dbg_check();
        // g.print_states();

        assert_eq!(g.active_items(), 1);
        // dbg!(&g);

    }

    #[test]
    fn simple_insert_with_other_indexes() {
        let mut g = IndexGapBuffer::new_internal(2, 4);
        g.insert_internal(YjsSpan { id: 1.into(), origin_left: 0, origin_right: 0 }, 0, 0, 0, &[1]);
        g.print_states();
        g.dbg_check();
        // dbg!(&g);
    }

    #[test]
    fn split() {
        let mut g = IndexGapBuffer::new_with_num_indexes(1);
        g.insert(YjsSpan {
            id: (0..10).into(),
            origin_left: 0,
            origin_right: 0,
        }, 0, 0, &[]);

        g.move_gap_and_split(0, 5);
        g.dbg_check();
        g.print_states();
        dbg!(&g);
    }

    #[test]
    fn move_gap_fuzzer() {
        // This test puts some junk in a gap buffer, then furiously moves around the gap to see if
        // anything breaks.
        let mut g = IndexGapBuffer::new_raw(2, 16);
        g.index_info[0].active = true;
        g.index_info[1].active = true;
        g.dbg_check();
        g.insert(YjsSpan {
            id: (0..10).into(),
            origin_left: 0,
            origin_right: 0,
        }, 0, 0, &[]);
        g.insert(YjsSpan {
            id: (20..30).into(),
            origin_left: 0,
            origin_right: 0,
        }, 10, 0, &[1]);
        g.insert(YjsSpan {
            id: (50..60).into(),
            origin_left: 0,
            origin_right: 0,
        }, 20, 0, &[]);

        g.dbg_check();
        g.print_states();

        assert_eq!(g.count_len(0), 30);
        assert_eq!(g.count_len(1), 10);

        let expected_idx_0 = g.iter_merged(0).collect::<Vec<_>>();
        let expected_idx_1 = g.iter_merged(1).collect::<Vec<_>>();

        let len = g.count_len(0);

        let mut rng = SmallRng::seed_from_u64(122);
        for _i in 0..100 {
            let pos = rng.gen_range(0..len+1);
            let (index, offset) = g.find(0, pos, true);
            // dbg!(pos, (index, offset));
            g.move_gap_and_split(index, offset);
            g.print_states();

            let actual_idx_0 = g.iter_merged(0).collect::<Vec<_>>();
            // dbg!(&actual_idx_0);
            assert_eq!(expected_idx_0, actual_idx_0);
            let actual_idx_1 = g.iter_merged(1).collect::<Vec<_>>();
            assert_eq!(expected_idx_1, actual_idx_1);

            g.dbg_check();
        }
    }
}
