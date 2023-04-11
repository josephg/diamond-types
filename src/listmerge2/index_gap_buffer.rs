use std::cmp::Ordering;
use std::ops::Range;
use rle::{HasLength, MergableSpan, SplitableSpan};
use crate::frontier::is_sorted_slice;
use crate::list::op_iter::OpMetricsIter;
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::operation::ListOpKind;
use crate::listmerge2::action_plan::{MergePlan, MergePlanAction};
use crate::listmerge2::Index;
use crate::listmerge2::yjsspan::{YjsSpan, SpanState};
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

impl IndexGapBuffer {
    fn new_internal(num_indexes: usize, starting_buf_size: usize) -> Self {
        assert!(num_indexes >= 1);
        assert!(starting_buf_size >= 2);

        let mut result = Self {
            items: vec![YjsSpan::default(); starting_buf_size],
            states: vec![SpanState::NotInsertedYet; starting_buf_size * num_indexes],
            gap_start_idx: 0,
            gap_end_idx: starting_buf_size,
            index_info: vec![IndexInfo::default(); num_indexes],
        };

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
        // Self::new_internal(num_indexes, 16)
        Self::new_internal(num_indexes, 256)
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
        for i in (0..num_indexes).rev() {
            if self.index_info[i].active {
                let old_start = old_size * i;
                let new_start = new_size * i;
                self.states.copy_within(old_start + self.gap_end_idx..old_start + old_size, new_start + new_gap_end);
                self.states.copy_within(old_start..old_start + self.gap_start_idx, new_start);
            }
        }
        self.gap_end_idx = new_gap_end;

        // We doubled - so we grew by old_size.
        old_size
    }

    fn move_gap(&mut self, new_gap_i: usize) {
        // TODO: This method currently doesn't merge adjacent items when the gap closes. Consider
        // adding that!
        let count_moved_size = |states: &[SpanState], moved_range: Range<usize>, base: usize| -> (usize, Range<usize>) {
            let r2 = moved_range.start+base..moved_range.end+base;
            let mut moved_size = 0;
            for (span, state) in self.items[moved_range.clone()].iter()
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
                let moved_range = new_gap_i..self.gap_start_idx;
                let new_gap_end = self.gap_end_idx - moved_range.len();

                // The annoying part: Updating all the indexes and states.
                let mut base = 0;
                let items_len = self.items.len();
                for index_info in self.index_info.iter_mut() {
                    let (moved_size, r2) = count_moved_size(&self.states, moved_range.clone(), base);
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
                let num_moved_items = new_gap_i - self.gap_end_idx;
                if num_moved_items == 0 { return; } // Nothing to do!

                let moved_range = self.gap_end_idx..new_gap_i;

                // Update the indexes and states. Code adapted from above.
                let mut base = 0;
                let items_len = self.items.len();
                for index_info in self.index_info.iter_mut() {
                    let (moved_size, r2) = count_moved_size(&self.states, moved_range.clone(), base);
                    self.states.copy_within(r2, self.gap_start_idx+base);
                    index_info.before_gap_len += moved_size;
                    base += items_len;
                }

                // Move the items themselves.
                self.items.copy_within(moved_range.clone(), self.gap_start_idx);

                self.gap_start_idx += num_moved_items;
                self.gap_end_idx = new_gap_i;
            }
        }
    }

    fn gap_size(&self) -> usize {
        self.gap_end_idx - self.gap_start_idx
    }

    // State for the new item is implicitly Inserted.
    fn insert(&mut self, new_item: YjsSpan, mut i: usize, mut offset: usize, update_index: Index, other_indexes: &[Index]) {
        // Indexes must be sorted.
        debug_assert!(is_sorted_slice::<true, _>(other_indexes));
        debug_assert!(!other_indexes.contains(&update_index));

        // assert!(i < self.item_len() || i == self.item_len() && offset == 0);

        // Could roll back inserts with offset=0 to the start of the next item, but I don't think
        // the logic below would use that hinge anyway.
        assert!(i < self.items.len());
        assert!(i < self.gap_start_idx || i >= self.gap_end_idx);

        let all_inserts = |i: usize| -> bool {
            for (index, info) in self.index_info.iter().enumerate() {
                if !info.active { continue; }
                if self.states[self.state_idx_at(index, i)] != SpanState::Inserted { return false; }
            }
            true
        };

        let new_item_len = new_item.len();

        let item = &self.items[i];

        if new_item.is_undiff() && item.is_undiff() && all_inserts(i) {
            // No sweat.
            self.items[i].append(new_item);
            self.add_to_gap_len(i, update_index, other_indexes, new_item_len);
            return;
        }

        if offset == item.len() {
            // Try to simply append the new item to the end of the existing item.
            if item.can_append(&new_item) && all_inserts(i) {
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

            if next_item_idx < self.items.len() && new_item.can_append(&self.items[next_item_idx]) && all_inserts(next_item_idx) {
                self.items[next_item_idx].prepend(new_item);
                self.add_to_gap_len(i, update_index, other_indexes, new_item_len);
                return;
            }
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
        self.set_item_state_inserted(i, update_index, other_indexes, new_item_len);
        self.gap_start_idx += 1;
        self.add_to_gap_len(i, update_index, other_indexes, new_item_len);

        if let Some((rem, mut rem_idx)) = remainder {
            if rem_idx >= self.gap_start_idx { rem_idx += grew_by; }
            self.items[i + 1] = rem;
            self.copy_item_state(rem_idx, i + 1);
            self.gap_start_idx += 1;
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

    // TODO: Refactor / adapt this when I implement deleting stuff.
    fn set_item_state_inserted(&mut self, mut i: usize, update_index: Index, other_indexes: &[Index], _item_len: usize) {
        // Indexes must be sorted.
        debug_assert!(is_sorted_slice::<true, _>(other_indexes));
        debug_assert!(!other_indexes.contains(&update_index));

        let mut other_i = 0;
        let items_len = self.items.len();
        for index in 0..self.index_info.len() {
            let state = if other_i < other_indexes.len() && other_indexes[other_i] == index {
                other_i += 1;
                SpanState::Inserted
            } else if index == update_index {
                SpanState::Inserted
            } else {
                SpanState::NotInsertedYet
            };

            self.states[i] = state;

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


    /// If there's a run of items with content length 0, if prefer_first is true we'll return
    /// the first position which matches the content length. Otherwise we'll return the last
    /// content position which matches.
    fn find(&self, index: Index, mut content_pos: usize, prefer_first: bool) -> (usize, usize) {
        debug_assert!(self.index_info[index].active);
        let gap_pos = self.index_info[index].before_gap_len;
        let index_start = self.start_state_idx(index);

        if content_pos < gap_pos || (prefer_first && content_pos == gap_pos) {
            // We're looking for the item in the first half of the gap buffer.
            if prefer_first && content_pos == 0 { return (0, 0); }

            // TODO: This scans from the start, but the desired position will often be closer to
            // the gap position rather than close to the start of the whole thing. Rewrite this to
            // scan backwards from the gap based on a heuristic.
            // let mut pos = 0;
            for (i, (span, state)) in self.items[..self.gap_start_idx].iter()
                .zip(self.states[index_start..index_start + self.gap_start_idx].iter().copied())
                .enumerate()
            {
                if state != SpanState::Inserted { continue; }

                let len = span.len();
                if len > content_pos || (prefer_first && len == content_pos) {
                    return (i, content_pos);
                }
                content_pos -= span.len();
            }

            if !prefer_first && content_pos == 0 { return (self.gap_start_idx, 0); }
            unreachable!("Content position overflowed gap_pos")
        } else {
            content_pos -= gap_pos;
            let index_after_gap = index_start + self.gap_end_idx;
            for (i, (span, state)) in self.items[index_after_gap..].iter()
                .zip(self.states[index_after_gap..index_start + self.items.len()].iter().copied())
                .enumerate()
            {
                // TODO: This loop body copied from above. Refactor to avoid that!
                if state != SpanState::Inserted { continue; }

                let len = span.len();
                if len > content_pos || (prefer_first && len == content_pos) {
                    return (i, content_pos);
                }
                content_pos -= span.len();
            }

            if !prefer_first && content_pos == 0 {
                // Essentially we've been asked for the index of the *next* item, but no such
                // position exists. Bail!
                panic!("Requested last item and prefer_first is false.");
            }
            panic!("Requested content position is past end of list");
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

// #[derive(
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
    use super::*;

    #[test]
    fn smoke_test() {
        let mut g = IndexGapBuffer::new_with_num_indexes(1);
        g.dbg_check();
        g.insert(YjsSpan {
            id: (1..10).into(),
            origin_left: 0,
            origin_right: 0,
        }, 0, 0, 0, &[]);
        g.dbg_check();
        g.insert(YjsSpan {
            id: (10..20).into(),
            origin_left: 0,
            origin_right: 0,
        }, 3, 1, 0, &[]);
        // }, 0, 20, 0, &[]);
        // dbg!(&g);
        g.dbg_check();
    }

    #[test]
    fn resize() {
        // let mut g = IndexGapBuffer::new_internal(1, 2);
        // g.dbg_check();
        // g.insert(YjsSpan { id: 1.into(), origin_left: 0, origin_right: 0 }, 0, 1, 0, &[]);
        // g.dbg_check();

        let mut g = IndexGapBuffer::new_internal(1, 2);
        g.insert(YjsSpan { id: 1.into(), origin_left: 0, origin_right: 0 }, 0, 0, 0, &[]);
        g.insert(YjsSpan { id: 2.into(), origin_left: 0, origin_right: 0 }, 0, 0, 0, &[]);
        g.dbg_check();
        g.print_states();
        dbg!(&g);
    }

    #[test]
    fn merge_undiff() {
        let mut g = IndexGapBuffer::new_internal(1, 4);
        g.insert(YjsSpan::new_undiff(1), 0, 1, 0, &[]);
        // g.insert(YjsSpan { id: 2.into(), origin_left: 0, origin_right: 0 }, 0, 0, 0, &[]);
        g.dbg_check();
        // g.print_states();

        assert_eq!(g.active_items(), 1);
        // dbg!(&g);

    }

    #[test]
    fn simple_insert_with_other_indexes() {
        let mut g = IndexGapBuffer::new_internal(2, 4);
        g.insert(YjsSpan { id: 1.into(), origin_left: 0, origin_right: 0 }, 0, 0, 0, &[1]);
        g.print_states();
        g.dbg_check();
        // dbg!(&g);
    }
}
