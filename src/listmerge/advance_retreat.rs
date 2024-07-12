use std::mem::replace;

use rle::{HasLength, RleDRun, SplitableSpan};

use crate::dtrange::DTRange;
use crate::list::operation::ListOpKind;
use crate::list::operation::ListOpKind::{Del, Ins};
use crate::listmerge::M2Tracker;
use crate::listmerge::markers::Marker;
use crate::listmerge::merge::notify_for;
use crate::LV;
use crate::ost::LeafIdx;
use crate::rev_range::RangeRev;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct QueryResultNew {
    tag: ListOpKind,
    target: RangeRev,
    offset: usize,
    leaf_idx: LeafIdx,
}

impl M2Tracker {
    /// Returns what happened here, target range, offset into range and a cursor into the range
    /// tree.
    ///
    /// This should only be used with times we have advanced through.
    ///
    /// Returns (ins / del, target, offset into target, rev, range_tree cursor).
    fn index_query_new(&self, lv: LV) -> QueryResultNew {
        debug_assert_ne!(lv, usize::MAX);

        let RleDRun {
            start, end, val: marker
        } = self.index.get_entry(lv);

        let offset = lv - start;
        let len = end - start;

        match marker {
            Marker::InsPtr(leaf_idx) => {
                debug_assert!(leaf_idx.exists());
                // For inserts, the target is simply the range of the item.
                // let start = lv - cursor.offset;
                QueryResultNew {
                    tag: Ins,
                    target: (start..end).into(),
                    offset,
                    leaf_idx,
                }
            }
            Marker::Del(target) => {
                let rr = RangeRev {
                    span: if target.fwd {
                        (target.target..target.target + len).into()
                    } else {
                        (target.target - len..target.target).into()
                    },
                    fwd: target.fwd,
                };
                QueryResultNew {
                    tag: Del,
                    target: rr,
                    offset,
                    leaf_idx: LeafIdx::default(),
                }
            }
        }
    }

    pub(crate) fn advance_by_range_new(&mut self, mut range: DTRange) {
        while !range.is_empty() {
            // Note the delete could be reversed - but we don't really care here; we just mark the
            // whole range anyway.
            // let (tag, target, mut len) = self.next_action(range.start);
            let QueryResultNew {
                tag,
                target,
                offset,
                mut leaf_idx,
            } = self.index_query_new(range.start);

            let len = usize::min(target.len() - offset, range.len());

            // If the target span is reversed, the part of target we eat each iteration changes.
            let mut target_range = target.range(offset, offset + len);

            // let mut len_remaining = len;
            while !target_range.is_empty() {
                // We'll only get a leaf pointer when we're inserting. Note we can't reuse the leaf
                // ptr across subsequent invocations because we mutate the range_tree.
                // let ptr = ptr.take().unwrap_or_else(|| self.old_marker_at(target_range.start));
                let leaf_idx = match replace(&mut leaf_idx, LeafIdx::default()) {
                    LeafIdx(usize::MAX) => self.marker_at(target_range.start),
                    x => x,
                };
                let (mut cursor, _pos) = self.range_tree.mut_cursor_before_item(target_range.start, leaf_idx);
                target_range.start += self.range_tree.mutate_entry(
                    &mut cursor,
                    target_range.len(),
                    &mut notify_for(&mut self.index),
                    |e| {
                        if tag == Ins {
                            e.current_state.mark_inserted();
                        } else {
                            e.delete();
                        }
                    }
                ).0;

                // TODO: Emplace it if we can.
                // cursor.flush(&mut self.range_tree);
                self.range_tree.emplace_cursor_unknown(cursor);
            }

            range.truncate_keeping_right(len);
        }
    }


    fn retreat_by_range_new(&mut self, mut range: DTRange) {
        // We need to go through the range in reverse order to make sure if we visit an insert then
        // delete of the same item, we un-delete before un-inserting.
        // TODO: Could probably relax this restriction when I feel more comfortable about overall
        // correctness.

        while !range.is_empty() {
            // TODO: This is gross. Clean this up. There's totally a nicer way to write this.
            let last_lv = range.last();

            if let Some(mut cursor) = self.range_tree.try_find_item(last_lv) {
                // Try just modifying the item directly.
                //
                // The item will only exist in the range tree at all if it was an insert.
                let (e, _offset) = cursor.0.get_item(&self.range_tree);
                // let chunk_start = last_lv - offset;
                let start = range.start.max(e.id.start);
                cursor.0.offset = start - e.id.start;
                let max_len = range.end - start;

                range.end -= self.range_tree.mutate_entry(
                    &mut cursor,
                    max_len,
                    &mut notify_for(&mut self.index),
                    |e| {
                        e.current_state.mark_not_inserted_yet();
                    }
                ).0;
                self.range_tree.emplace_cursor_unknown(cursor);
            } else {
                // Figure it out the "slow" way, by looking up the item in the index.
                let QueryResultNew { tag, target, offset, mut leaf_idx } = self.index_query_new(last_lv);

                let chunk_start = last_lv - offset;
                let start = range.start.max(chunk_start);
                let end = usize::min(range.end, chunk_start + target.len());

                let e_offset = start - chunk_start; // Usually 0.

                let len = end - start;
                debug_assert!(len <= range.len());
                range.end -= len;

                let mut target_range = target.range(e_offset, e_offset + len);

                while !target_range.is_empty() {
                    // STATS.with(|s| {
                    //     let mut s = s.borrow_mut();
                    //     s.2 += 1;
                    // });

                    // Because the tag is either entirely delete or entirely insert, its safe to move
                    // forwards in this child range. (Which I'm doing because that makes the code much
                    // easier to reason about).

                    // We can't reuse the pointer returned by the index_query call because we mutate
                    // each loop iteration.

                    let leaf_idx = match replace(&mut leaf_idx, LeafIdx::default()) {
                        LeafIdx(usize::MAX) => self.marker_at(target_range.start),
                        x => x,
                    };
                    // let mut cursor = self.old_range_tree.mut_cursor_before_item(target_range.start, ptr);
                    let (mut cursor, _pos) = self.range_tree.mut_cursor_before_item(target_range.start, leaf_idx);

                    target_range.start += self.range_tree.mutate_entry(
                        &mut cursor,
                        target_range.len(),
                        &mut notify_for(&mut self.index),
                        |e| {
                            if tag == Ins {
                                e.current_state.mark_not_inserted_yet();
                            } else {
                                e.current_state.undelete();
                            }
                        }
                    ).0;

                    // TODO: Emplace it if we can.
                    // cursor.flush(&mut self.range_tree);
                    self.range_tree.emplace_cursor_unknown(cursor);
                }
            }
        }

        // self.check_index();
    }
}

impl M2Tracker {
    pub(crate) fn advance_by_range(&mut self, range: DTRange) {
        // self.advance_by_range_old(range);
        self.advance_by_range_new(range);
    }
    pub(crate) fn retreat_by_range(&mut self, range: DTRange) {
        // self.retreat_by_range_old(range);
        self.retreat_by_range_new(range);
    }
}