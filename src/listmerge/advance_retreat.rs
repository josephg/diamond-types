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
use crate::stats::marker_a;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct QueryResult {
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
    fn index_query(&self, lv: LV) -> QueryResult {
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
                QueryResult {
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
                QueryResult {
                    tag: Del,
                    target: rr,
                    offset,
                    leaf_idx: LeafIdx::default(),
                }
            }
        }
    }

    fn adv_retreat_range(&mut self, mut range: DTRange, incr: i32) {
        // This method handles both advancing and retreating. In either case, because of the way
        // SpanState is designed, we need to either increment or decrement the state of every
        // visited item in the LV range.

        // Note: When retreating, we still visit all the items in the range in earliest-to-latest
        // order. This is a bit of a wild optimisation, because its possible (common even) for
        // the range to include an edit which inserts a character followed by an edit which deletes
        // the character. The obvious way to process that would be to first undo the delete event,
        // then undo the insert.
        //
        // However, that requires that we visit the range in reverse order, which has worse
        // performance and requires advance and retreat to be handled differently. So long as the
        // *result* of running retreat() is the same, its safe to not do that, and instead treat the
        // span state as an integer and just decrement it twice.
        while !range.is_empty() {
            if let Some(mut cursor) = self.range_tree.try_find_item(range.start) {
                // crate::stats::marker_a();


                // Try just modifying the item directly.
                //
                // The item will only exist in the range tree at all if it was an insert.
                let (e, _offset) = cursor.0.get_item(&self.range_tree);
                // let chunk_start = last_lv - offset;
                let start = range.start.max(e.id.start);
                cursor.0.offset = start - e.id.start;
                let max_len = range.end - start;

                range.start += self.range_tree.mutate_entry(
                    &mut cursor,
                    max_len,
                    &mut notify_for(&mut self.index),
                    |e| {
                        e.current_state.0 = e.current_state.0.wrapping_add_signed(incr);
                    }
                ).0;

                self.range_tree.emplace_cursor_unknown(cursor);

            } else {
                // crate::stats::marker_b();

                let QueryResult {
                    target,
                    offset,
                    mut leaf_idx,
                    ..
                } = self.index_query(range.start);

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
                            e.current_state.0 = e.current_state.0.wrapping_add_signed(incr);
                        }
                    ).0;

                    self.range_tree.emplace_cursor_unknown(cursor);
                }

                range.truncate_keeping_right(len);
            }
        }

    }

    pub(crate) fn advance_by_range(&mut self, range: DTRange) {
        self.adv_retreat_range(range, 1);
    }

    pub(crate) fn retreat_by_range(&mut self, range: DTRange) {
        self.adv_retreat_range(range, -1);
    }

    //         // if let Some(mut cursor) = self.range_tree.try_find_item(last_lv) {
    //         //     // Try just modifying the item directly.
    //         //     //
    //         //     // The item will only exist in the range tree at all if it was an insert.
    //         //     let (e, _offset) = cursor.0.get_item(&self.range_tree);
    //         //     // let chunk_start = last_lv - offset;
    //         //     let start = range.start.max(e.id.start);
    //         //     cursor.0.offset = start - e.id.start;
    //         //     let max_len = range.end - start;
    //         //
    //         //     range.end -= self.range_tree.mutate_entry(
    //         //         &mut cursor,
    //         //         max_len,
    //         //         &mut notify_for(&mut self.index),
    //         //         |e| {
    //         //             e.current_state.mark_not_inserted_yet();
    //         //         }
    //         //     ).0;
    //         //     self.range_tree.emplace_cursor_unknown(cursor);
    //         // } else {
}
