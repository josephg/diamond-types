use std::mem::replace;
use std::ptr::NonNull;
use content_tree::NodeLeaf;
use rle::{HasLength, RleDRun, SplitableSpan};
use crate::listmerge::{DocRangeIndex, M2Tracker};
use crate::listmerge::merge::{new_notify_for, old_notify_for};
use crate::rev_range::RangeRev;
use crate::listmerge::yjsspan::CRDTSpan;
use crate::list::operation::ListOpKind;
use crate::list::operation::ListOpKind::{Del, Ins};
use crate::dtrange::{DTRange, UNDERWATER_START};
use crate::listmerge::markers::{Marker, Marker2};
use crate::LV;
use crate::ost::LeafIdx;

// #[derive(Debug, Eq, PartialEq)]
// pub(super) struct QueryResultOld {
//     tag: ListOpKind,
//     target: RangeRev,
//     offset: usize,
//     ptr: Option<NonNull<NodeLeaf<CRDTSpan, DocRangeIndex>>>
// }
// 
// impl M2Tracker {
//     /// Returns what happened here, target range, offset into range and a cursor into the range
//     /// tree.
//     ///
//     /// This should only be used with times we have advanced through.
//     ///
//     /// Returns (ins / del, target, offset into target, rev, range_tree cursor).
//     fn index_query_old(&self, lv: LV) -> QueryResultOld {
//         debug_assert_ne!(lv, usize::MAX);
// 
//         let RleDRun {
//             start, end, val: marker
//         } = self.old_index.get_entry(lv);
// 
//         // println!("{:?}", marker);
//         // dbg!(&self.index.index2);
// 
//         let offset = lv - start;
//         let len = end - start;
// 
//         match marker {
//             Marker::InsPtr(ptr) => {
//                 debug_assert!(ptr != NonNull::dangling());
//                 // For inserts, the target is simply the range of the item.
//                 // let start = lv - cursor.offset;
//                 QueryResultOld {
//                     tag: Ins,
//                     target: (start..end).into(),
//                     offset,
//                     ptr: Some(ptr)
//                 }
//             }
//             Marker::Del(target) => {
//                 let rr = RangeRev {
//                     span: if target.fwd {
//                         (target.target..target.target + len).into()
//                     } else {
//                         (target.target - len..target.target).into()
//                     },
//                     fwd: target.fwd,
//                 };
//                 QueryResultOld { tag: Del, target: rr, offset, ptr: None }
//             }
//         }
//     }
// 
//     pub(crate) fn advance_by_range_old(&mut self, mut range: DTRange) {
//         while !range.is_empty() {
//             // Note the delete could be reversed - but we don't really care here; we just mark the
//             // whole range anyway.
//             // let (tag, target, mut len) = self.next_action(range.start);
//             let QueryResultOld {
//                 tag,
//                 target,
//                 offset,
//                 mut ptr
//             } = self.index_query_old(range.start);
// 
//             let len = usize::min(target.len() - offset, range.len());
// 
//             // If the target span is reversed, the part of target we eat each iteration changes.
//             let mut target_range = target.range(offset, offset + len);
// 
//             // let mut len_remaining = len;
//             while !target_range.is_empty() {
//                 // We'll only get a pointer when we're inserting. Note we can't reuse the ptr
//                 // across subsequent invocations because we mutate the range_tree.
//                 let ptr = ptr.take().unwrap_or_else(|| self.old_marker_at(target_range.start));
//                 let mut cursor = self.old_range_tree.mut_cursor_before_item(target_range.start, ptr);
//                 target_range.start += cursor.mutate_single_entry_notify(
//                     target_range.len(),
//                     old_notify_for(&mut self.old_index),
//                     |e| {
//                         if tag == ListOpKind::Ins {
//                             e.current_state.mark_inserted();
//                         } else {
//                             e.delete();
//                         }
//                     }
//                 ).0;
//             }
// 
//             range.truncate_keeping_right(len);
//         }
//     }
// 
// 
//     fn retreat_by_range_old(&mut self, mut range: DTRange) {
//         // We need to go through the range in reverse order to make sure if we visit an insert then
//         // delete of the same item, we un-delete before un-inserting.
//         // TODO: Could probably relax this restriction when I feel more comfortable about overall
//         // correctness.
// 
//         while !range.is_empty() {
//             // TODO: This is gross. Clean this up. There's totally a nicer way to write this.
//             let req_time = range.last();
//             let QueryResultOld { tag, target, offset, mut ptr } = self.index_query_old(req_time);
// 
//             let chunk_start = req_time - offset;
//             let start = range.start.max(chunk_start);
//             let end = usize::min(range.end, chunk_start + target.len());
// 
//             let e_offset = start - chunk_start; // Usually 0.
// 
//             let len = end - start;
//             debug_assert!(len <= range.len());
//             range.end -= len;
// 
//             let mut target_range = target.range(e_offset, e_offset + len);
// 
//             while !target_range.is_empty() {
//                 // STATS.with(|s| {
//                 //     let mut s = s.borrow_mut();
//                 //     s.2 += 1;
//                 // });
// 
//                 // Because the tag is either entirely delete or entirely insert, its safe to move
//                 // forwards in this child range. (Which I'm doing because that makes the code much
//                 // easier to reason about).
// 
//                 // We can't reuse the pointer returned by the index_query call because we mutate
//                 // each loop iteration.
//                 let ptr = ptr.take().unwrap_or_else(|| self.old_marker_at(target_range.start));
//                 let mut cursor = self.old_range_tree.mut_cursor_before_item(target_range.start, ptr);
// 
//                 target_range.start += cursor.mutate_single_entry_notify(
//                     target_range.len(),
//                     old_notify_for(&mut self.old_index),
//                     |e| {
//                         if tag == ListOpKind::Ins {
//                             e.current_state.mark_not_inserted_yet();
//                         } else {
//                             e.current_state.undelete();
//                         }
//                     }
//                 ).0;
//             }
//         }
// 
//         // self.check_index();
//     }
// }

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
        } = self.new_index.get_entry(lv);

        // println!("{:?}", marker);
        // dbg!(&self.index.index2);

        let offset = lv - start;
        let len = end - start;

        match marker {
            Marker2::InsPtr(leaf_idx) => {
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
            Marker2::Del(target) => {
                let rr = RangeRev {
                    span: if target.fwd {
                        (target.target..target.target + len).into()
                    } else {
                        (target.target - len..target.target).into()
                    },
                    fwd: target.fwd,
                };
                QueryResultNew{ tag: Del, target: rr, offset, leaf_idx: LeafIdx::default() }
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
                    LeafIdx(usize::MAX) => self.new_marker_at(target_range.start),
                    x => x,
                };
                let (mut cursor, _pos) = self.new_range_tree.mut_cursor_before_item(target_range.start, leaf_idx);
                target_range.start += self.new_range_tree.mutate_entry(
                    &mut cursor,
                    target_range.len(),
                    &mut new_notify_for(&mut self.new_index),
                    |e| {
                        if tag == Ins {
                            e.current_state.mark_inserted();
                        } else {
                            e.delete();
                        }
                    }
                ).0;

                // TODO: Emplace it if we can.
                cursor.flush(&mut self.new_range_tree);
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
            let req_time = range.last();
            let QueryResultNew { tag, target, offset, mut leaf_idx } = self.index_query_new(req_time);

            let chunk_start = req_time - offset;
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
                // let ptr = ptr.take().unwrap_or_else(|| self.old_marker_at(target_range.start));
                let leaf_idx = match replace(&mut leaf_idx, LeafIdx::default()) {
                    LeafIdx(usize::MAX) => self.new_marker_at(target_range.start),
                    x => x,
                };
                // let mut cursor = self.old_range_tree.mut_cursor_before_item(target_range.start, ptr);
                let (mut cursor, _pos) = self.new_range_tree.mut_cursor_before_item(target_range.start, leaf_idx);

                target_range.start += self.new_range_tree.mutate_entry(
                    &mut cursor,
                    target_range.len(),
                    &mut new_notify_for(&mut self.new_index),
                    |e| {
                        if tag == Ins {
                            e.current_state.mark_not_inserted_yet();
                        } else {
                            e.current_state.undelete();
                        }
                    }
                ).0;

                // TODO: Emplace it if we can.
                cursor.flush(&mut self.new_range_tree);
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