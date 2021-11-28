use std::ptr::NonNull;
use content_tree::{ContentTreeRaw, DEFAULT_IE, DEFAULT_LE, NodeLeaf};
use rle::{HasLength, SplitableSpan};
use crate::list::m2::{DocRangeIndex, M2Tracker};
use crate::list::m2::markers::Marker::{DelTarget, InsPtr};
use crate::list::m2::merge::notify_for;
use crate::list::m2::rev_span::TimeSpanRev;
use crate::list::m2::yjsspan2::YjsSpan2;
use crate::list::operation::InsDelTag;
use crate::list::operation::InsDelTag::{Del, Ins};
use crate::localtime::TimeSpan;
use crate::ROOT_TIME;

impl M2Tracker {
    /// Returns what happened here, target range, offset into range and a cursor into the range
    /// tree.
    ///
    /// This should only be used with times we have advanced through.
    ///
    /// Returns (ins / del, target, offset into target, rev, range_tree cursor).
    fn index_query(&self, time: usize) -> (InsDelTag, TimeSpanRev, usize, Option<NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>>>) {
        assert_ne!(time, ROOT_TIME); // Not sure what to do in this case.

        let index_len = self.index.offset_len();
        if time >= index_len {
            panic!("Index query past the end");
            // (Ins, (index_len..usize::MAX).into(), time - index_len, self.range_tree.unsafe_cursor_at_end())
        } else {
            let cursor = self.index.cursor_at_offset_pos(time, false);
            let entry = cursor.get_raw_entry();

            match entry.inner {
                InsPtr(ptr) => {
                    // For inserts, the target is simply the range of the item.
                    let start = time - cursor.offset;
                    (Ins, (start..start+entry.len).into(), cursor.offset, Some(ptr))
                }
                DelTarget(target) => {
                    (Del, target, cursor.offset, None)
                }
            }
        }
    }

    pub(crate) fn advance_by_range(&mut self, mut range: TimeSpan) {
        while !range.is_empty() {
            // Note the delete could be reversed - but we don't really care here; we just mark the
            // whole range anyway.
            // let (tag, target, mut len) = self.next_action(range.start);
            let (tag, target, offset, ptr) = self.index_query(range.start);

            let len = usize::min(target.len() - offset, range.len());

            // If the target span is reversed, the part of target we eat each iteration changes.
            let mut target_range = target.range(offset, offset + len);

            // let mut len_remaining = len;
            while !target_range.is_empty() {
                let amt_modified = unsafe {
                    // We'll only get a pointer when we're inserting.
                    let ptr = ptr.unwrap_or_else(|| self.marker_at(target_range.start));
                    let mut cursor = ContentTreeRaw::unsafe_cursor_before_item(target_range.start, ptr);
                    ContentTreeRaw::unsafe_mutate_single_entry_notify(|e| {
                        if tag == InsDelTag::Ins {
                            // println!("Re-inserting {:?}", e.id);
                            e.state.mark_inserted();
                        } else {
                            // println!("Re-deleting {:?}", e.id);
                            e.delete();
                        }
                    }, &mut cursor, target_range.len(), notify_for(&mut self.index)).0
                };
                target_range.start += amt_modified;
            }

            range.truncate_keeping_right(len);
        }
    }


    pub(crate) fn retreat_by_range(&mut self, mut range: TimeSpan) {
        // We need to go through the range in reverse order to make sure if we visit an insert then
        // delete of the same item, we un-delete before un-inserting.
        // TODO: Could probably relax this restriction when I feel more comfortable about overall
        // correctness.

        while !range.is_empty() {
            // TODO: This is gross. Clean this up. There's totally a nicer way to write this.
            let req_time = range.last();
            let (tag, target, offset, _ptr) = self.index_query(req_time);

            let chunk_start = req_time - offset;
            let start = range.start.max(chunk_start);
            let end = usize::min(range.end, chunk_start + target.len());

            let e_offset = start - chunk_start; // Usually 0.

            let len = end - start;
            debug_assert!(len <= range.len());
            range.end -= len;

            let mut target_range = target.range(e_offset, e_offset + len);
            
            while !target_range.is_empty() {
                // Because the tag is either entirely delete or entirely insert, its safe to move
                // forwards in this child range. (Which I'm doing because that makes the code much
                // easier to reason about).

                // We can't actually use the pointer returned by the index_query call because we
                // mutate each loop iteraton.

                // TODO: We probably just fetched this pointer above. Reuse that!
                let ptr = self.marker_at(target_range.start);
                let mut cursor = self.range_tree.cursor_before_item(target_range.start, ptr);
                unsafe {
                    let amt_modified = ContentTreeRaw::unsafe_mutate_single_entry_notify(|e| {
                        if tag == InsDelTag::Ins {
                            // println!("Uninserting {:?}", e.id);
                            e.state.mark_not_inserted_yet();
                        } else {
                            // println!("Undeleting {:?}", e.id);
                            e.state.undelete();
                        }
                    }, &mut cursor, target_range.len(), notify_for(&mut self.index)).0;

                    target_range.start += amt_modified;
                }
            }
        }

        self.check_index();
    }
}