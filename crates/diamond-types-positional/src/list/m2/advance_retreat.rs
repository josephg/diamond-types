use content_tree::{ContentTreeRaw, Toggleable};
use rle::{HasLength, SplitableSpan};
use crate::list::m2::M2Tracker;
use crate::list::m2::merge::notify_for;
use crate::list::operation::InsDelTag;
use crate::localtime::TimeSpan;
use crate::rle::KVPair;

impl M2Tracker {
    pub(crate) fn advance_by_range(&mut self, mut range: TimeSpan) {
        while !range.is_empty() {
            // Note the delete could be reversed - but we don't really care here; we just mark the
            // whole range anyway.
            // let (tag, target, mut len) = self.next_action(range.start);
            let (tag, mut target, offset, ptr) = self.index_query(range.start);
            target.truncate_keeping_right(offset);
            let len = target.len().min(range.len());

            // let mut cursor = self.get_unsafe_cursor_before(target);

            let amt_modified = unsafe {
                // We'll only get a pointer when we're inserting.
                let ptr = ptr.unwrap_or_else(|| self.marker_at(target.span.start));
                let mut cursor = ContentTreeRaw::cursor_before_item(target.span.start, ptr);
                ContentTreeRaw::unsafe_mutate_single_entry_notify(|e| {
                    if tag == InsDelTag::Ins {
                        e.state.mark_inserted();
                    } else {
                        e.delete();
                    }
                }, &mut cursor, len, notify_for(&mut self.index))
            };

            range.truncate_keeping_right(amt_modified);
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
            let (tag, mut target, offset, ptr) = self.index_query(req_time);
            let e_start = req_time - offset;
            let start = range.start.max(e_start);
            let e_offset = start - e_start;
            target.truncate_keeping_right(e_offset); // Only if e_offset > 0?

            let mut len = target.len().min(range.len());
            // debug_assert_eq!(offset - e_offset + 1, len);

            // dbg!(range, tag, target, len);
            // len = len.min(range.len());
            debug_assert!(len <= range.len());

            range.end -= len;

            let mut next = target.span.start;
            while len > 0 {
                // Because the tag is either entirely delete or entirely insert, its safe to move forwards.
                // dbg!(target, &self.range_tree);
                // let mut cursor = self.get_unsafe_cursor_before(target);

                unsafe {
                    // dbg!(next);
                    // We can't actually use the pointer returned by the index_query call because we
                    // mutate each loop iteraton.
                    let ptr = self.marker_at(next);
                    let mut cursor = ContentTreeRaw::cursor_before_item(next, ptr);
                    // let mut cursor = ContentTreeRaw::cursor_before_item(next, ptr);
                    let amt_modified = ContentTreeRaw::unsafe_mutate_single_entry_notify(|e| {
                        if tag == InsDelTag::Ins {
                            e.state.mark_not_inserted_yet();
                        } else {
                            e.state.undelete();
                        }
                    }, &mut cursor, len, notify_for(&mut self.index));

                    // dbg!(amt_modified);
                    next += amt_modified;
                    len -= amt_modified;
                }
            }
        }

        self.check_index();
    }
}