use content_tree::{ContentTreeRaw, Toggleable};
use rle::{HasLength, SplitableSpan};
use crate::list::m2::M2Tracker;
use crate::list::m2::merge::notify_for;
use crate::list::operation::InsDelTag;
use crate::localtime::TimeSpan;
use crate::rle::KVPair;

impl M2Tracker {
    pub(crate) fn advance_by_range(&mut self, mut range: TimeSpan) {
        let x = range.contains(99);
        while !range.is_empty() {
            // Note the delete could be reversed - but we don't really care here; we just mark the
            // whole range anyway.
            // let (tag, target, mut len) = self.next_action(range.start);
            let (tag, mut target, offset, ptr) = self.index_query(range.start);

            let len = usize::min(target.len() - offset, range.len());

            // If the target span is reversed, we only really want the
            // dbg!((range, tag, target, offset, len), target.range(offset, offset + len));
            // let target_start = target.range(offset, len).start;
            let mut target_start = target.range(offset, offset + len).start;

            // let t1 = target.range(offset, len).start;
            // let t2 = target.range(offset, offset + len).start;
            // let b = t1 != t2;

            // let mut cursor = self.get_unsafe_cursor_before(target);

            let mut len_remaining = len;
            while len_remaining > 0 {
                let amt_modified = unsafe {
                    // We'll only get a pointer when we're inserting.
                    let ptr = ptr.unwrap_or_else(|| self.marker_at(target_start));
                    let mut cursor = ContentTreeRaw::cursor_before_item(target_start, ptr);
                    ContentTreeRaw::unsafe_mutate_single_entry_notify(|e| {
                        if tag == InsDelTag::Ins {
                            println!("Re-inserting {:?}", e.id);
                            e.state.mark_inserted();
                        } else {
                            println!("Re-deleting {:?}", e.id);
                            e.delete();
                        }
                    }, &mut cursor, len_remaining, notify_for(&mut self.index)).0
                };
                target_start += amt_modified;
                len_remaining -= amt_modified;
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
            let (tag, mut target, offset, ptr) = self.index_query(req_time);
            let e_start = req_time - offset;

            let start = range.start.max(e_start);
            let end = usize::min(range.end, e_start + target.len());

            let e_offset = start - e_start;

            let len = end - start;
            // dbg!((&range, &target, e_offset, len));
            // target.truncate_keeping_right(e_offset);
            let target_start = target.range(e_offset, e_offset + len).start;

            // debug_assert_eq!(offset - e_offset + 1, len);

            // dbg!((&self.range_tree, &self.index));
            // dbg!((range, tag, target, len));
            // len = len.min(range.len());
            debug_assert!(len <= range.len());

            let new_end = range.end - len; // TODO: Hack. Just update range here.

            let mut next = target_start; // TODO: Inline?
            let mut len_remaining = len; // TODO: Inline.
            while len_remaining > 0 {
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
                            // println!("Uninserting {:?}", e.id);
                            e.state.mark_not_inserted_yet();
                        } else {
                            // println!("Undeleting {:?}", e.id);
                            e.state.undelete();
                        }
                    }, &mut cursor, len_remaining, notify_for(&mut self.index)).0;

                    // dbg!(amt_modified);
                    next += amt_modified;
                    len_remaining -= amt_modified;
                }
            }

            range.end = new_end;
        }

        self.check_index();
    }
}