use content_tree::{ContentTreeRaw, Toggleable};
use rle::{HasLength, SplitableSpan};
use crate::list::m2::M2Tracker;
use crate::list::m2::merge::notify_for;
use crate::list::operation::InsDelTag;
use crate::localtime::TimeSpan;
use crate::rle::KVPair;

impl M2Tracker {
    /// Returns (tag, target, len)
    fn next_action(&self, time: usize) -> (InsDelTag, usize, usize) {
        let (d, offset) = self.deletes.find_sparse(time);

        match d {
            Ok(KVPair(time, del)) => {
                let mut del = *del;
                del.truncate_keeping_right(offset);
                (InsDelTag::Del, del.target.start, del.len())
                // (InsDelTag::Del, target.start + offset, target.len() - offset)
            }
            Err(ins_span) => {
                (InsDelTag::Ins, time, ins_span.len() - offset)
            }
        }
    }

    /// Returns (tag, target, len)
    fn last_action(&self, req_range: TimeSpan) -> (InsDelTag, usize, usize) {
        assert!(!req_range.is_empty());

        let (d, offset) = self.deletes.find_sparse(req_range.end - 1);

        match d {
            Ok(KVPair(actual_range_start, del)) => {
                // We've found a delete which matches, but the actual_range_start points to the last
                // item in the delete we found. We want to grab as many deleted items as possible.

                let del_op_start = req_range.start.max(*actual_range_start);
                let inner_offset = del_op_start - actual_range_start;

                let mut del = *del;
                del.truncate_keeping_right(inner_offset);
                debug_assert_eq!(offset - inner_offset + 1, del.len());
                (InsDelTag::Del, del.target.start, del.len())
                // (InsDelTag::Del, del.start + inner_offset, offset - inner_offset + 1)
            }
            Err(ins_span) => {
                let start = req_range.start.max(ins_span.start);
                let inner_offset = start - ins_span.start;

                (InsDelTag::Ins, ins_span.start + inner_offset, offset - inner_offset + 1)
            }
        }
    }

    pub(crate) fn advance_by_range(&mut self, mut range: TimeSpan) {
        while !range.is_empty() {
            // Note the delete could be reversed - but we don't really care here; we just mark the
            // whole range anyway.
            let (tag, target, mut len) = self.next_action(range.start);
            len = len.min(range.len());

            let mut cursor = self.get_unsafe_cursor_before(target);

            unsafe {
                let amt_modified = ContentTreeRaw::unsafe_mutate_entry_notify(|e| {
                    if tag == InsDelTag::Ins {
                        e.state.mark_inserted();
                    } else {
                        e.delete();
                    }
                }, &mut cursor, len, notify_for(&mut self.index));

                range.truncate_keeping_right(amt_modified);
            }
        }
    }

    pub(crate) fn retreat_by_range(&mut self, mut range: TimeSpan) {
        // We need to go through the range in reverse order.
        // TODO: This is pretty gross. Clean this up. There's totally a nicer way to write this.
        while !range.is_empty() {
            let (tag, mut target, mut len) = self.last_action(range);
            // dbg!(range, tag, target, len);
            // len = len.min(range.len());
            debug_assert!(len <= range.len());

            range.end -= len;

            while len > 0 {
                // Because the tag is either entirely delete or entirely insert, its safe to move forwards.
                let mut cursor = self.get_unsafe_cursor_before(target);

                unsafe {
                    let amt_modified = ContentTreeRaw::unsafe_mutate_entry_notify(|e| {
                        if tag == InsDelTag::Ins {
                            e.state.mark_not_inserted_yet();
                        } else {
                            e.state.undelete();
                        }
                    }, &mut cursor, len, notify_for(&mut self.index));

                    // dbg!(amt_modified);
                    target += amt_modified;
                    len -= amt_modified;
                }
            }
        }
    }
}