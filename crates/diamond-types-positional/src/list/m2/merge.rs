use std::cmp::Ordering;
use std::ptr::NonNull;
use smallvec::SmallVec;
use content_tree::{ContentTreeRaw, Cursor, DEFAULT_IE, DEFAULT_LE, MutCursor, NodeLeaf, null_notify, UnsafeCursor};
use rle::{HasLength, Searchable, SplitableSpan};
use crate::list::{ListCRDT, Time};
use crate::list::m2::{DocRangeIndex, M2Tracker, SpaceIndex};
use crate::list::m2::yjsspan2::{YjsSpan2, YjsSpanState};
use crate::list::operation::{InsDelTag, PositionalComponent};
use crate::localtime::TimeSpan;
use crate::rle::KVPair;
use crate::{AgentId, ROOT_TIME};
use crate::list::m2::markers::MarkerEntry;


pub(super) fn notify_for(index: &mut SpaceIndex) -> impl FnMut(YjsSpan2, NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>>) + '_ {
    // |_entry: YjsSpan2, _leaf| {}
    move |entry: YjsSpan2, leaf| {
        let mut start = entry.id.start;
        let mut len = entry.len();

        let index_len = index.len() as usize;
        if start > index_len {
            // Insert extra dummy data to cover deletes.
            len += start - index_len;
            start = index_len;
        }

        index.replace_range_at_offset(start, MarkerEntry {
            ptr: Some(leaf), len
        });

        // index.replace_range(entry.order as usize, MarkerEntry {
        //     ptr: Some(leaf), len: entry.len() as u32
        // });
    }
}


impl M2Tracker {
    pub(super) fn marker_at(&self, time: Time) -> NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>> {
        let cursor = self.index.cursor_at_offset_pos(time, false);
        // Gross.
        cursor.get_item().unwrap().unwrap()
    }


    pub(crate) fn get_unsafe_cursor_before(&self, time: Time) -> UnsafeCursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
        if time == ROOT_TIME {
            // Or maybe we should just abort?
            self.range_tree.unsafe_cursor_at_end()
        } else {
            let marker = self.marker_at(time);
            unsafe {
                ContentTreeRaw::cursor_before_item(time, marker)
            }
        }
    }

    pub(crate) fn get_cursor_before(&self, time: Time) -> Cursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
        unsafe { Cursor::unchecked_from_raw(&self.range_tree, self.get_unsafe_cursor_before(time)) }
    }

    pub(super) fn get_unsafe_cursor_after(&self, time: Time, stick_end: bool) -> UnsafeCursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE> {
        if time == ROOT_TIME {
            self.range_tree.unsafe_cursor_at_start()
        } else {
            let marker = self.marker_at(time);
            // let marker: NonNull<NodeLeaf<YjsSpan, ContentIndex>> = self.markers.at(order as usize).unwrap();
            // self.content_tree.
            let mut cursor = unsafe {
                ContentTreeRaw::cursor_before_item(time, marker)
            };
            // The cursor points to parent. This is safe because of guarantees provided by
            // cursor_before_item.
            cursor.offset += 1;
            if !stick_end { cursor.roll_to_next_entry(); }
            cursor
        }
    }

    // pub(super) fn integrate(&mut self, agent: AgentId, item: YjsSpan2, ins_content: Option<&str>, cursor_hint: Option<MutCursor<YjsSpan2, FullMetrics, DEFAULT_IE, DEFAULT_LE>>) {
    pub(super) fn integrate(&mut self, list: &ListCRDT, agent: AgentId, item: YjsSpan2, ins_content: Option<&str>, mut cursor: UnsafeCursor<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>) {
        assert!(item.len() > 0);

        // Ok now that's out of the way, lets integrate!
        cursor.roll_to_next_entry();
        // let mut cursor = cursor_hint.map_or_else(|| {
        //     self.get_unsafe_cursor_after(item.origin_left, false)
        // }, |mut c| {
        //     // Ideally this wouldn't be necessary.
        //     c.roll_to_next_entry();
        //     c.inner
        // });

        // These are almost never used. Could avoid the clone here... though its pretty cheap.
        let left_cursor = cursor.clone();
        let mut scan_start = cursor.clone();
        let mut scanning = false;

        loop {
            let other_order = match unsafe { cursor.unsafe_get_item() } {
                None => { break; } // End of the document
                Some(o) => { o }
            };

            // Almost always true. Could move this short circuit earlier?
            if other_order == item.origin_right { break; }

            // This code could be better optimized, but its already O(n * log n), and its extremely
            // rare that you actually get concurrent inserts at the same location in the document
            // anyway.

            let other_entry = *cursor.get_raw_entry();
            // let other_order = other_entry.order + cursor.offset as u32;

            let other_left_order = other_entry.origin_left_at_offset(cursor.offset);
            let other_left_cursor = self.get_unsafe_cursor_after(other_left_order, false);

            // YjsMod semantics
            match unsafe { other_left_cursor.unsafe_cmp(&left_cursor) } {
                Ordering::Less => { break; } // Top row
                Ordering::Greater => { } // Bottom row. Continue.
                Ordering::Equal => {
                    if item.origin_right == other_entry.origin_right {
                        // Origin_right matches. Items are concurrent. Order by agent names.
                        let my_name = list.get_agent_name(agent);
                        let other_loc = list.client_with_localtime.get(other_order);
                        let other_name = list.get_agent_name(other_loc.agent);
                        assert_ne!(my_name, other_name);

                        if my_name < other_name {
                            // Insert here.
                            break;
                        } else {
                            scanning = false;
                        }
                    } else {
                        // Set scanning based on how the origin_right entries are ordered.
                        let my_right_cursor = self.get_cursor_before(item.origin_right);
                        let other_right_cursor = self.get_cursor_before(other_entry.origin_right);

                        if other_right_cursor < my_right_cursor {
                            if !scanning {
                                scanning = true;
                                scan_start = cursor.clone();
                            }
                        } else {
                            scanning = false;
                        }
                    }
                }
            }

            // This looks wrong. The entry in the range tree is a run with:
            // - Incrementing orders (maybe from different peers)
            // - With incrementing origin_left.
            // Q: Is it possible that we get different behaviour if we don't separate out each
            // internal run within the entry and visit each one separately?
            //
            // The fuzzer says no, we don't need to do that. I assume its because internal entries
            // have higher origin_left, and thus they can't be peers with the newly inserted item
            // (which has a lower origin_left).
            if !cursor.next_entry() {
                // This is dirty. If the cursor can't move to the next entry, we still need to move
                // it to the end of the current element or we'll prepend. next_entry() doesn't do
                // that for some reason. TODO: Clean this up.
                cursor.offset = other_entry.len();
                break;
            }
        }
        if scanning { cursor = scan_start; }

        if cfg!(debug_assertions) {
            let pos = unsafe { cursor.count_content_pos() };
            let len = self.range_tree.content_len();
            assert!(pos <= len);
        }

        assert_eq!(ins_content, None, "Blerp inserting text not implemented");
        // if let Some(text) = self.list.text_content.as_mut() {
        //     let pos = unsafe { cursor.count_content_pos() };
        //     if let Some(ins_content) = ins_content {
        //         // debug_assert_eq!(count_chars(&ins_content), item.len as usize);
        //         text.insert(pos, ins_content);
        //     } else {
        //         // todo!("Figure out what to do when inserted content not present");
        //         // This is really dirty. This will happen when we're integrating remote txns which
        //         // are missing inserted content - usually because the remote peer hasn't kept
        //         // deleted text.
        //         //
        //         // In that case, we're inserting content which is about to be deleted by another
        //         // incoming operation.
        //         //
        //         // Ideally it would be nice to flag the range here and cancel it out with the
        //         // corresponding incoming delete. But thats really awkward, and this hack is super
        //         // simple.
        //         let content = SmartString::from("x").repeat(item.len as usize);
        //         text.insert(pos, content.as_str());
        //     }
        // }

        // Now insert here.
        unsafe { ContentTreeRaw::unsafe_insert_notify(&mut cursor, item, notify_for(&mut self.index)); }
        // cursor
    }

    fn apply_range(&mut self, list: &ListCRDT, range: TimeSpan) {
        if range.is_empty() { return; }

        // let mut cwl_idx = self.list.client_with_localtime.find_index(range.start).unwrap();

        // TODO: This is super dirty. I'm just doing it like this to get around a borrow checker issue.
        // let ops_iter = list.iter_ops(range).collect::<SmallVec<[KVPair<PositionalComponent>; 5]>>();

        // for mut pair in ops_iter.into_iter() {
        for mut pair in list.iter_ops(range) {
            loop {
                let span = list.get_crdt_span(TimeSpan { start: pair.0, end: pair.0 + pair.1.len });
                if span.len() < pair.1.len() {
                    let local_pair = pair.truncate_keeping_right(span.len());
                    self.apply(list, span.agent, &local_pair);
                } else {
                    self.apply(list, span.agent, &pair);
                    break;
                }
            }
        }
    }

    /// This is for advancing us directly based on the edit.
    fn apply(&mut self, list: &ListCRDT, agent: AgentId, pair: &KVPair<PositionalComponent>) {
        // The op must have been applied at the branch that the tracker is currently at.
        let KVPair(time, op) = pair;

        match op.tag {
            InsDelTag::Ins => {
                if op.rev { unimplemented!("Implement me!") }

                // To implement this we need to:
                // 1. Find the item directly before the requested position. This is our origin-left.
                // 2. Scan forward until the next item which isn't in the not yet inserted state.
                // this is our origin right.
                // 3. Use the integrate() method to actually insert - since we need to handle local
                // conflicts.

                let (origin_left, mut cursor) = if op.pos == 0 {
                    (ROOT_TIME, self.range_tree.mut_cursor_at_start())
                } else {
                    let mut cursor = self.range_tree.mut_cursor_at_content_pos((op.pos - 1) as usize, false);
                    let origin_left = cursor.get_item().unwrap();
                    assert!(cursor.next_item());
                    (origin_left, cursor)
                };

                // Origin_right should be the next item which isn't in the NotInsertedYet state.
                // If we reach the end of the document before that happens, use ROOT_TIME.
                let origin_right = if !cursor.roll_to_next_entry() {
                    ROOT_TIME
                } else {
                    loop {
                        let e = cursor.try_get_raw_entry();
                        if let Some(e) = e {
                            if e.state == YjsSpanState::NotInsertedYet {
                                if !cursor.next_entry() { break ROOT_TIME; }
                                // Otherwise keep looping.
                            } else {
                                // We can use this.
                                break e.at_offset(cursor.offset);
                            }
                        } else { break ROOT_TIME; }
                    }
                };

                // let origin_right = cursor.get_item().unwrap_or(ROOT_TIME);

                let item = YjsSpan2 {
                    id: TimeSpan::new(*time, *time + op.len),
                    origin_left,
                    origin_right,
                    state: YjsSpanState::Inserted,
                };

                // This is dirty because the cursor's lifetime is not associated with self.
                let cursor = cursor.inner;
                self.integrate(list, agent, item, None, cursor);
            }

            InsDelTag::Del => {
                let deleted_items = self.range_tree.local_deactivate_at_content_notify(op.pos, op.len, notify_for(&mut self.index));

                let mut next_time = *time;
                for item in deleted_items {
                    self.deletes.push(KVPair(next_time, item.id));
                    next_time += item.len();
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::list::ListCRDT;
    use super::*;

    #[test]
    fn foo() {
        let mut list = ListCRDT::new();
        list.get_or_create_agent_id("seph");
        list.local_insert(0, 0, "hi there");
        list.local_delete(0, 2, 3);

        let mut t = M2Tracker::new();

        let end = list.get_next_time();
        dbg!(end);
        t.apply_range(&list, (0..end).into());
        dbg!(&t);

        // t.retreat_by_range((0..end).into());
        t.retreat_by_range((8..end).into());
        t.retreat_by_range((7..8).into());
        dbg!(&t);
    }
}