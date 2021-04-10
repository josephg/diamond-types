use crate::marker_tree::entry::EntryTraits;
use crate::marker_tree::{MarkerTree, Cursor, NodeLeaf, FlushMarker, NUM_LEAF_ENTRIES, DeleteResult};
use std::ptr::NonNull;
use std::ptr;
use std::pin::Pin;
use smallvec::SmallVec;
use crate::marker_tree::root::{extend_delete, DeleteOp};
use std::mem::swap;

impl<E: EntryTraits> MarkerTree<E> {

    /// Insert item at the position pointed to by the cursor. If the item is split, the remainder is
    /// returned. The cursor is modified in-place.
    pub fn splice_insert<F>(self: &Pin<Box<Self>>, mut items: &[E], cursor: &mut Cursor<E>, flush_marker: &mut FlushMarker, notify: &mut F)
        where F: FnMut(E, NonNull<NodeLeaf<E>>)
    {
        // dbg!(items, &cursor);
        if items.len() == 0 { return; }

        // let mut items_content_len = items.iter().fold(0, |a, b| {
        //     a + b.content_len()
        // });

        // cursor.node.as_ref() would be better but it would hold a borrow to cursor :/
        let mut node = unsafe { &mut *cursor.node.as_ptr() };

        // let new_item_length = item.len();
        // let mut items_iter = items.iter().peekable();

        if cursor.offset == 0 && cursor.idx > 0 { // TODO: Benchmark to see if this actually helps any.
            // We'll roll the cursor back to opportunistically see if we can append.
            cursor.idx -= 1;
            cursor.offset = node.data[cursor.idx].len(); // blerp could be cleaner.
        }

        let seq_len = node.data[cursor.idx].len();
        // Remainder is the trimmed off returned value.
        let remainder = if cursor.offset == seq_len || cursor.offset == 0 {
            None
        } else {
            // splice the item into the current cursor location.
            let entry: &mut E = &mut node.data[cursor.idx];
            let remainder = entry.truncate(cursor.offset);
            flush_marker.0 -= (seq_len - cursor.offset) as isize;
            // We don't need to update cursor since its already where it needs to be.

            Some(remainder)
        };

        // dbg!(&remainder);

        if cursor.offset != 0 {
            debug_assert_eq!(cursor.offset, node.data[cursor.idx].len());
            // Try and append as much as we can after the current entry
            let mut items_idx = 0;
            let cur_entry: &mut E = &mut node.data[cursor.idx];
            while items_idx < items.len() { // There's probably a cleaner way to write this loop.
                let next = items[items_idx];
                if cur_entry.can_append(&next) {
                    flush_marker.0 += next.content_len() as isize;
                    notify(next, cursor.node);
                    cur_entry.append(next);

                    cursor.offset = cur_entry.len();
                    items_idx += 1;
                } else { break; }
            }
            if items_idx == items.len() && remainder.is_none() {
                return; // WE're done here. Cursor is at the end of the previous entry.
            }
            items = &items[items_idx..];
            debug_assert!(items.len() >= 1);

            cursor.offset = 0;
            cursor.idx += 1; // NOTE: Cursor might point past the end of the node.
        }
        // debug_assert_eq!(cursor.offset, 0);

        // Step 2: Make room in the leaf for the new items.
        // I'm setting up node again to work around a borrow checker issue.
        // let mut node = unsafe { cursor.node.as_mut() };
        let space_needed = items.len() + remainder.is_some() as usize;
        let num_filled = node.len_entries();
        debug_assert!(space_needed > 0);
        // Only 2 in debug mode! Could remove this restriction but it doesn't matter yet.
        // (Hint to later self: Call insert_after() in a loop.)
        assert!(space_needed <= NUM_LEAF_ENTRIES / 2);

        let remainder_moved = if num_filled + space_needed > NUM_LEAF_ENTRIES {
            // println!("spill {} {}", num_filled, space_needed);
            // We need to split the node. The proper b-tree way to do this is to make sure there's
            // always N/2 items in every leaf after a split, but I don't think it'll matter here.
            // Instead I'll split at idx, and insert the new items in whichever child has more space
            // afterwards.

            // We have to flush regardless, because we might have truncated the current element.
            flush_marker.flush(node);

            if cursor.idx < NUM_LEAF_ENTRIES / 2 {
                // Split then elements go in left branch, so the cursor isn't updated.
                node.split_at(cursor.idx, 0, notify);
                node.num_entries += space_needed as u8;
                false
            } else {
                // This will adjust num_entries based on the padding parameter.
                let new_node_ptr = node.split_at(cursor.idx, space_needed, notify);
                cursor.node = new_node_ptr;
                cursor.idx = 0;
                node = unsafe { &mut *cursor.node.as_ptr() };
                true
            }
        } else {
            // We need to move the existing items. This doesn't effect sizes.
            if num_filled > cursor.idx {
                node.data[..].copy_within(cursor.idx..num_filled, cursor.idx + space_needed);
            }
            node.num_entries += space_needed as u8;
            false
        };

        // Step 3: There's space now, so we can just insert.
        // println!("items {:?} cursor {:?}", items, cursor);
        // node.num_entries += space_needed as u8;
        for e in items {
            flush_marker.0 += e.content_len() as isize;
            notify(*e, cursor.node);
        }
        node.data[cursor.idx..cursor.idx + items.len()].copy_from_slice(items);

        // Point the cursor to the end of the last inserted item.
        cursor.idx += items.len() - 1;
        cursor.offset = items[items.len() - 1].len();

        // The cursor isn't updated to point after remainder.
        if let Some(e) = remainder {
            flush_marker.0 += e.content_len() as isize;
            if remainder_moved {
                notify(e, cursor.node);
            }
            node.data[cursor.idx + 1] = e;
        }
    }

    /// Replace the item at the cursor position with the new items provided by items.
    ///
    /// Items must have a maximum length of 3, due to limitations in split_insert above.
    /// The cursor's offset is ignored. The cursor ends up at the end of the inserted items.
    pub fn replace_entry<F>(self: &Pin<Box<Self>>, cursor: &mut Cursor<E>, mut items: &[E], flush_marker: &mut FlushMarker, notify: &mut F)
        where F: FnMut(E, NonNull<NodeLeaf<E>>) {
        assert!(items.len() >= 1 && items.len() <= 3);

        let entry = cursor.get_entry_mut();
        // println!("replace_entry {:?} {:?} with {:?}", flush_marker.0, &entry, items);
        flush_marker.0 -= entry.content_len() as isize;
        *entry = items[0];
        flush_marker.0 += entry.content_len() as isize;
        cursor.offset = entry.len();

        // And insert the rest.
        self.splice_insert(&items[1..], cursor, flush_marker, notify);
    }

    pub fn insert<F>(self: &Pin<Box<Self>>, mut cursor: Cursor<E>, new_entry: E, mut notify: F)
        where F: FnMut(E, NonNull<NodeLeaf<E>>) {

        let len = new_entry.content_len();
        let expected_size = self.count + len;

        let mut marker = FlushMarker(0);
        self.splice_insert(&[new_entry], &mut cursor, &mut marker, &mut notify);
        marker.flush(unsafe { cursor.get_node_mut() });
        // println!("tree after insert {:#?}", self);

        // self.cache_cursor(pos + new_entry.content_len(), cursor);

        if cfg!(debug_assertions) {
            // self.print_ptr_tree();

            // self.as_ref().get_ref().check();

            // Check the total size of the tree has grown by len.
            assert_eq!(expected_size, self.count);
        }
    }

    pub fn local_delete<F>(self: &Pin<Box<Self>>, mut cursor: Cursor<E>, deleted_len: usize, mut notify: F) -> DeleteResult
        where F: FnMut(E, NonNull<NodeLeaf<E>>) {
        // println!("local_delete len: {} at cursor {:?}", deleted_len, cursor);

        let cursor_pos = cursor.count_pos();
        assert!(cursor_pos + deleted_len <= self.count);
        // dbg!(cursor_pos, self.count);

        let expected_size = self.count - deleted_len;
        let mut result: DeleteResult = SmallVec::default();
        let mut flush_marker = FlushMarker(0);
        let mut delete_remaining = deleted_len;
        cursor.roll_to_next(false);

        while delete_remaining > 0 {
            // Mark as much as we can for delete in the current node.
            // dbg!(cursor, delete_remaining);
            // dbg!(cursor.get_node());
            debug_assert!(!cursor.get_entry().is_invalid());
            // dbg!(cursor.get_entry());

            while cursor.get_entry().is_delete() {
                Self::next_entry_or_panic(&mut cursor, &mut flush_marker);
            }

            let node = unsafe { cursor.get_node_mut() };
            let mut entry: E = node.data[cursor.idx];
            let mut entry_len = entry.content_len();
            debug_assert!(entry.is_insert()); // We should have skipped already deleted nodes.

            // dbg!(cursor, entry);
            assert!(cursor.offset < entry_len);

            // Delete as many characters as we can in the document each time through this loop.
            // There's 1-3 parts here - part1<part2>part3

            // Trim off the first part
            let a = if cursor.offset > 0 {
                entry_len -= cursor.offset;
                Some(entry.truncate_keeping_right(cursor.offset))
            } else { None };

            // Trim off the last part
            let (c, deleted_here) = if delete_remaining < entry_len {
                (Some(entry.truncate(delete_remaining)), delete_remaining)
            } else { (None, entry_len) };

            entry.mark_deleted();
            extend_delete(&mut result, DeleteOp {
                loc: entry.at_offset(0),
                len: deleted_here as _
            });

            if let Some(a) = a {
                if let Some(c) = c {
                    self.replace_entry(&mut cursor, &[a, entry, c], &mut flush_marker, &mut notify);
                } else {
                    self.replace_entry(&mut cursor, &[a, entry], &mut flush_marker, &mut notify);
                }
            } else {
                if let Some(c) = c {
                    self.replace_entry(&mut cursor, &[entry, c], &mut flush_marker, &mut notify);
                } else {
                    // self.replace_entry(&mut cursor, &[entry], &mut flush_marker, &mut notify);
                    node.data[cursor.idx] = entry;
                    cursor.offset = deleted_here;
                    flush_marker.0 -= deleted_here as isize;
                }
            }
            delete_remaining -= deleted_here;
        }

        // The cursor is potentially after any remainder.
        flush_marker.flush(unsafe { cursor.get_node_mut() });

        result
    }

    pub fn local_delete2<F>(self: &Pin<Box<Self>>, mut cursor: Cursor<E>, deleted_len: usize, mut notify: F) -> DeleteResult
        where F: FnMut(E, NonNull<NodeLeaf<E>>) {
        let expected_size = self.count - deleted_len;
        let mut result: DeleteResult = SmallVec::default();
        let mut flush_marker = FlushMarker(0);
        cursor.roll_to_next(false);
        let mut delete_remaining = deleted_len;

        while delete_remaining > 0 {
            // Mark as much as we can for delete in the current node.
            while cursor.get_entry().is_delete() {
                Self::next_entry_or_panic(&mut cursor, &mut flush_marker);
            }

            let node = unsafe { cursor.get_node_mut() };
            let mut entry: &mut E = &mut node.data[cursor.idx];
            let entry_len = entry.content_len();
            debug_assert!(entry.is_insert()); // We should have skipped already deleted nodes.


            // Delete as many characters as we can in the document each time through this loop.
            // There's 4 semi-overlapping cases here. <xxx> marks deleted characters
            // 1. <xxx>
            // 2. <xxx>text
            // 3. text<xxx>
            // 4. te<xxx>xt
            //
            // In cases 2, 3 and 4 we will need to split the current node.

            if cursor.offset == 0 {
                // dbg!(&entry, delete_remaining);
                // Cases 1 and 2. We'll mark the entry for delete

                // First trim off any remaining inserts that should be unaffected.

                // We'll pull the remainder off, and mark the rest deleted. It all ends up in
                // flush_marker anyway.
                flush_marker.0 -= entry_len as isize;

                let (remainder, deleted_here) = if delete_remaining < entry_len {
                    // Case 2 - <xxx>text
                    let remainder = entry.truncate(delete_remaining);
                    (Some(remainder), delete_remaining)
                } else { (None, entry_len) };

                extend_delete(&mut result, DeleteOp {
                    loc: entry.at_offset(0),
                    len: deleted_here as _
                });
                entry.mark_deleted();
                cursor.offset = deleted_here; // Move to the end of entry.
                // And re-insert remainder.
                if let Some(remainder) = remainder {
                    // dbg!(&remainder);
                    // This will update flush_marker for us, and move the cursor after remainder.
                    self.splice_insert(&[remainder], &mut cursor, &mut flush_marker, &mut notify);
                }

                delete_remaining -= deleted_here;
            } else {
                // Cases 3 and 4. We need to first split the content we want to leave in place.

                // There's 2 or 3 parts here - part1<part2>part3. middle is parts 2 and 3.
                debug_assert!(cursor.offset < entry_len);
                let middle_len = entry_len - cursor.offset; // <xxx>aaa or <xxx>
                flush_marker.0 -= middle_len as isize;
                let mut middle = entry.truncate(cursor.offset);
                debug_assert_eq!(middle.len(), middle_len);

                // Peel off part 3.
                let (remainder, deleted_here) = if delete_remaining < middle_len {
                    // Case 4 - te<xxx>st
                    let remainder = middle.truncate(delete_remaining);
                    (Some(remainder), delete_remaining)
                } else {
                    (None, middle_len)
                };
                // Now middle is just part 2, and remainder is part 3.
                extend_delete(&mut result, DeleteOp {
                    loc: middle.at_offset(0),
                    len: deleted_here as _
                });

                // Mark middle for deletion and insert back.
                middle.mark_deleted();
                if let Some(r) = remainder {
                    self.splice_insert(&[middle, r], &mut cursor, &mut flush_marker, &mut notify);
                } else {
                    self.splice_insert(&[middle], &mut cursor, &mut flush_marker, &mut notify);
                }

                delete_remaining -= deleted_here;
            }
        }

        // The cursor is potentially after any remainder.
        flush_marker.flush(unsafe { cursor.get_node_mut() });

        result
    }

}


#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use crate::marker_tree::{MarkerTree, Entry, FlushMarker};
    use crate::common::CRDTLocation;

    #[test]
    fn splice_insert_test() {
        let mut tree: Pin<Box<MarkerTree<Entry>>> = MarkerTree::new();
        let entry = Entry {
            loc: CRDTLocation {agent: 0, seq: 1000},
            len: 100
        };
        let mut cursor = tree.cursor_at_pos(0, false);
        let mut marker = FlushMarker(0);
        tree.splice_insert(&[entry], &mut cursor, &mut marker, &mut |_e, _x| {});
        marker.flush(unsafe {cursor.get_node_mut() });

        let entry = Entry {
            loc: CRDTLocation {agent: 0, seq: 1100},
            len: 20
        };
        cursor = tree.cursor_at_pos(15, false);
        tree.splice_insert(&[entry], &mut cursor, &mut marker, &mut |_e, _x| {});
        marker.flush(unsafe {cursor.get_node_mut() });

        println!("{:#?}", tree);

        tree.check();
    }
}