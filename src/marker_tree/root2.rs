use crate::marker_tree::entry::EntryTraits;
use crate::marker_tree::{MarkerTree, Cursor, NodeLeaf, FlushMarker, NUM_LEAF_ENTRIES};
use std::ptr::NonNull;
use std::ptr;
use std::pin::Pin;

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
                return; // WE're done here.
            }
            items = &items[items_idx..];

            // roll_next, but don't skip empty entries in the node.
            // if cursor.idx + 1 == NUM_LEAF_ENTRIES {
            //     flush_marker.flush(node);
            //     dbg!(cursor.traverse(true));
            //     assert_eq!(cursor.offset, 0);
            //     // cursor.offset = 0;
            //     // cursor.next_entry_marker(flush_marker);
            // } else {
            //     cursor.offset = 0;
            //     cursor.idx += 1;
            // }
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
        cursor.idx += items.len();

        // cursor.offset = node.data[idx].len();
        cursor.offset = 0; // Mmm might be better to roll back to the end of the prev element here.

        // The cursor isn't updated to point after remainder.
        if let Some(e) = remainder {
            flush_marker.0 += e.content_len() as isize;
            if remainder_moved {
                notify(e, cursor.node);
            }
            node.data[cursor.idx] = e;
        }
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