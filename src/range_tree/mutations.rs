use crate::range_tree::entry::{EntryTraits, CRDTItem};
use crate::range_tree::{RangeTree, Cursor, NodeLeaf, FlushMarker, NUM_LEAF_ENTRIES, DeleteResult, ParentPtr, Node, NodePtr, NUM_NODE_CHILDREN, NodeInternal};
use std::ptr::NonNull;
use std::{ptr, mem};
use std::pin::Pin;
use smallvec::SmallVec;
use crate::range_tree::root::{extend_delete};

impl<E: EntryTraits> RangeTree<E> {
    /// Insert item(s) at the position pointed to by the cursor. If the item is split, the remainder
    /// is returned. The cursor is modified in-place to point after the inserted items.
    ///
    /// If the cursor points in the middle of an item, the item is split.
    ///
    /// TODO: Add support for item prepending to this method, for backspace operations.
    pub(super) fn splice_insert<F>(self: &Pin<Box<Self>>, mut items: &[E], cursor: &mut Cursor<E>, flush_marker: &mut FlushMarker, notify: &mut F)
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
            // We're at the end of an element. Try and append as much as we can here.
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

            if remainder.is_none() && cursor.idx < NUM_LEAF_ENTRIES {
                // We'll also try to *prepend* some content on the front of the subsequent element
                // I'm sure there's a way to do this using iterators, but I'm not sure it would be
                // cleaner.

                // This optimization improves performance when the user hits backspace. We end up
                // merging all the deleted elements together. This adds complexity in exchange for
                // making the tree simpler. For real edit sequences (like the automerge-perf data
                // set) this gives about an 8% performance increase.
                let mut end_idx = items.len() - 1;
                let cur_entry = &mut node.data[cursor.idx];
                loop {
                    let next = items[end_idx];
                    if next.can_append(cur_entry) {
                        flush_marker.0 += next.content_len() as isize;
                        notify(next, cursor.node);
                        cur_entry.prepend(next);
                    } else { break; }

                    if end_idx == 0 {
                        return; // We've prepended everything.
                    } else { end_idx -= 1; }
                }
                items = &items[..=end_idx];
            }
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
    pub(super) fn replace_entry<F>(self: &Pin<Box<Self>>, cursor: &mut Cursor<E>, items: &[E], flush_marker: &mut FlushMarker, notify: &mut F)
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
}

impl<E: EntryTraits + CRDTItem> RangeTree<E> {
    pub fn local_delete<F>(self: &Pin<Box<Self>>, mut cursor: Cursor<E>, deleted_len: usize, mut notify: F) -> DeleteResult<E>
        where F: FnMut(E, NonNull<NodeLeaf<E>>) {
        // println!("local_delete len: {} at cursor {:?}", deleted_len, cursor);

        // TODO: Benchmark this.
        if cfg!(debug_assertions) {
            let cursor_pos = cursor.count_pos();
            assert!(cursor_pos + deleted_len <= self.count);
        }
        // dbg!(cursor_pos, self.count);

        let expected_size = self.count - deleted_len;
        let mut result: DeleteResult<E> = SmallVec::default();
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

            extend_delete(&mut result, entry);
            entry.mark_deleted();

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

        if cfg!(debug_assertions) {
            // self.print_ptr_tree();
            // self.as_ref().get_ref().check();

            // Check the total size of the tree has grown by len.
            assert_eq!(expected_size, self.count);
        }

        result
    }

    /// Delete up to max_deleted_len from the marker tree, at the location specified by cursor.
    /// We will always delete at least one item. Consumers of this API should call this in a loop.
    ///
    /// Returns the number of items marked for deletion.
    pub fn remote_delete<F>(self: &Pin<Box<Self>>, mut cursor: Cursor<E>, max_deleted_len: usize, notify: F) -> usize
        where F: FnMut(E, NonNull<NodeLeaf<E>>) {

        cursor.roll_to_next(false);
        let entry = cursor.get_entry();
        let len = entry.len();
        let amt_deleted = usize::min(len - cursor.offset, max_deleted_len);

        // If the entry is already marked as deleted, we do nothing. This is needed because
        // local_delete will skip deletes and go delete something else.
        if entry.is_insert() {
            // The deleted region could be in the middle of an item and that has all sorts of
            // complexity. Just delegate to local_delete above, which will take care of all that
            // jazz.
            //
            // Even though we're just editing an item here, the item could be split by the delete,
            // so notify may end up called.
            self.local_delete(cursor, amt_deleted, notify);
        }

        amt_deleted
    }
}

impl<E: EntryTraits> NodeLeaf<E> {

    /// Split this leaf node at the specified index, so 0..idx stays and idx.. moves to a new node.
    ///
    /// The new node has additional `padding` empty items at the start of its list.
    fn split_at<F>(&mut self, idx: usize, padding: usize, notify: &mut F) -> NonNull<NodeLeaf<E>>
        where F: FnMut(E, NonNull<NodeLeaf<E>>)
    {
        // println!("split_at {} {}", idx, padding);
        unsafe {
            // TODO(optimization): We're currently copying / moving everything *after* idx. If idx
            // is small, we could instead move everything before idx - which would save a bunch of
            // calls to notify and save us needing to fix up a bunch of parent pointers. More work
            // here, but probably less work overall.

            let mut new_node = Self::new(); // The new node has a danging parent pointer
            let new_filled_len = self.len_entries() - idx;
            let new_len = new_filled_len + padding;
            debug_assert!(new_len <= NUM_LEAF_ENTRIES);

            if new_filled_len > 0 {
                ptr::copy_nonoverlapping(&self.data[idx], &mut new_node.data[padding], new_filled_len);
            }

            new_node.num_entries = new_len as u8; // Not including padding!

            // zero out the old entries
            let mut stolen_length: usize = 0;
            for e in &mut self.data[idx..self.num_entries as usize] {
                stolen_length += e.content_len();
                *e = E::default();
            }
            self.num_entries = idx as u8;

            // eprintln!("split_at idx {} self_entries {} stolel_len {} self {:?}", idx, self_entries, stolen_length, &self);

            let mut inserted_node = Node::Leaf(Box::pin(new_node));
            // This is the pointer to the new item we'll end up returning.
            let new_leaf_ptr = NonNull::new_unchecked(inserted_node.unwrap_leaf_mut().get_unchecked_mut());
            for e in &inserted_node.unwrap_leaf().data[padding..new_len] {
                notify(*e, new_leaf_ptr);
            }

            insert_after(self.parent, inserted_node, NodePtr::Leaf(NonNull::new_unchecked(self)), stolen_length as _);

            // TODO: It would be cleaner to return a Pin<&mut NodeLeaf> here instead of the pointer.
            new_leaf_ptr
        }
    }
}

// I'm really not sure where to put this method. Its not really associated with
// any of the tree implementation methods. This seems like a hidden spot. Maybe
// range_tree? I could put it in impl ParentPtr? I dunno...
fn insert_after<E: EntryTraits>(
    mut parent: ParentPtr<E>,
    mut inserted_node: Node<E>,
    mut insert_after: NodePtr<E>,
    mut stolen_length: u32) {
    unsafe {
        // Ok now we need to walk up the tree trying to insert. At each step
        // we will try and insert inserted_node into parent next to old_node
        // (topping out at the head).
        loop {
            // First try and simply emplace in the new element in the parent.
            if let ParentPtr::Internal(mut n) = parent {
                let parent_ref = n.as_ref();
                let count = parent_ref.count_children();
                if count < NUM_NODE_CHILDREN {
                    // Great. Insert the new node into the parent and return.
                    inserted_node.set_parent(ParentPtr::Internal(n));

                    let old_idx = parent_ref.find_child(insert_after).unwrap();
                    let new_idx = old_idx + 1;

                    let parent_ref = n.as_mut();
                    parent_ref.data[old_idx].0 -= stolen_length;
                    parent_ref.splice_in(new_idx, stolen_length, inserted_node);

                    // eprintln!("1");
                    return;
                }
            }

            // Ok so if we've gotten here we need to make a new internal
            // node filled with inserted_node, then move and all the goodies
            // from ParentPtr.
            match parent {
                ParentPtr::Root(mut r) => {
                    // This is the simpler case. The new root will be a new
                    // internal node containing old_node and inserted_node.
                    let new_root = Node::Internal(NodeInternal::new_with_parent(ParentPtr::Root(r)));
                    let mut old_root = mem::replace(&mut r.as_mut().root, new_root);

                    // *inserted_node.get_parent_mut() = parent_ptr;

                    let root = r.as_mut();
                    let count = root.count as u32;
                    let mut new_root_ref = root.root.unwrap_internal_mut();
                    // let parent_ptr = ParentPtr::Internal(NonNull::new_unchecked(new_root_ref));
                    let parent_ptr = new_root_ref.as_ref().to_parent_ptr();

                    // Reassign parents for each node
                    old_root.set_parent(parent_ptr);
                    inserted_node.set_parent(parent_ptr);

                    new_root_ref.as_mut().project_data_mut()[0] = (count - stolen_length, Some(old_root));
                    new_root_ref.as_mut().project_data_mut()[1] = (stolen_length, Some(inserted_node));

                    // r.as_mut().print_ptr_tree();
                    return;
                },

                ParentPtr::Internal(mut n) => {
                    // And this is the complex case. We have MAX_CHILDREN+1
                    // items (in some order) to distribute between two
                    // internal nodes (one old, one new). Then we iterate up
                    // the tree.
                    let left_sibling = n.as_ref();
                    parent = left_sibling.parent; // For next iteration through the loop.
                    debug_assert!(left_sibling.count_children() == NUM_NODE_CHILDREN);

                    // let mut right_sibling = NodeInternal::new_with_parent(parent);
                    let mut right_sibling_box = Node::Internal(NodeInternal::new_with_parent(parent));
                    let mut right_sibling = right_sibling_box.unwrap_internal_mut();
                    let old_idx = left_sibling.find_child(insert_after).unwrap();

                    let left_sibling = n.as_mut();
                    left_sibling.data[old_idx].0 -= stolen_length;
                    let mut new_stolen_length = 0;
                    // Dividing this into cases makes it easier to reason
                    // about.
                    if old_idx < NUM_NODE_CHILDREN /2 {
                        // Move all items from MAX_CHILDREN/2..MAX_CHILDREN
                        // into right_sibling, then splice inserted_node into
                        // old_parent.
                        for i in 0..NUM_NODE_CHILDREN /2 {
                            let (c, e) = mem::replace(&mut left_sibling.data[i + NUM_NODE_CHILDREN /2], (0, None));
                            if let Some(mut e) = e {
                                e.set_parent(right_sibling.as_ref().to_parent_ptr());
                                new_stolen_length += c;
                                right_sibling.as_mut().project_data_mut()[i] = (c, Some(e));
                            }

                        }

                        let new_idx = old_idx + 1;
                        inserted_node.set_parent(ParentPtr::Internal(NonNull::new_unchecked(left_sibling)));
                        left_sibling.splice_in(new_idx, stolen_length, inserted_node);
                    } else {
                        // The new element is in the second half of the
                        // group.
                        let new_idx = old_idx - NUM_NODE_CHILDREN /2 + 1;

                        inserted_node.set_parent(right_sibling.as_ref().to_parent_ptr());
                        let mut new_entry = (stolen_length, Some(inserted_node));
                        new_stolen_length = stolen_length;

                        let mut src = NUM_NODE_CHILDREN /2;
                        for dest in 0..=NUM_NODE_CHILDREN /2 {
                            if dest == new_idx {
                                right_sibling.as_mut().project_data_mut()[dest] = mem::take(&mut new_entry);
                            } else {
                                let (c, e) = mem::replace(&mut left_sibling.data[src], (0, None));

                                if let Some(mut e) = e {
                                    e.set_parent(right_sibling.as_ref().to_parent_ptr());
                                    new_stolen_length += c;
                                    right_sibling.as_mut().project_data_mut()[dest] = (c, Some(e));
                                    src += 1;
                                } else { break; }
                            }
                        }
                        debug_assert!(new_entry.1.is_none());
                    }

                    insert_after = NodePtr::Internal(n);
                    inserted_node = right_sibling_box;
                    stolen_length = new_stolen_length;
                    // And iterate up the tree.
                },
            };
        }
    }
}


#[cfg(test)]
mod tests {
    // use std::pin::Pin;
    use crate::range_tree::{RangeTree, Entry, FlushMarker};
    use crate::common::CRDTLocation;

    #[test]
    fn splice_insert_test() {
        let tree = RangeTree::new();
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

    #[test]
    fn backspace_collapses() {
        let tree = RangeTree::new();

        let cursor = tree.cursor_at_pos(0, false);
        let entry = Entry {
            loc: CRDTLocation {agent: 0, seq: 1000},
            len: 100
        };
        tree.insert(cursor, entry, &mut |_, _| {});
        assert_eq!(tree.count_entries(), 1);

        // Ok now I'm going to delete the last and second-last elements. We should end up with
        // two entries.
        let cursor = tree.cursor_at_pos(99, false);
        tree.local_delete(cursor, 1, &mut |_, _| {});
        assert_eq!(tree.count_entries(), 2);

        let cursor = tree.cursor_at_pos(98, false);
        tree.local_delete(cursor, 1, &mut |_, _| {});
        assert_eq!(tree.count_entries(), 2);
    }
}