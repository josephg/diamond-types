use std::{mem, ptr};
use std::hint::unreachable_unchecked;
use std::pin::Pin;
use std::ptr::NonNull;

use smallvec::SmallVec;

use super::*;
use rle::AppendRle;

/// This file contains the core code for content-tree's mutation operations.

impl<E: ContentTraits, I: TreeMetrics<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    /// Insert item(s) at the position pointed to by the cursor. If the item is split, the remainder
    /// is returned. The cursor is modified in-place to point after the inserted items.
    ///
    /// If the cursor points in the middle of an item, the item is split.
    ///
    /// The list of items must have a maximum length of 3, so we can always insert all the new items
    /// in half of a leaf node. (This is a somewhat artificial constraint, but its fine here.)
    unsafe fn insert_internal<F>(mut items: &[E], cursor: &mut UnsafeCursor<E, I, IE, LE>, flush_marker: &mut I::Update, mut notify_here: bool, notify: &mut F)
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        if items.is_empty() { return; }
        assert!(items.len() <= 3);

        // cursor.get_node_mut() would be better but it would borrow the cursor.
        let mut node = &mut *cursor.node.as_ptr();

        let remainder = if cursor.offset == usize::MAX {
            debug_assert_eq!(cursor.idx, 0);
            debug_assert_eq!(node.num_entries, 0);
            // We're inserting into the start of a tree. I could short circuit here, but the
            // complexity isn't worth the performance boost given it just happens once per tree.
            cursor.offset = 0;
            None
        } else if cursor.offset == 0 && cursor.idx > 0 {
            // We'll roll the cursor back to opportunistically see if we can append.
            cursor.idx -= 1;
            cursor.offset = node.data[cursor.idx].len(); // blerp could be cleaner.
            None
        } else {
            // We could also roll back if cursor.offset == 0 and cursor.idx == 0 but when I tried it it
            // didn't make any difference in practice because insert() is always called with stick_end.

            // Remainder is the trimmed off returned value.
            if cursor.offset == node.data[cursor.idx].len() || cursor.offset == 0 {
                None
            } else {
                // splice the item into the current cursor location.
                let entry: &mut E = &mut node.data[cursor.idx];
                let remainder = entry.truncate(cursor.offset);
                I::decrement_marker(flush_marker, &remainder);
                // flush_marker -= (seq_len - cursor.offset) as isize;
                // We don't need to update cursor since its already where it needs to be.

                Some(remainder)
            }
        };

        // If we prepend to the start of the following tree node, the cursor will need to be
        // adjusted accordingly.
        let mut trailing_offset = 0;

        if cursor.offset != 0 {
            // We're at the end of an element. Try and append as much as we can here.
            debug_assert_eq!(cursor.offset, node.data[cursor.idx].len());
            // Try and append as much as we can after the current entry
            let mut items_idx = 0;
            let cur_entry: &mut E = &mut node.data[cursor.idx];
            while items_idx < items.len() { // There's probably a cleaner way to write this loop.
                let next = items[items_idx];
                if cur_entry.can_append(&next) {
                    I::increment_marker(flush_marker, &next);
                    // flush_marker += next.content_len() as isize;
                    if notify_here { notify(next, cursor.node) };
                    cur_entry.append(next);

                    cursor.offset = cur_entry.len();
                    items_idx += 1;
                } else { break; }
            }
            if items_idx == items.len() && remainder.is_none() {
                return; // We're done here. Cursor is at the end of the previous entry.
            }
            items = &items[items_idx..];
            // Note items might be empty now. We might just have remainder left.

            cursor.offset = 0;
            cursor.idx += 1; // NOTE: Cursor might point past the end of the node.

            if remainder.is_none() && !items.is_empty() && cursor.idx < node.len_entries() {
                // We'll also try to *prepend* some content on the front of the subsequent element
                // I'm sure there's a way to do this using iterators, but I'm not sure it would be
                // cleaner.

                // This optimization improves performance when the user hits backspace. We end up
                // merging all the deleted elements together. This adds complexity in exchange for
                // making the tree simpler. For real edit sequences (like the automerge-perf data
                // set) this gives about an 8% performance increase.

                // It may be worth being more aggressive here. We're currently not trying this trick
                // when the cursor is at the end of the current node. That might be worth trying!
                let mut end_idx = items.len() - 1;
                let cur_entry = &mut node.data[cursor.idx];
                loop {
                    let next = items[end_idx];
                    if next.can_append(cur_entry) {
                        I::increment_marker(flush_marker, &next);
                        if notify_here { notify(next, cursor.node) };
                        trailing_offset += next.len();
                        cur_entry.prepend(next);
                    } else { break; }

                    if end_idx == 0 {
                        cursor.offset = trailing_offset;
                        return; // We've prepended everything.
                    } else { end_idx -= 1; }
                }
                items = &items[..=end_idx];
            }
        }

        debug_assert_eq!(cursor.offset, 0);

        // Step 2: Make room in the leaf for the new items.
        // I'm setting up node again to work around a borrow checker issue.
        // let mut node = unsafe { cursor.node.as_mut() };
        let space_needed = items.len() + remainder.is_some() as usize;
        let num_filled = node.len_entries();
        debug_assert!(space_needed > 0);
        assert!(space_needed <= LE / 2);

        let remainder_moved = if num_filled + space_needed > LE {
            // We need to split the node. The proper b-tree way to do this is to make sure there's
            // always N/2 items in every leaf after a split, but I don't think it'll matter here.
            // Instead I'll split at idx, and insert the new items in whichever child has more space
            // afterwards.

            // We have to flush regardless, because we might have truncated the current element.
            node.flush_metric_update(flush_marker);

            if cursor.idx < LE / 2 {
                // Split then elements go in left branch, so the cursor isn't updated.
                node.split_at(cursor.idx, 0, notify);
                node.num_entries += space_needed as u8;
                false
            } else {
                // This will adjust num_entries based on the padding parameter.
                let new_node_ptr = node.split_at(cursor.idx, space_needed, notify);
                cursor.node = new_node_ptr;
                cursor.idx = 0;
                node = &mut *cursor.node.as_ptr();
                notify_here = true;
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

        let remainder_idx = cursor.idx + items.len();

        if !items.is_empty() {
            for e in items {
                I::increment_marker(flush_marker, e);
                // flush_marker.0 += e.content_len() as isize;
                if notify_here { notify(*e, cursor.node) };
            }
            node.data[cursor.idx..cursor.idx + items.len()].copy_from_slice(items);

            // Point the cursor to the end of the last inserted item.
            cursor.idx += items.len() - 1;
            cursor.offset = items[items.len() - 1].len();

            if trailing_offset > 0 {
                cursor.move_forward_by_offset(trailing_offset, Some(flush_marker));
            }
        }

        // The cursor isn't updated to point after remainder.
        if let Some(e) = remainder {
            I::increment_marker(flush_marker, &e);
            if remainder_moved {
                notify(e, cursor.node);
            }
            node.data[remainder_idx] = e;
        }
    }

    pub unsafe fn unsafe_insert_notify<F>(cursor: &mut UnsafeCursor<E, I, IE, LE>, new_entry: E, mut notify: F)
    where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {
        let mut marker = I::Update::default();
        Self::insert_internal(&[new_entry], cursor, &mut marker, true, &mut notify);

        cursor.get_node_mut().flush_metric_update(&mut marker);
        // cursor.compress_node();
    }

    #[inline(always)]
    pub fn insert_at_start_notify<F>(self: &mut Pin<Box<Self>>, new_entry: E, notify: F)
    where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        unsafe { Self::unsafe_insert_notify(&mut self.unsafe_cursor_at_start(), new_entry, notify) }
    }

    #[inline(always)]
    pub fn insert_at_start(self: &mut Pin<Box<Self>>, new_entry: E) {
        self.insert_at_start_notify(new_entry, null_notify);
    }

    #[inline(always)]
    pub fn push_notify<F>(self: &mut Pin<Box<Self>>, new_entry: E, notify: F)
    where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        unsafe { Self::unsafe_insert_notify(&mut self.unsafe_cursor_at_end(), new_entry, notify) }
    }

    /// Push a new entry to the end of the tree. The new entry will be merged with the existing
    /// last entry if possible.
    #[inline(always)]
    pub fn push(self: &mut Pin<Box<Self>>, new_entry: E)
    {
        self.push_notify(new_entry, null_notify);
    }

    /// Replace the item at the cursor position with the new items provided by items.
    ///
    /// Items must have a maximum length of 3, due to limitations in split_insert above.
    /// The cursor's offset is ignored. The cursor ends up at the end of the inserted items.
    #[allow(clippy::len_zero)]
    unsafe fn replace_entry<F>(cursor: &mut UnsafeCursor<E, I, IE, LE>, items: &[E], flush_marker: &mut I::Update, notify: &mut F)
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {
        assert!(items.len() >= 1 && items.len() <= 3);

        // Essentially here we want to:
        // 1. Concatenate as much from items as we can into the previous element
        // 2. With the rest:
        //   - If we run out of items, slide back (deleting the item under the cursor)
        //   - If we have 1 item left, replace inline
        //   - If we have more than 1 item left, replace then insert.
        // Even though we can delete items here, we will never end up with an empty node. So no
        // need to worry about the complex cases of delete.

        // Before anything else, we'll give a token effort trying to concatenate the item onto the
        // previous item.
        let mut items_idx = 0;
        let node = cursor.node.as_mut();
        debug_assert!(cursor.idx < node.len_entries());

        if cursor.idx >= 1 {
            let elem = &mut node.data[cursor.idx - 1];
            loop { // This is a crap for / while loop.
                let item = &items[items_idx];
                if elem.can_append(item) {
                    I::increment_marker(flush_marker, item);
                    elem.append(*item);
                    items_idx += 1;
                    if items_idx >= items.len() { break; }
                } else { break; }
            }
        }

        // let entry = cursor.get_raw_entry_mut();
        let entry = &mut node.data[cursor.idx];
        I::decrement_marker(flush_marker, entry);

        if items_idx >= items.len() {
            // Nuke the item under the cursor and shuffle everything back.
            node.splice_out(cursor.idx);
            if cursor.idx >= node.len_entries() {
                // The cursor might now be pointing past the end of this node.
                debug_assert!(node.len_entries() >= 1);
                cursor.idx -= 1;
                cursor.offset = node.data[cursor.idx].len();
            } else {
                cursor.offset = 0;
            }
        } else {
            // First replace the item directly.
            *entry = items[items_idx];
            I::increment_marker(flush_marker, entry);

            cursor.offset = entry.len();
            // notify(*entry, cursor.node);

            // And insert the rest, if there are any.
            Self::insert_internal(&items[items_idx + 1..], cursor, flush_marker, false, notify);
        }

        if cfg!(debug_assertions) {
            // The cursor should always end up inside the node.
            let node = cursor.get_node_mut();
            debug_assert!(cursor.idx < node.len_entries());
        }
    }

    /// Replace the current entry with the items passed via items[]. Items.len must be <= 3. The
    /// cursor offset is ignored. This is a fancy method - use sparingly.
    pub unsafe fn unsafe_replace_entry_notify<N>(cursor: &mut UnsafeCursor<E, I, IE, LE>, items: &[E], mut notify: N)
        where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {

        let mut flush_marker = I::Update::default();
        Self::replace_entry(cursor, items, &mut flush_marker, &mut notify);
        cursor.get_node_mut().flush_metric_update(&mut flush_marker);
    }

    #[inline]
    unsafe fn replace_entry_simple<F>(cursor: &mut UnsafeCursor<E, I, IE, LE>, new_item: E, flush_marker: &mut I::Update, notify: &mut F)
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {
        notify(new_item, cursor.node);
        cursor.offset = new_item.len();
        let entry = cursor.get_raw_entry_mut();
        I::decrement_marker(flush_marker, entry);
        *entry = new_item;
        I::increment_marker(flush_marker, entry);
    }

    pub unsafe fn unsafe_replace_entry_simple_notify<N>(cursor: &mut UnsafeCursor<E, I, IE, LE>, new_item: E, mut notify: N)
        where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {

        let mut flush_marker = I::Update::default();
        Self::replace_entry_simple(cursor, new_item, &mut flush_marker, &mut notify);
        cursor.get_node_mut().flush_metric_update(&mut flush_marker);
    }


    /// Replace as much of the current entry from cursor onwards as we can.
    unsafe fn unsafe_mutate_entry_internal<MapFn, N, R>(
        map_fn: MapFn,
        cursor: &mut UnsafeCursor<E, I, IE, LE>,
        replace_max: usize,
        flush_marker: &mut I::Update,
        notify: &mut N
    ) -> (usize, R)
    where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>), MapFn: FnOnce(&mut E) -> R
    {
        let node = cursor.get_node_mut();
        debug_assert!(cursor.idx < node.len_entries());
        let mut entry: E = node.data[cursor.idx];
        let mut entry_len = entry.len();

        assert!(cursor.offset < entry_len);

        // There's 1-3 parts here - part1<part2>part3

        // Trim off the first part
        let a = if cursor.offset > 0 {
            entry_len -= cursor.offset;
            Some(entry.truncate_keeping_right(cursor.offset))
        } else { None };

        // Trim off the last part
        let (c, replaced_here) = if replace_max < entry_len {
            (Some(entry.truncate(replace_max)), replace_max)
        } else { (None, entry_len) };

        let return_val = map_fn(&mut entry);

        match (a, c) {
            (Some(a), Some(c)) => {
                let c_len = c.len();
                Self::replace_entry(cursor, &[a, entry, c], flush_marker, notify);
                cursor.move_back_by_offset(c_len, Some(flush_marker));
            },
            (Some(a), None) => {
                Self::replace_entry(cursor, &[a, entry], flush_marker, notify);
            },
            (None, Some(c)) => {
                let c_len = c.len();
                Self::replace_entry(cursor, &[entry, c], flush_marker, notify);
                cursor.move_back_by_offset(c_len, Some(flush_marker));
            },
            (None, None) => {
                // Short circuit for:
                // self.replace_entry(&mut cursor, &[entry], &mut flush_marker, &mut notify);

                // TODO: Check if the replacement item can be appended to the previous element?
                // Self::replace_entry_simple(cursor, entry, flush_marker, notify);
                I::decrement_marker(flush_marker, &node.data[cursor.idx]);
                node.data[cursor.idx] = entry;
                cursor.offset = replaced_here;
                I::increment_marker(flush_marker, &entry);
                //notify(entry, cursor.node);
            }
        }

        (replaced_here, return_val)
    }

    pub unsafe fn unsafe_mutate_single_entry_notify<MapFn, R, N>(
        map_fn: MapFn,
        cursor: &mut UnsafeCursor<E, I, IE, LE>,
        replace_max: usize,
        mut notify: N
    ) -> (usize, R)
    where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>), MapFn: FnOnce(&mut E) -> R {
        let mut flush_marker = I::Update::default();
        let (amt_modified, ret) = Self::unsafe_mutate_entry_internal(map_fn, cursor, replace_max, &mut flush_marker, &mut notify);

        cursor.get_node_mut().flush_metric_update(&mut flush_marker);
        (amt_modified, ret)
    }

    pub unsafe fn unsafe_mutate_entries_notify<MapFn, N>(
        map_fn: MapFn,
        cursor: &mut UnsafeCursor<E, I, IE, LE>,
        replace_len: usize,
        mut notify: N
    )
    where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>), MapFn: Fn(&mut E) {
        let mut flush_marker = I::Update::default();
        let mut remaining = replace_len;
        while remaining > 0 {
            cursor.roll_to_next_entry_marker(&mut flush_marker);
            let (consumed_here, _) = Self::unsafe_mutate_entry_internal(&map_fn, cursor, remaining, &mut flush_marker, &mut notify);
            assert!(consumed_here > 0, "Could not mutate past end of list");
            remaining -= consumed_here;
            // cursor.next_entry_marker(Some(&mut flush_marker));
        }

        cursor.get_node_mut().flush_metric_update(&mut flush_marker);
    }

    /// Replace the range from cursor..cursor + replaced_len with new_entry.
    pub unsafe fn unsafe_replace_range_notify<N>(cursor: &mut UnsafeCursor<E, I, IE, LE>, new_entry: E, notify: N)
        where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {

        let mut flush_marker = I::Update::default();
        Self::replace_range_internal(cursor, new_entry.len(), new_entry, &mut flush_marker, notify);
        cursor.get_node_mut().flush_metric_update(&mut flush_marker);
        // cursor.compress_node();
    }

    unsafe fn replace_range_internal<N>(cursor: &mut UnsafeCursor<E, I, IE, LE>, mut replaced_len: usize, new_entry: E, flush_marker: &mut I::Update, mut notify: N)
        where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {

        let node = cursor.node.as_mut();

        if cursor.idx >= node.len_entries() {
            // The cursor already points past the end of the entry.
            cursor.roll_to_next_entry();
            Self::insert_internal(&[new_entry], cursor, flush_marker, true, &mut notify);
            return;
        }

        // Dirty.
        // if node.num_entries >= cursor.idx as u8 {
        //     // The only way this can happen normally is by creating a cursor at the end of the
        //     // document. So we're inserting, not replacing.
        //     self.insert_internal(&[new_entry], &mut cursor, flush_marker, &mut notify);
        // }

        let entry = &mut node.data[cursor.idx];
        let entry_len = entry.len();

        // This is awful. We're just going to have to go case by case.

        // If we can just append the new entry here, do that and delete.
        if cursor.offset == entry_len && entry.can_append(&new_entry) {
            assert!(cursor.offset > 0);
            notify(new_entry, cursor.node);
            I::increment_marker(flush_marker, &new_entry);
            entry.append(new_entry);
            cursor.offset += new_entry.len();

            Self::delete_internal(cursor, replaced_len, flush_marker, &mut notify);
            return;
        }

        if !cursor.roll_to_next_entry() { // Only valid because flush_marker is empty here.
            debug_assert_eq!(*flush_marker, I::Update::default());

            // We've reached the end of the tree. Can't replace more, so we just insert here.
            Self::insert_internal(&[new_entry], cursor, flush_marker, false, &mut notify);
            return;
        }

        let mut node = cursor.node.as_mut();
        let mut entry = &mut node.data[cursor.idx];
        let mut entry_len = entry.len();

        if cursor.offset > 0 {
            if cursor.offset + replaced_len < entry_len {
                // We're replacing a strict subset. Delegate to replace_entry[a, new, c].
                let mut a = *entry;
                a.truncate(cursor.offset);

                let mut c = *entry;
                c.truncate_keeping_right(cursor.offset + replaced_len);
                let c_len = c.len();

                // This will update flush_marker for us.
                Self::replace_entry(cursor, &[a, new_entry, c], flush_marker, &mut notify);

                // Move the cursor back to be pointing at the end of new_entry.
                cursor.move_back_by_offset(c_len, Some(flush_marker));
                return;
            } else {
                // Remove (truncate) the remainder of this entry. Then continue.
                let removed = entry.truncate(cursor.offset);
                I::decrement_marker(flush_marker, &removed);
                replaced_len -= entry_len - cursor.offset;
                debug_assert_eq!(entry_len - cursor.offset, removed.len());

                if replaced_len == 0 || !cursor.next_entry_marker(Some(flush_marker)) {
                    // Only inserting remains.
                    Self::insert_internal(&[new_entry], cursor, flush_marker, false, &mut notify);
                    return;
                }

                // Could check for appending in this case, but its unlikely given we've just
                // truncated. (Unless we're replacing like for like).
                node = cursor.node.as_mut();
                entry = &mut node.data[cursor.idx];
                entry_len = entry.len();
            }
        }

        debug_assert_eq!(cursor.offset, 0);

        if replaced_len >= entry_len {
            // Replace this item inline.
            // Note that even if the size hasn't changed, they might have different character
            // sizes or something like that.
            I::decrement_marker(flush_marker, entry);
            I::increment_marker(flush_marker, &new_entry);
            notify(new_entry, cursor.node);
            cursor.offset = new_entry.len();
            *cursor.get_raw_entry_mut() = new_entry;

            if replaced_len > entry_len {
                // Delete any extra trailing length.
                cursor.next_entry_marker(Some(flush_marker));
                Self::delete_internal(cursor, replaced_len - entry_len, flush_marker, &mut notify);
            } // Otherwise we're done.
        } else { // replaced_len < entry_len
            // Replace this item with [new, remainder].
            let mut remainder = *entry;
            let remainder = remainder.truncate(replaced_len);
            let rem_len = remainder.len();
            Self::replace_entry(cursor, &[new_entry, remainder], flush_marker, &mut notify);
            cursor.move_back_by_offset(rem_len, Some(flush_marker));
        }
    }

    /// Internal method to remove whole entries inside the current leaf. Could be moved into Leaf.
    /// It doesn't really make sense to take a &Self here.
    ///
    /// This method requires that the passed cursor is at the start of an item. (cursor.offset = 0).
    ///
    /// We return a tuple of (should_iterate, the number of remaining items to delete).
    /// If should_iterate is true, keep calling this in a loop. (Eh I need a better name for that
    /// variable).
    unsafe fn delete_entry_range(cursor: &mut UnsafeCursor<E, I, IE, LE>, mut del_items: usize, flush_marker: &mut I::Update) -> (bool, usize) {
        // This method only deletes whole items.
        debug_assert_eq!(cursor.offset, 0);
        debug_assert!(del_items > 0);

        let mut node = cursor.get_node_mut();
        // If the cursor is at the end of the leaf, flush and roll.
        if cursor.idx >= node.num_entries as usize {
            node.flush_metric_update(flush_marker);
            // If we reach the end of the tree, discard trailing deletes.
            if !cursor.traverse_forward() { return (false, 0); }
            node = cursor.get_node_mut();
        }

        if cursor.idx >= LE { unreachable_unchecked(); }
        let start_range = cursor.idx;
        let mut end_range = cursor.idx;

        // 1. Find the end index to remove
        let len_entries = node.len_entries();
        // let mut node = unsafe { &mut *cursor.node.as_ptr() };
        while end_range < len_entries && del_items > 0 {
            let entry = node.data[end_range];
            let entry_len = entry.len();
            if entry_len <= del_items {
                I::decrement_marker(flush_marker, &entry);
                del_items -= entry_len;
                end_range += 1;
            } else {
                break;
            }
        }

        if start_range == 0 && end_range == len_entries && !node.has_root_as_parent() {
            // Remove the entire leaf from the tree.
            node.flush_metric_update(flush_marker);

            let node = cursor.node;
            let has_next = cursor.traverse_forward();
            if !has_next {
                // This is weird and hacky but - we've just deleted the last item in the tree.
                // If the cursor is still pointing to this element afterwards, the cursor will
                // be invalid. So instead I'll move the cursor to the end of the previous item.
                //
                // If this is the only item, the cursor will stay here. (traverse_backward
                // returns false). And the item itself will end up being reused by the
                // NodeLeaf::remove() logic.
                //
                // The resulting behaviour of all this is tested by the fuzzer. If any of these
                // assumptions break later, the tests should catch it.
                cursor.traverse_backwards();
                del_items = 0; // There's nothing remaining to delete.
            }

            NodeLeaf::remove(node);
            (has_next, del_items)
        } else if end_range > start_range {
            // Delete from [start_range..end_range)
            // println!("Delete entry range from {} to {} (m: {:?})", start_range, end_range, flush_marker);
            let len_entries = node.len_entries();
            let tail_count = len_entries - end_range;
            if tail_count > 0 {
                node.data.copy_within(end_range..len_entries, start_range);
            }
            node.num_entries = (start_range + tail_count) as u8;

            // If the result is to remove all entries, the leaf should have been removed instead.
            debug_assert!(node.num_entries > 0 || node.parent.is_root());

            // This is unnecessary but for some total magic reason, disabling this results in a
            // performance regression.
            // #[cfg(debug_assertions)]
            node.data[start_range + tail_count..].fill(E::default());

            // TODO: And rebalance if the node is now less than half full.
            (true, del_items)
        } else {
            (false, del_items)
        }
    }

    unsafe fn delete_internal<N>(cursor: &mut UnsafeCursor<E, I, IE, LE>, mut del_items: usize, flush_marker: &mut I::Update, notify: &mut N)
        where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {

        if del_items == 0 { return; }

        // First trim the current element.
        if cursor.offset > 0 {
            let node = cursor.node.as_mut();
            let entry = &mut node.data[cursor.idx];
            let entry_len = entry.len();

            let remaining_len = entry_len - cursor.offset;
            if remaining_len > 0 {
                if remaining_len <= del_items {
                    // Simply truncate and discard the rest of this entry.
                    I::decrement_marker(flush_marker, &entry.truncate(cursor.offset));
                    del_items -= remaining_len;
                    if del_items == 0 { return; }
                } else { // remaining_len > del_items
                    let mut remainder = entry.truncate(cursor.offset);
                    I::decrement_marker(flush_marker, &remainder);

                    remainder.truncate_keeping_right(del_items);

                    // And insert the rest, if there are any. I'm using insert() to do this because
                    // we don't want our cursor changed as a result of the insert. This also makes
                    // a fresh flush marker, but that's not a big deal.

                    // The code below is equivalent to, but marginally faster than:
                    // self.insert(cursor.clone(), remainder, notify);

                    let mut c2 = cursor.clone();
                    Self::insert_internal(&[remainder], &mut c2, flush_marker, false, notify);
                    c2.get_node_mut().flush_metric_update(flush_marker);

                    return;
                }
            }

            // If we've run out of items in the tree to delete, silently return.
            if !cursor.next_entry_marker(Some(flush_marker)) { return; }
        }

        debug_assert!(del_items > 0);
        debug_assert_eq!(cursor.offset, 0);

        // Ok, we're at the start of an entry. Scan and delete entire entries from this leaf.

        while del_items > 0 {
            let (iterate, num) = Self::delete_entry_range(cursor, del_items, flush_marker);
            del_items = num;
            if !iterate { break; }
            // delete_entry_range only deletes from the current item each iteration.
        }


        let node = cursor.node.as_mut();
        if del_items > 0 {
            // Trim the final entry.
            //
            // Note this code doesn't handle the case when del_items > 0 but we're at the end of the
            // tree. Thats currently impossible given the code in delete_entry_range() according to the
            // fuzzer, so its probably not something to be concerned by.

            // let node = unsafe { cursor.get_node_mut() };
            debug_assert!(cursor.idx < node.len_entries());
            debug_assert!(node.data[cursor.idx].len() > del_items);

            let trimmed = node.data[cursor.idx].truncate_keeping_right(del_items);
            I::decrement_marker(flush_marker, &trimmed);
        } else if cursor.idx >= node.len_entries() {
            debug_assert_eq!(cursor.offset, 0);
            if cursor.idx == 0 {
                // We've removed all items in the tree.
                cursor.offset = usize::MAX;
                debug_assert!(node.parent.is_root());
            } else {
                cursor.idx -= 1;
                cursor.offset = node.data[cursor.idx].len();
            }
        }
    }

    /// Delete the specified number of items from the b-tree at the cursor.
    /// Cursor may be modified to point to the start of the next item.
    pub unsafe fn unsafe_delete_notify<F>(cursor: &mut UnsafeCursor<E, I, IE, LE>, del_items: usize, mut notify: F)
    where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        let mut marker = I::Update::default();
        Self::delete_internal(cursor, del_items, &mut marker, &mut notify);
        cursor.get_node_mut().flush_metric_update(&mut marker);
    }

    pub fn delete_at_start_notify<F>(self: &mut Pin<Box<Self>>, del_items: usize, mut notify: F)
    where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        let mut marker = I::Update::default();
        let mut cursor = self.unsafe_cursor_at_start();
        unsafe {
            Self::delete_internal(&mut cursor, del_items, &mut marker, &mut notify);
            cursor.get_node_mut().flush_metric_update(&mut marker);
        }
    }

    pub fn delete_at_start(self: &mut Pin<Box<Self>>, del_items: usize) {
        self.delete_at_start_notify(del_items, null_notify);
    }
}

impl<E: ContentTraits + Toggleable, I: TreeMetrics<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    pub unsafe fn local_deactivate_notify<F>(self: &mut Pin<Box<Self>>, mut cursor: UnsafeCursor<E, I, IE, LE>, deleted_len: usize, mut notify: F) -> DeleteResult<E>
    where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        // println!("local_delete len: {} at cursor {:?}", deleted_len, cursor);

        if cfg!(debug_assertions) {
            // TODO: Restore this.
            // let cursor_pos = cursor.count_pos();
            // assert!(cursor_pos + deleted_len <= self.count);
        }
        // dbg!(cursor_pos, self.count);

        // TODO: And this.
        // let expected_size = self.count - deleted_len;

        let mut result: DeleteResult<E> = SmallVec::default();
        let mut flush_marker = I::Update::default();
        let mut delete_remaining = deleted_len;
        cursor.roll_to_next_entry();

        while delete_remaining > 0 {
            // We're iterating through entries, marking entries for delete along the way.
            // debug_assert!(cursor.get_raw_entry().is_valid());

            while cursor.get_raw_entry().is_deactivated() {
                Self::next_entry_or_panic(&mut cursor, &mut flush_marker);
            }

            // dbg!(self, delete_remaining, &flush_marker);

            delete_remaining -= Self::unsafe_mutate_entry_internal(|e| {
                result.push_rle(*e);
                e.mark_deactivated();
            }, &mut cursor, delete_remaining, &mut flush_marker, &mut notify).0;
        }
        cursor.compress_node();

        // The cursor is potentially after any remainder.
        cursor.get_node_mut().flush_metric_update(&mut flush_marker);

        if cfg!(debug_assertions) {
            // self.print_ptr_tree();
            // self.as_ref().get_ref().check();

            // Check the total size of the tree has grown by len.
            // assert_eq!(expected_size, self.count);
        }

        result
    }

    unsafe fn set_enabled<F>(cursor: &mut UnsafeCursor<E, I, IE, LE>, max_len: usize, want_enabled: bool, notify: F) -> (usize, bool)
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {

        cursor.roll_to_next_entry();
        let entry = cursor.get_raw_entry();

        if entry.is_activated() != want_enabled {
            // The region could be in the middle of an item and that has all sorts of complexity.
            // Just delegate to mutate_entry above, which will take care of all that jazz.
            //
            // Even though we're just editing an item here, the item could be split as a result,
            // so notify may end up called.
            let (amt_modified, _) = Self::unsafe_mutate_single_entry_notify(|e| {
                if want_enabled { e.mark_activated(); } else { e.mark_deactivated(); }
            }, cursor, max_len, notify);

            (amt_modified, true)
        } else {
            // The range has already been activated / deactivated.
            (max_len.min(entry.len() - cursor.offset), false)
        }
    }

    /// Deactivate up to max_deleted_len from the marker tree, at the location specified by cursor.
    /// We will always process at least one item. Consumers of this API should call this in a loop.
    ///
    /// If the entry is already marked as deleted, unlike local_deactivate, this method does
    /// nothing. local_deactivate will skip over deleted items and delete something else.
    ///
    /// Returns the number of items we tried to deactivate, and whether we were successful.
    /// (eg (1, true) means we marked 1 item for deletion. (2, false) means we skipped past 2 items
    /// which were already deactivated.
    ///
    /// TODO: It might be cleaner to make the caller check for deleted items if we return 0.
    ///
    /// TODO: Consider returning / mutating the cursor. Subsequent items will probably be in this
    /// node. It would be marginally faster to find a cursor using a hint, and subsequent deletes
    /// in the txn we're applying will usually be in this node (usually the next item in this node).
    pub unsafe fn unsafe_remote_deactivate_notify<F>(cursor: &mut UnsafeCursor<E, I, IE, LE>, max_deleted_len: usize, notify: F) -> (usize, bool)
    where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        Self::set_enabled(cursor, max_deleted_len, false, notify)
    }

    pub unsafe fn unsafe_remote_reactivate_notify<F>(cursor: &mut UnsafeCursor<E, I, IE, LE>, max_len: usize, notify: F) -> (usize, bool)
    where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        Self::set_enabled(cursor, max_len, true, notify)
    }
}

impl<E: ContentTraits, I: FindOffset<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    // TODO: All these methods could just use self.mut_cursor_at...
    pub fn insert_at_offset_notify<F>(self: &mut Pin<Box<Self>>, pos: usize, new_entry: E, notify: F)
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        let mut cursor = self.unsafe_cursor_at_offset_pos(pos, true);
        unsafe { Self::unsafe_insert_notify(&mut cursor, new_entry, notify); }
    }

    pub fn insert_at_offset(self: &mut Pin<Box<Self>>, pos: usize, new_entry: E) {
        let mut cursor = self.unsafe_cursor_at_offset_pos(pos, true);
        unsafe { Self::unsafe_insert_notify(&mut cursor, new_entry, null_notify); }
    }

    pub fn replace_range_at_offset_notify<N>(self: &mut Pin<Box<Self>>, offset: usize, new_entry: E, notify: N)
        where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        let mut cursor = self.unsafe_cursor_at_offset_pos(offset, true);
        unsafe { Self::unsafe_replace_range_notify(&mut cursor, new_entry, notify); }
    }

    pub fn replace_range_at_offset(self: &mut Pin<Box<Self>>, offset: usize, new_entry: E) {
        let mut cursor = self.unsafe_cursor_at_offset_pos(offset, true);
        unsafe { Self::unsafe_replace_range_notify(&mut cursor, new_entry, null_notify); }
    }

    pub fn delete_at_offset_notify<F>(self: &mut Pin<Box<Self>>, pos: usize, del_items: usize, notify: F)
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        let mut cursor = self.unsafe_cursor_at_offset_pos(pos, false);
        unsafe { Self::unsafe_delete_notify(&mut cursor, del_items, notify); }
    }

    pub fn delete_at_offset(self: &mut Pin<Box<Self>>, pos: usize, del_items: usize) {
        let mut cursor = self.unsafe_cursor_at_offset_pos(pos, false);
        unsafe { Self::unsafe_delete_notify(&mut cursor, del_items, null_notify); }
    }
}

impl<E: ContentTraits + ContentLength, I: FindContent<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    pub fn insert_at_content_notify<F>(self: &mut Pin<Box<Self>>, pos: usize, new_entry: E, notify: F)
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        let mut cursor = self.unsafe_cursor_at_content_pos(pos, true);
        unsafe { Self::unsafe_insert_notify(&mut cursor, new_entry, notify); }
    }

    pub fn insert_at_content(self: &mut Pin<Box<Self>>, pos: usize, new_entry: E) {
        let mut cursor = self.unsafe_cursor_at_content_pos(pos, true);
        unsafe { Self::unsafe_insert_notify(&mut cursor, new_entry, null_notify); }
    }

    pub fn replace_range_at_content_notify<N>(self: &mut Pin<Box<Self>>, pos: usize, new_entry: E, notify: N)
        where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        let mut cursor = self.unsafe_cursor_at_content_pos(pos, true);
        unsafe { Self::unsafe_replace_range_notify(&mut cursor, new_entry, notify); }
    }
    pub fn replace_range_at_content(self: &mut Pin<Box<Self>>, pos: usize, new_entry: E) {
        let mut cursor = self.unsafe_cursor_at_content_pos(pos, true);
        unsafe { Self::unsafe_replace_range_notify(&mut cursor, new_entry, null_notify); }
    }

    pub fn delete_at_content_notify<F>(self: &mut Pin<Box<Self>>, pos: usize, del_items: usize, notify: F)
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        let mut cursor = self.unsafe_cursor_at_content_pos(pos, false);
        unsafe { Self::unsafe_delete_notify(&mut cursor, del_items, notify); }
    }
    pub fn delete_at_content(self: &mut Pin<Box<Self>>, pos: usize, del_items: usize) {
        let mut cursor = self.unsafe_cursor_at_content_pos(pos, false);
        unsafe { Self::unsafe_delete_notify(&mut cursor, del_items, null_notify); }
    }
}

impl<E: ContentTraits + ContentLength + Toggleable, I: FindContent<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    pub fn local_deactivate_at_content_notify<F>(self: &mut Pin<Box<Self>>, offset: usize, deleted_len: usize, notify: F) -> DeleteResult<E>
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        let cursor = self.unsafe_cursor_at_content_pos(offset, false);
        unsafe { self.local_deactivate_notify(cursor, deleted_len, notify) }
    }
}

impl<E: ContentTraits, I: TreeMetrics<E>, const IE: usize, const LE: usize> NodeLeaf<E, I, IE, LE> {

    /// Split this leaf node at the specified index, so 0..idx stays and idx.. moves to a new node.
    ///
    /// The new node has additional `padding` empty items at the start of its list.
    fn split_at<F>(&mut self, idx: usize, padding: usize, notify: &mut F) -> NonNull<NodeLeaf<E, I, IE, LE>>
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>)
    {
        // println!("split_at {} {}", idx, padding);
        unsafe {
            // TODO(optimization): We're currently copying / moving everything *after* idx. If idx
            // is small, we could instead move everything before idx - which would save a bunch of
            // calls to notify and save us needing to fix up a bunch of parent pointers. More work
            // here, but probably less work overall.

            let mut new_node = Self::new(self.next); // The new node has a danging parent pointer
            let new_filled_len = self.len_entries() - idx;
            let new_len = new_filled_len + padding;
            debug_assert!(new_len <= LE);

            if new_filled_len > 0 {
                ptr::copy_nonoverlapping(&self.data[idx], &mut new_node.data[padding], new_filled_len);
            }

            new_node.num_entries = new_len as u8; // Not including padding!

            // zero out the old entries
            // let mut stolen_length: usize = 0;
            let mut stolen_length = I::Value::default();
            // dbg!(&self.data);
            for e in &mut self.data[idx..self.num_entries as usize] {
                I::increment_offset(&mut stolen_length, e);
                // stolen_length += e.content_len();
                *e = E::default();
            }
            self.num_entries = idx as u8;

            // eprintln!("split_at idx {} stolen_length {:?} self {:?}", idx, stolen_length, &self);

            let mut new_node_boxed = Box::pin(new_node);

            // This is the pointer to the new item we'll end up returning.
            let new_leaf_ptr = NonNull::new_unchecked(new_node_boxed.as_mut().get_unchecked_mut());
            self.next = Some(new_leaf_ptr);

            for e in &new_node_boxed.as_ref().data[padding..new_len] {
                notify(*e, new_leaf_ptr);
            }

            insert_after(self.parent, Node::Leaf(new_node_boxed), NodePtr::Leaf(NonNull::new_unchecked(self)), stolen_length);

            // TODO: It would be cleaner to return a Pin<&mut NodeLeaf> here instead of the pointer.
            new_leaf_ptr
        }
    }

    /// Remove this leaf from the tree. Cursor positioned after leaf.
    ///
    /// It is invalid to call this on the last node in the tree - which will have the parent as a
    /// root.
    unsafe fn remove(self_ptr: NonNull<NodeLeaf<E, I, IE, LE>>) {
        // I'm really not sure what sort of self reference this method should take. We could take a
        // Pin<*mut Self> - which feels more correct. Using NonNull<Self> is convenient because of
        // cursor, though we'll dereference it anyway so maybe Pin<&mut Self>? O_o
        //
        // Function is unsafe.
        let leaf = self_ptr.as_ref();
        debug_assert!(!leaf.has_root_as_parent());

        if let Some(mut prev) = leaf.prev_leaf() {
            prev.as_mut().next = leaf.next;
        }

        NodeInternal::remove_leaf(leaf.parent.unwrap_internal(), self_ptr);
    }
}

impl<E: ContentTraits, I: TreeMetrics<E>, const IE: usize, const LE: usize> NodeInternal<E, I, IE, LE> {
    unsafe fn slice_out(&mut self, child: NodePtr<E, I, IE, LE>) -> Node<E, I, IE, LE> {
        if self.children[1].is_none() {
            // short circuit.

            // If we're in this situation, children[0] must be Some(child).
            debug_assert_eq!(self.find_child(child).unwrap(), 0);

            self.children[0].take().unwrap()
        } else {
            let idx = self.find_child(child).unwrap();
            let num_children = self.count_children();

            let removed = self.children[idx].take().unwrap();

            let count = num_children - idx - 1;
            if count > 0 {
                ptr::copy(
                    &self.children[idx + 1],
                    &mut self.children[idx],
                    count
                );

                self.metrics.copy_within(idx + 1..num_children, idx);
            }

            // This pointer has been moved. We need to set its entry to None without dropping it.
            std::mem::forget(self.children[num_children - 1].take());

            removed
        }
    }

    unsafe fn remove_leaf(mut self_ptr: NonNull<NodeInternal<E, I, IE, LE>>, child: NonNull<NodeLeaf<E, I, IE, LE>>) {
        let spare = self_ptr.as_mut().slice_out(NodePtr::Leaf(child));
        Self::ripple_delete(self_ptr, spare);
    }

    unsafe fn ripple_delete(mut self_ptr: NonNull<NodeInternal<E, I, IE, LE>>, mut spare_leaf: Node<E, I, IE, LE>) {
        debug_assert!(spare_leaf.is_leaf());

        let self_ref = self_ptr.as_mut();

        if self_ref.children[0].is_none() {
            // This child is empty. Remove it from its parent.
            match self_ref.parent {
                ParentPtr::Root(mut root) => {
                    // We're removing the last item from the tree. The tree must always have at
                    // least 1 item, so we need to replace the single child. We could replace it
                    // with a fresh node, which would be simpler, but doing that would mess up the
                    // cursor (which we don't have access to here). And it would require an
                    // additional allocation - though this is rare anyway.
                    let root = root.as_mut();
                    spare_leaf.set_parent(root.to_parent_ptr());
                    // spare_leaf.unwrap_leaf_mut().get_unchecked_mut().num_entries = 0;
                    spare_leaf.unwrap_leaf_mut().get_unchecked_mut().clear_all();
                    root.root = spare_leaf;
                }
                ParentPtr::Internal(mut parent) => {
                    // Remove recursively.
                    parent.as_mut().slice_out(NodePtr::Internal(self_ptr));
                    Self::ripple_delete(parent, spare_leaf);
                }
            }
        }
    }
}


// I'm really not sure where to put these methods. Its not really associated with
// any of the tree implementation methods. This seems like a hidden spot. Maybe
// content-tree? I could put it in impl ParentPtr? I dunno...
unsafe fn insert_after<E: ContentTraits, I: TreeMetrics<E>, const INT_ENTRIES: usize, const LEAF_ENTRIES: usize>(
    mut parent: ParentPtr<E, I, INT_ENTRIES, LEAF_ENTRIES>,
    mut inserted_leaf_node: Node<E, I, INT_ENTRIES, LEAF_ENTRIES>,
    mut insert_after: NodePtr<E, I, INT_ENTRIES, LEAF_ENTRIES>,
    mut stolen_length: I::Value) {
    // println!("insert_after {:?} leaf {:#?} parent {:#?}", stolen_length, inserted_leaf_node, parent);
    // Ok now we need to walk up the tree trying to insert. At each step
    // we will try and insert inserted_node into parent next to old_node
    // (topping out at the head).
    loop {
        // First try and simply emplace in the new element in the parent.
        if let ParentPtr::Internal(mut n) = parent {
            let parent_ref = n.as_ref();
            let count = parent_ref.count_children();
            if count < INT_ENTRIES {
                // Great. Insert the new node into the parent and return.
                inserted_leaf_node.set_parent(ParentPtr::Internal(n));

                let old_idx = parent_ref.find_child(insert_after).unwrap();
                let new_idx = old_idx + 1;

                let parent_ref = n.as_mut();
                // dbg!(&parent_ref.data[old_idx].0, stolen_length);
                parent_ref.metrics[old_idx] -= stolen_length;
                parent_ref.splice_in(new_idx, stolen_length, inserted_leaf_node);

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
                let mut count = root.count;
                let mut new_internal_root = root.root.unwrap_internal_mut();
                // let parent_ptr = ParentPtr::Internal(NonNull::new_unchecked(new_root_ref));
                let parent_ptr = new_internal_root.as_ref().to_parent_ptr();

                // Reassign parents for each node
                old_root.set_parent(parent_ptr);
                inserted_leaf_node.set_parent(parent_ptr);

                count -= stolen_length;
                new_internal_root.as_mut().set_entry(0, count, Some(old_root));
                new_internal_root.as_mut().set_entry(1, stolen_length, Some(inserted_leaf_node));

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
                debug_assert!(left_sibling.count_children() == INT_ENTRIES);

                // let mut right_sibling = NodeInternal::new_with_parent(parent);
                let mut right_sibling_box = Node::Internal(NodeInternal::new_with_parent(parent));
                let mut right_sibling = right_sibling_box.unwrap_internal_mut();
                let old_idx = left_sibling.find_child(insert_after).unwrap();

                let left_sibling = n.as_mut();
                left_sibling.metrics[old_idx] -= stolen_length;
                let mut new_stolen_length = I::Value::default();
                // Dividing this into cases makes it easier to reason
                // about.
                if old_idx < INT_ENTRIES /2 {
                    // Move all items from MAX_CHILDREN/2..MAX_CHILDREN
                    // into right_sibling, then splice inserted_node into
                    // old_parent.
                    for i in 0..INT_ENTRIES /2 {
                        let ii = i + INT_ENTRIES /2;
                        // let c = mem::replace(&mut left_sibling.index[ii], I::IndexOffset::default());
                        let c = mem::take(&mut left_sibling.metrics[ii]);
                        // let e = mem::replace(&mut left_sibling.children[ii], None);
                        let e = mem::take(&mut left_sibling.children[ii]);
                        if let Some(mut e) = e {
                            e.set_parent(right_sibling.as_ref().to_parent_ptr());
                            new_stolen_length += c;
                            right_sibling.as_mut().set_entry(i, c, Some(e));
                        }

                    }

                    let new_idx = old_idx + 1;
                    inserted_leaf_node.set_parent(ParentPtr::Internal(NonNull::new_unchecked(left_sibling)));
                    left_sibling.splice_in(new_idx, stolen_length, inserted_leaf_node);
                } else {
                    // The new element is in the second half of the
                    // group.
                    let new_idx = old_idx - INT_ENTRIES /2 + 1;

                    inserted_leaf_node.set_parent(right_sibling.as_ref().to_parent_ptr());
                    let mut new_entry = (stolen_length, Some(inserted_leaf_node));
                    new_stolen_length = stolen_length;

                    let mut src = INT_ENTRIES /2;
                    for dest in 0..=INT_ENTRIES /2 {
                        if dest == new_idx {
                            right_sibling.as_mut().set_entry(dest, mem::take(&mut new_entry.0), mem::take(&mut new_entry.1));
                        } else {
                            let c = mem::take(&mut left_sibling.metrics[src]);
                            let e = mem::take(&mut left_sibling.children[src]);
                            // let (c, e) = mem::replace(&mut left_sibling.data[src], (I::IndexOffset::default(), None));

                            if let Some(mut e) = e {
                                e.set_parent(right_sibling.as_ref().to_parent_ptr());
                                new_stolen_length += c;
                                right_sibling.as_mut().set_entry(dest, c, Some(e));
                                src += 1;
                            } else { break; }
                        }
                    }
                    debug_assert!(new_entry.1.is_none());
                }

                insert_after = NodePtr::Internal(n);
                inserted_leaf_node = right_sibling_box;
                stolen_length = new_stolen_length;
                // And iterate up the tree.
            },
        };
    }
}

#[cfg(test)]
mod tests {
    // use std::pin::Pin;
    use super::*;
    use crate::testrange::TestRange;

    #[test]
    fn splice_insert_test() {
        let mut tree = ContentTreeRaw::<TestRange, ContentMetrics, DEFAULT_IE, DEFAULT_LE>::new();
        let entry = TestRange {
            id: 1000,
            len: 100,
            is_activated: true
        };
        tree.insert_at_content(15, entry);
        tree.check();

        let entry = TestRange {
            id: 1100,
            len: 20,
            is_activated: true
        };
        tree.insert_at_content(15, entry);
        tree.check();

        // println!("{:#?}", tree);
        assert_eq!(tree.raw_iter().collect::<Vec<TestRange>>(), vec![
            TestRange { id: 1000, len: 15, is_activated: true },
            TestRange { id: 1100, len: 20, is_activated: true },
            TestRange { id: 1015, len: 85, is_activated: true },
        ]);
    }

    #[test]
    fn delete_collapses() {
        let mut tree = ContentTreeRaw::<TestRange, ContentMetrics, DEFAULT_IE, DEFAULT_LE>::new();

        let entry = TestRange {
            id: 1000,
            len: 100,
            is_activated: true,
        };
        tree.insert_at_content(0, entry);
        assert_eq!(tree.count_entries(), 1);

        // I'm going to delete two items in the middle.
        tree.local_deactivate_at_content_notify(50, 1, null_notify);
        assert_eq!(tree.count_entries(), 3);

        // dbg!(&tree);
        tree.local_deactivate_at_content_notify(50, 1, null_notify);
        // dbg!(&tree);

        assert_eq!(tree.raw_iter().collect::<Vec<TestRange>>(), vec![
            TestRange { id: 1000, len: 50, is_activated: true },
            TestRange { id: 1050, len: 2, is_activated: false },
            TestRange { id: 1052, len: 48, is_activated: true },
        ]);
    }

    #[test]
    fn backspace_collapses() {
        let mut tree = ContentTreeRaw::<TestRange, ContentMetrics, DEFAULT_IE, DEFAULT_LE>::new();

        let entry = TestRange {
            id: 1000,
            len: 100,
            is_activated: true,
        };
        tree.insert_at_content_notify(0, entry, null_notify);
        assert_eq!(tree.count_entries(), 1);

        // Ok now I'm going to delete the last and second-last elements. We should end up with
        // two entries.
        tree.local_deactivate_at_content_notify(99, 1, null_notify);
        assert_eq!(tree.count_entries(), 2);

        tree.local_deactivate_at_content_notify(98, 1, null_notify);
        assert_eq!(tree.count_entries(), 2);

        assert_eq!(tree.raw_iter().collect::<Vec<TestRange>>(), vec![
            TestRange { id: 1000, len: 98, is_activated: true },
            TestRange { id: 1098, len: 2, is_activated: false },
        ]);
        tree.check();
    }

    #[test]
    fn delete_single_item() {
        let mut tree = ContentTreeRaw::<TestRange, ContentMetrics, DEFAULT_IE, DEFAULT_LE>::new();
        tree.insert_at_start(TestRange { id: 0, len: 10, is_activated: true });

        tree.delete_at_start(10);
        assert_eq!(tree.len(), 0);
        tree.check();
    }

    #[test]
    fn delete_all_items() {
        let mut tree = ContentTreeRaw::<TestRange, ContentMetrics, DEFAULT_IE, DEFAULT_LE>::new();
        let num = DEFAULT_LE + 1;
        for i in 0..num {
            tree.insert_at_start_notify(TestRange { id: i as _, len: 10, is_activated: true }, null_notify);
        }
        // dbg!(&tree);
        assert!(!tree.root.is_leaf());

        tree.delete_at_start_notify(10 * num, null_notify);
        assert_eq!(tree.len(), 0);
        tree.check();
    }

    #[test]
    fn delete_past_end() {
        let mut tree = ContentTreeRaw::<TestRange, ContentMetrics, DEFAULT_IE, DEFAULT_LE>::new();
        tree.insert_at_start_notify(TestRange { id: 10 as _, len: 10, is_activated: true }, null_notify);
        tree.delete_at_content_notify(10, 100, null_notify);

        assert_eq!(tree.raw_iter().collect::<Vec<TestRange>>(), vec![
            TestRange { id: 10, len: 10, is_activated: true },
        ]);
    }

    #[test]
    fn push_into_empty() {
        let mut tree = ContentTreeRaw::<TestRange, ContentMetrics, DEFAULT_IE, DEFAULT_LE>::new();
        tree.push(TestRange { id: 0, len: 10, is_activated: true });
    }

    #[test]
    fn mutation_wrappers() {
        let mut tree = ContentTreeRaw::<TestRange, FullMetricsU32, DEFAULT_IE, DEFAULT_LE>::new();
        tree.insert_at_content(0, TestRange { id: 0, len: 10, is_activated: true });
        assert_eq!(tree.offset_len(), 10);
        assert_eq!(tree.content_len(), 10);

        tree.replace_range_at_content(3, TestRange { id: 100, len: 3, is_activated: false });
        assert_eq!(tree.offset_len(), 10);
        assert_eq!(tree.content_len(), 7);

        assert_eq!(tree.at_content(4), Some((7, true)));
        assert_eq!(tree.at_offset(4), Some((101, false)));

        // TODO: Eh and do the others - insert_at_offset, replace_range_at_offset, etc.
        tree.delete_at_offset(5, 3);
        assert_eq!(tree.offset_len(), 7);
        assert_eq!(tree.content_len(), 5);

        tree.delete_at_content(0, 1);
        assert_eq!(tree.offset_len(), 6);
        assert_eq!(tree.content_len(), 4);
    }

    #[test]
    fn mutate_range() {
        let mut tree = ContentTreeRaw::<TestRange, FullMetricsU32, DEFAULT_IE, DEFAULT_LE>::new();
        tree.push(TestRange { id: 0, len: 10, is_activated: true });

        unsafe {
            let mut cursor = tree.unsafe_cursor_at_offset_pos(5, false);
            ContentTreeRaw::unsafe_mutate_entries_notify(|r| {
                assert_eq!(r, &TestRange { id: 5, len: 2, is_activated: true });

                r.len = 1;
            }, &mut cursor, 2, null_notify);
        }
        // Tree now contains [0..5] [7..10].
        // dbg!(&tree);
        unsafe {
            let mut cursor = tree.unsafe_cursor_at_offset_pos(5, false);
            ContentTreeRaw::unsafe_mutate_entries_notify(|r| {
                // We should get 5 then 7.
                assert_eq!(r.len, 1);
                assert!(r.id == 5 || r.id == 7);
                r.len += 1;
            }, &mut cursor, 2, null_notify);
        }

        // This looks wrong, but its right.
        assert_eq!(tree.raw_iter().collect::<Vec<TestRange>>(), vec![
            TestRange { id: 0, len: 9, is_activated: true },
            TestRange { id: 8, len: 2, is_activated: true },
        ]);
        // dbg!(&tree);
    }
}
