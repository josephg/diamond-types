use std::marker::PhantomData;
use std::ops::Deref;

use rle::Searchable;

use super::*;

/// This file provides the safe implementation methods for cursors.

impl<'a, E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> Cursor<'a, E, I, IE, LE> {
    pub unsafe fn unchecked_from_raw(_tree: &'a ContentTree<E, I, IE, LE>, cursor: UnsafeCursor<E, I, IE, LE>) -> Self {
        Cursor {
            inner: cursor,
            marker: PhantomData
        }
    }

    // TODO: Implement from_raw as well, where we walk up the tree to check the root.

    pub fn count_pos(&self) -> I::IndexValue {
        unsafe { self.inner.count_pos() }
    }
}

impl<'a, E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> Deref for Cursor<'a, E, I, IE, LE> {
    type Target = UnsafeCursor<E, I, IE, LE>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> Iterator for Cursor<'a, E, I, IE, LE> {
    type Item = E;

    fn next(&mut self) -> Option<Self::Item> {
        // When the cursor is past the end, idx is an invalid value.
        if self.inner.idx == usize::MAX {
            return None;
        }

        // The cursor is at the end of the current element. Its a bit dirty doing this twice but
        // This will happen for a fresh cursor in an empty document, or when iterating using a
        // cursor made by some other means.
        if self.inner.idx >= unsafe { self.inner.node.as_ref() }.len_entries() {
            let has_next = self.inner.next_entry();
            if !has_next {
                self.inner.idx = usize::MAX;
                return None;
            }
        }

        let current = self.inner.get_raw_entry();
        // Move the cursor forward preemptively for the next call to next().
        let has_next = self.inner.next_entry();
        if !has_next {
            self.inner.idx = usize::MAX;
        }
        Some(current)
    }
}

impl<'a, E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> MutCursor<'a, E, I, IE, LE> {
    pub unsafe fn unchecked_from_raw(_tree: &mut Pin<Box<ContentTree<E, I, IE, LE>>>, cursor: UnsafeCursor<E, I, IE, LE>) -> Self {
        MutCursor {
            inner: cursor,
            marker: PhantomData
        }
    }

    // TODO: Implement from_raw as well.

    #[inline(always)]
    pub fn insert_notify<F>(&mut self, new_entry: E, notify: F)
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {

        unsafe {
            ContentTree::unsafe_insert(&mut self.inner, new_entry, notify);
        }
    }

    #[inline(always)]
    pub fn insert(&mut self, new_entry: E) {
        unsafe {
            ContentTree::unsafe_insert(&mut self.inner, new_entry, null_notify);
        }
    }

    #[inline(always)]
    pub fn replace_range_notify<N>(&mut self, new_entry: E, notify: N)
        where N: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {
        unsafe {
            ContentTree::unsafe_replace_range(&mut self.inner, new_entry, notify);
        }
    }

    #[inline(always)]
    pub fn replace_range(&mut self, new_entry: E) {
        unsafe {
            ContentTree::unsafe_replace_range(&mut self.inner, new_entry, null_notify);
        }
    }

    #[inline(always)]
    pub fn delete_notify<F>(&mut self, del_items: usize, notify: F)
        where F: FnMut(E, NonNull<NodeLeaf<E, I, IE, LE>>) {
        unsafe {
            ContentTree::unsafe_delete(&mut self.inner, del_items, notify);
        }
    }

    #[inline(always)]
    pub fn delete(&mut self, del_items: usize) {
        unsafe {
            ContentTree::unsafe_delete(&mut self.inner, del_items, null_notify);
        }
    }
}

impl<'a, E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> Deref for MutCursor<'a, E, I, IE, LE> {
    type Target = Cursor<'a, E, I, IE, LE>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        // Safe because cursor types are repr(transparent).
        unsafe { std::mem::transmute(self) }
    }
}

impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> ContentTree<E, I, IE, LE> {
    #[inline(always)]
    pub fn cursor_at_start(&self) -> Cursor<E, I, IE, LE> {
        unsafe {
            Cursor::unchecked_from_raw(self, self.unsafe_cursor_at_start())
        }
    }

    #[inline(always)]
    pub fn cursor_at_end(&self) -> Cursor<E, I, IE, LE> {
        unsafe {
            Cursor::unchecked_from_raw(self, self.unsafe_cursor_at_end())
        }
    }

    #[inline(always)]
    pub fn cursor_at_query<F, G>(&self, raw_pos: usize, stick_end: bool, offset_to_num: F, entry_to_num: G) -> Cursor<E, I, IE, LE>
    where F: Fn(I::IndexValue) -> usize, G: Fn(E) -> usize {
        unsafe {
            Cursor::unchecked_from_raw(self, self.unsafe_cursor_at_query(raw_pos, stick_end, offset_to_num, entry_to_num))
        }
    }

    // And the mut variants...
    #[inline(always)]
    pub fn mut_cursor_at_start<'a>(self: &'a mut Pin<Box<Self>>) -> MutCursor<'a, E, I, IE, LE> {
        unsafe {
            MutCursor::unchecked_from_raw(self, self.unsafe_cursor_at_start())
        }
    }

    #[inline(always)]
    pub fn mut_cursor_at_end<'a>(self: &'a mut Pin<Box<Self>>) -> MutCursor<'a, E, I, IE, LE> {
        unsafe {
            MutCursor::unchecked_from_raw(self, self.unsafe_cursor_at_end())
        }
    }

    #[inline(always)]
    pub fn mut_cursor_at_query<'a, F, G>(self: &'a mut Pin<Box<Self>>, raw_pos: usize, stick_end: bool, offset_to_num: F, entry_to_num: G) -> MutCursor<'a, E, I, IE, LE>
        where F: Fn(I::IndexValue) -> usize, G: Fn(E) -> usize {
        unsafe {
            MutCursor::unchecked_from_raw(self, self.unsafe_cursor_at_query(raw_pos, stick_end, offset_to_num, entry_to_num))
        }
    }
}

/// Iterator for all the items inside the entries. Unlike entry iteration we use the offset here.
#[derive(Debug)]
pub struct ItemIterator<'a, E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize>(pub Cursor<'a, E, I, IE, LE>);

impl<'a, E: EntryTraits + Searchable, I: TreeIndex<E>, const IE: usize, const LE: usize> Iterator for ItemIterator<'a, E, I, IE, LE> {
    type Item = E::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // I'll set idx to an invalid value
        if self.0.inner.idx == usize::MAX {
            None
        } else {
            let entry = self.0.get_raw_entry();
            let len = entry.len();
            let item = entry.at_offset(self.0.inner.offset);
            self.0.inner.offset += 1;

            if self.0.inner.offset >= len {
                // Skip to the next entry for the next query.
                let has_next = self.0.inner.next_entry();
                if !has_next {
                    // We're done.
                    self.0.inner.idx = usize::MAX;
                }
            }
            Some(item)
        }
    }
}