use std::cmp::Ordering;
use std::hint::unreachable_unchecked;

use rle::Searchable;

use super::*;
use std::ops::AddAssign;

// TODO: All these methods should be unsafe and have safe wrappers in safe_cursor.
impl<E: ContentTraits, I: TreeMetrics<E>, const IE: usize, const LE: usize> UnsafeCursor<E, I, IE, LE> {
    pub(crate) fn new(node: NonNull<NodeLeaf<E, I, IE, LE>>, idx: usize, offset: usize) -> Self {
        UnsafeCursor { node, idx, offset }
    }

    #[allow(clippy::mut_from_ref)] // Dirty.
    pub(crate) unsafe fn get_node_mut(&self) -> &mut NodeLeaf<E, I, IE, LE> {
        &mut *self.node.as_ptr()
    }

    // #[allow(unused)]
    // pub(super) fn get_node(&self) -> &NodeLeaf<E, I, IE, LE> {
    //     unsafe { self.node.as_ref() }
    // }

    /// Internal method for prev_entry and next_entry when we need to move laterally. This moves
    /// the cursor to the next / prev node in the tree, with no regard for the current position.
    ///
    /// Returns true if the move was successful, or false if we're at the first / last item in the
    /// tree.
    pub fn traverse_forward(&mut self) -> bool {
        let node = unsafe { self.node.as_ref() };
        if let Some(n) = node.next {
            self.node = n;
            self.idx = 0;
            self.offset = 0;
            true
        } else { false }
    }

    pub fn traverse_backwards(&mut self) -> bool {
        let node = unsafe { self.node.as_ref() };
        if let Some(n) = node.prev_leaf() {
            let node_ref = unsafe { n.as_ref() };
            self.node = n;
            self.idx = node_ref.len_entries() - 1;
            self.offset = node_ref.data[self.idx].len();
            true
        } else { false }
    }

    /// Move back to the previous entry. Returns true if it exists, otherwise
    /// returns false if we're at the start of the doc already.
    pub fn prev_entry_marker(&mut self, marker: Option<&mut I::Update>) -> bool {
        if self.idx > 0 {
            self.idx -= 1;
            self.offset = self.get_raw_entry().len();
            // println!("prev_entry get_entry returns {:?}", self.get_entry());
            true
        } else {
            if let Some(marker) = marker {
                unsafe { self.node.as_mut() }.flush_metric_update(marker);
            }
            self.traverse_backwards()
        }
    }

    pub fn prev_entry(&mut self) -> bool {
        self.prev_entry_marker(None)
    }

    /// Go to the next entry marker and update the (optional) flush marker.
    /// Returns true if successful, or false if we've reached the end of the document.
    #[inline(always)]
    pub fn next_entry_marker(&mut self, marker: Option<&mut I::Update>) -> bool {
        // TODO: Do this without code duplication of next/prev entry marker.
        unsafe {
            if self.idx + 1 < self.node.as_ref().num_entries as usize {
                self.idx += 1;
                self.offset = 0;
                true
            } else {
                if let Some(marker) = marker {
                    self.node.as_mut().flush_metric_update(marker);
                }
                self.traverse_forward()
            }
        }
    }

    #[inline(always)]
    pub fn next_entry(&mut self) -> bool {
        self.next_entry_marker(None)
    }

    pub unsafe fn count_pos_raw<Out, F, G, H>(&self, offset_to_num: F, entry_len: G, entry_len_at: H) -> Out
        where Out: AddAssign + Default, F: Fn(I::Value) -> Out, G: Fn(&E) -> Out, H: Fn(&E, usize) -> Out
    {
        // We're a cursor into an empty tree.
        if self.offset == usize::MAX { return Out::default(); }

        let node = self.node.as_ref();
        let mut pos = Out::default();

        if self.idx >= node.data.len() { unreachable_unchecked(); }

        // First find out where we are in the current node.
        for e in &node.data[0..self.idx] {
            pos += entry_len(e);
        }

        if self.offset != 0 {
            let e = &node.data[self.idx];
            pos += if self.offset < e.len() {
                entry_len_at(e, self.offset)
            } else {
                entry_len(e)
            };
        }

        // Ok, now iterate up to the root counting offsets as we go.
        let mut parent = node.parent;
        let mut node_ptr = NodePtr::Leaf(self.node);
        loop {
            match parent {
                ParentPtr::Root(_) => { break; }, // done.

                ParentPtr::Internal(n) => {
                    let node_ref = n.as_ref();
                    let idx = node_ref.find_child(node_ptr).unwrap();

                    for c in &node_ref.metrics[0..idx] {
                        pos += offset_to_num(*c);
                    }

                    // node_ptr = NodePtr::Internal(unsafe { NonNull::new_unchecked(node_ref as *const _ as *mut _) });
                    node_ptr = NodePtr::Internal(n);
                    parent = node_ref.parent;
                }
            }
        }

        pos
    }

    pub unsafe fn count_pos(&self) -> I::Value {
        self.count_pos_raw(|v| v, |e| {
            let mut v = I::Value::default();
            I::increment_offset(&mut v, e);
            v
        }, |e, offset| {
            // This is kinda awful, but hopefully the optimizer takes care of this
            let mut e = e.clone();
            e.truncate(offset);
            let mut v = I::Value::default();
            I::increment_offset(&mut v, &e);
            v
        })
    }

    // TODO: Check if its faster if this returns by copy or byref.
    /// Note this ignores the cursor's offset.
    pub fn get_raw_entry(&self) -> &E {
        assert_ne!(self.offset, usize::MAX, "Cannot get entry for a cursor to an empty list");
        let node = unsafe { self.node.as_ref() };
        &node.data[self.idx]
    }

    pub fn try_get_raw_entry(&self) -> Option<E> {
        let node = unsafe { self.node.as_ref() };
        if self.idx < node.len_entries() {
            Some(node.data[self.idx])
        } else { None }
    }

    pub fn get_raw_entry_mut(&mut self) -> &mut E {
        assert_ne!(self.offset, usize::MAX, "Cannot get entry for a cursor to an empty list");
        let node = unsafe { self.node.as_mut() };
        debug_assert!(self.idx < node.len_entries());
        &mut node.data[self.idx]
    }

    /// This is a terrible name. This method modifies a cursor at the end of an entry
    /// to be a cursor to the start of the next entry - potentially in the following leaf.
    ///
    /// Returns false if the resulting cursor location points past the end of the tree.
    pub(crate) fn roll_to_next_entry_internal<F: FnOnce(&mut Self)>(&mut self, f: F) -> bool {
        unsafe {
            if self.offset == usize::MAX {
                // The tree is empty.
                false
            } else {
                let node = self.node.as_ref();
                let seq_len = node.data[self.idx].len();

                debug_assert!(self.offset <= seq_len);

                if self.offset < seq_len { return true; }
                f(self);
                self.next_entry()
            }
        }
    }

    pub fn roll_to_next_entry(&mut self) -> bool {
        self.roll_to_next_entry_internal(|_| {})
    }

    pub fn roll_to_next_entry_marker(&mut self, marker: &mut I::Update) -> bool {
        self.roll_to_next_entry_internal(|cursor| {
            unsafe {
                cursor.node.as_mut().flush_metric_update(marker);
            }
        })
    }

    // pub fn roll_to_next_entry(&mut self) -> bool {
    //     unsafe {
    //         if self.offset == usize::MAX {
    //             // The tree is empty.
    //             false
    //         } else {
    //             let node = self.node.as_ref();
    //             let seq_len = node.data[self.idx].len();
    //
    //             debug_assert!(self.offset <= seq_len);
    //
    //             if self.offset < seq_len { return true; }
    //             self.next_entry()
    //         }
    //     }
    // }

    // TODO: This is inefficient in a loop.
    pub fn next_item(&mut self) -> bool {
        if !self.roll_to_next_entry() {
            return false;
        }
        self.offset += 1;
        true
    }

    pub fn move_forward_by_offset(&mut self, mut amt: usize, mut marker: Option<&mut I::Update>) {
        loop {
            let len_here = self.get_raw_entry().len();
            if self.offset + amt <= len_here {
                self.offset += amt;
                break;
            }
            amt -= len_here - self.offset;
            if !self.next_entry_marker(marker.take()) {
                panic!("Cannot move back before the start of the tree");
            }
        }
    }

    // How widely useful is this? This is optimized for small moves.
    pub fn move_back_by_offset(&mut self, mut amt: usize, mut marker: Option<&mut I::Update>) {
        while self.offset < amt {
            amt -= self.offset;
            self.offset = 0;
            if !self.prev_entry_marker(marker.take()) {
                panic!("Cannot move back before the start of the tree");
            }
        }
        self.offset -= amt;
    }

    /// This helper method attempts to minimize the size of the leaf around the cursor using
    /// append() methods, when possible.
    pub fn compress_node(&mut self) {
        if self.idx >= LE { return; } // For the optimizer.

        let node = unsafe { self.node.as_mut() };

        if self.idx >= node.len_entries() {
            // The cursor is pointing past the end of the node. Don't bother.
            return;
        }

        let mut merged = 0;

        for i in self.idx.max(1)..node.num_entries as usize {
            // Some optimizer fun.
            if i >= LE || i - 1 - merged >= LE || i - merged >= LE {
                unsafe { unreachable_unchecked(); }
            }

            let dest_idx = i - 1 - merged;

            if node.data[dest_idx].can_append(&node.data[i]) {
                if i == self.idx {
                    // This works because we only compress from the cursor onwards.
                    self.offset += node.data[dest_idx].len();
                    self.idx = dest_idx;
                }

                node.data[dest_idx].append(node.data[i]);
                merged += 1;
            } else if merged > 0 {
                node.data[i - merged] = node.data[i];
            } // TODO: Else consider aborting here.
        }
        node.num_entries -= merged as u8;
    }

    pub fn check(&self) {
        let node = unsafe { self.node.as_ref() };

        if node.num_entries == 0 {
            assert_eq!(self.idx, 0);
            assert_eq!(self.offset, usize::MAX);
        } else {
            assert!(self.idx < node.len_entries());
            assert!(self.offset <= node.data[self.idx].len());
        }
    }

    pub unsafe fn unsafe_cmp(&self, other: &Self) -> Ordering {
        if self.node == other.node {
            // We'll compare cursors directly.
            if self.idx == other.idx { self.offset.cmp(&other.offset) }
            else { self.idx.cmp(&other.idx) }
        } else {
            // Recursively walk up the trees to find the common ancestor.
            let mut n1 = NodePtr::Leaf(self.node);
            let mut n2 = NodePtr::Leaf(other.node);
            loop {
                // Look at the parents
                let p1 = n1.get_parent().unwrap_internal();
                let p2 = n2.get_parent().unwrap_internal();

                if p1 == p2 {
                    let node = p1.as_ref();
                    let idx1 = node.find_child(n1).unwrap();
                    let idx2 = node.find_child(n2).unwrap();
                    return idx1.cmp(&idx2);
                }

                // Otherwise keep traversing upwards!
                n1 = NodePtr::Internal(p1);
                n2 = NodePtr::Internal(p2);
            }
        }
    }
}

// An explicit implementation is needed because the derive macros bound too tightly
impl<E: ContentTraits, I: TreeMetrics<E>, const IE: usize, const LE: usize> PartialEq for UnsafeCursor<E, I, IE, LE> {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node && self.idx == other.idx && self.offset == other.offset
    }
}

impl<E: ContentTraits, I: TreeMetrics<E>, const IE: usize, const LE: usize> Eq for UnsafeCursor<E, I, IE, LE> {}


impl<E: ContentTraits + Searchable, I: TreeMetrics<E>, const IE: usize, const LE: usize> UnsafeCursor<E, I, IE, LE> {
    pub unsafe fn unsafe_get_item(&self) -> Option<E::Item> {
        // TODO: Optimize this. This is gross.
        let mut cursor = self.clone();
        if cursor.roll_to_next_entry() {
            Some(cursor.get_raw_entry().at_offset(cursor.offset))
        } else { None }
    }
}

// Unused.
// impl<E: EntryTraits + CRDTItem + Searchable, I: TreeIndex<E>, const IE: usize, const LE: usize> Cursor<E, I, IE, LE> {
//     /// Calculate and return the predecessor ID at the cursor. This is used to calculate the CRDT
//     /// location for an insert position.
//     ///
//     /// The cursor is not moved backwards (? mistake?) - so it must be stick_end: true.
//     pub fn tell_predecessor(mut self) -> Option<E::Item> {
//         while (self.offset == 0 && self.idx == 0) || self.get_raw_entry().is_deactivated() {
//             // println!("\nentry {:?}", self);
//             let exists = self.prev_entry();
//             if !exists { return None; }
//             // println!("-> prev {:?} inside {:#?}", self, unsafe { self.node.as_ref() });
//             // println!();
//         }
//
//         let entry = self.get_raw_entry(); // Shame this is called twice but eh.
//         Some(entry.at_offset(self.offset - 1))
//     }
// }

impl<E: ContentTraits + ContentLength, I: FindContent<E>, const IE: usize, const LE: usize> UnsafeCursor<E, I, IE, LE> {
    pub unsafe fn unsafe_count_content_pos(&self) -> usize {
        self.count_pos_raw(I::index_to_content, E::content_len, E::content_len_at_offset)
    }

    // pub unsafe fn count_pos_raw<Out, F, G, H>(&self, offset_to_num: F, entry_len: G, entry_len_at: H) -> Out
    //     where Out: AddAssign + Default, F: Fn(I::IndexValue) -> Out, G: Fn(&E) -> Out, H: Fn(&E, usize) -> Out
    pub fn move_forward_by_content(&mut self, _amt: usize) {
        todo!();
        // loop {
        //     let e = self.get_raw_entry();
        //     let len_here = e.content_len();
        //     let content_offset = if self.offset > 0 {
        //         e.content_len_at_offset(self.offset)
        //     } else { 0 };
        //
        //     if content_offset + amt <= len_here {
        //         self.offset += amt;
        //         break;
        //     }
        //     amt -= len_here - self.offset;
        //     if !self.next_entry_marker(marker.take()) {
        //         panic!("Cannot move back before the start of the tree");
        //     }
        // }
    }

    pub fn move_back_by_content(&mut self, _amt: usize) {
        todo!()
    }
}

impl<E: ContentTraits, I: FindOffset<E>, const IE: usize, const LE: usize> UnsafeCursor<E, I, IE, LE> {
    pub unsafe fn unsafe_count_offset_pos(&self) -> usize {
        self.count_pos_raw(I::index_to_offset, E::len, |_, off| off)

        // I::index_to_offset(self.old_count_pos())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testrange::TestRange;

    #[test]
    fn compare_cursors() {
        let mut tree = ContentTreeRaw::<TestRange, RawPositionMetricsU32, DEFAULT_IE, DEFAULT_LE>::new();

        let cursor = tree.unsafe_cursor_at_start();
        assert_eq!(cursor, cursor);

        tree.insert_at_start_notify(TestRange { id: 0, len: 1, is_activated: true }, null_notify);

        let c1 = tree.unsafe_cursor_at_start();
        let c2 = tree.unsafe_cursor_at_end();
        assert_eq!(unsafe { c1.unsafe_cmp(&c2) }, Ordering::Less);

        // Ok now lets add a bunch of junk to make sure the tree has a bunch of internal nodes
        for i in 0..1000 {
            tree.insert_at_start_notify(TestRange { id: i, len: 1, is_activated: true }, null_notify);
        }

        let c1 = tree.unsafe_cursor_at_start();
        let c2 = tree.unsafe_cursor_at_end();
        assert_eq!(unsafe { c1.unsafe_cmp(&c2) }, Ordering::Less);
    }

    #[test]
    fn empty_tree_has_empty_iter() {
        // Regression.
        let tree = ContentTreeRaw::<TestRange, RawPositionMetricsU32, DEFAULT_IE, DEFAULT_LE>::new();
        for _item in tree.raw_iter() {
            panic!("Found spurious item");
        }
    }
}