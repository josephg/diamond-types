use super::*;
use crate::range_tree::entry::CRDTItem;
use std::cmp::Ordering;
use std::hint::unreachable_unchecked;

// impl<'a, E: EntryTraits> Cursor<'a, E> {
impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> Cursor<E, I, IE, LE> {
    pub(super) fn new(node: NonNull<NodeLeaf<E, I, IE, LE>>, idx: usize, offset: usize) -> Self {
        // TODO: This is creating a cursor with 'static lifetime, which isn't really what we want.
        Cursor {
            node, idx, offset,
            // _marker: marker::PhantomData
        }
    }

    #[allow(clippy::mut_from_ref)] // Dirty.
    pub(super) unsafe fn get_node_mut(&self) -> &mut NodeLeaf<E, I, IE, LE> {
        &mut *self.node.as_ptr()
    }

    #[allow(unused)]
    pub(super) fn get_node(&self) -> &NodeLeaf<E, I, IE, LE> {
        unsafe { self.node.as_ref() }
    }

    /// Internal method for prev_entry and next_entry when we need to move laterally.
    pub fn traverse(&mut self, direction_forward: bool) -> bool {
        // println!("** traverse called {:?} {}", self, traverse_next);
        // idx is 0. Go up as far as we can until we get to an index that has room, or we hit the
        // root.
        let node = unsafe { self.node.as_ref() };

        let mut parent = node.parent;
        let mut node_ptr = NodePtr::Leaf(self.node);
        loop {
            match parent {
                ParentPtr::Root(_) => { return false; },
                ParentPtr::Internal(n) => {
                    let node_ref = unsafe { n.as_ref() };
                    // Time to find ourself up this tree.
                    let idx = node_ref.find_child(node_ptr).unwrap();
                    // println!("found myself at {}", idx);

                    let next_idx: Option<usize> = if direction_forward {
                        let next_idx = idx + 1;
                        // This would be much cleaner if I put a len field in NodeInternal instead.
                        // TODO: Consider using node_ref.count_children() instead of this mess.
                        if (next_idx < IE) && node_ref.children[next_idx].is_some() {
                            Some(next_idx)
                        } else { None }
                    } else if idx > 0 {
                        Some(idx - 1)
                    } else { None };
                    // println!("index {:?}", next_idx);

                    if let Some(next_idx) = next_idx {
                        // Whew - now we can descend down from here.
                        // println!("traversing laterally to {}", next_idx);
                        node_ptr = unsafe { node_ref.children[next_idx].as_ref().unwrap().as_ptr() };
                        break;
                    } else {
                        // idx is 0. Keep climbing that ladder!
                        node_ptr = NodePtr::Internal(unsafe { NonNull::new_unchecked(node_ref as *const _ as *mut _) });
                        parent = node_ref.parent;
                    }
                }
            }
        }

        // Now back down. We don't need idx here because we just take the first / last item in each
        // node going down the tree.
        loop {
            // println!("nodeptr {:?}", node_ptr);
            match node_ptr {
                NodePtr::Internal(n) => {
                    let node_ref = unsafe { n.as_ref() };
                    let next_idx = if direction_forward {
                        0
                    } else {
                        let num_children = node_ref.count_children();
                        assert!(num_children > 0);
                        num_children - 1
                    };
                    node_ptr = unsafe { node_ref.children[next_idx].as_ref().unwrap().as_ptr() };
                },
                NodePtr::Leaf(n) => {
                    // Finally.
                    let node_ref = unsafe { n.as_ref() };
                    assert!(node_ref.num_entries > 0);
                    // println!("landed in leaf {:#?}", node_ref);
                    self.node = n;
                    if direction_forward {
                        self.idx = 0;
                        self.offset = 0;
                    } else {
                        self.idx = node_ref.len_entries() - 1;
                        self.offset = node_ref.data[self.idx].len();
                        // println!("leaf {:?}", self);
                    }
                    return true;
                }
            }
        }
    }

    /// Move back to the previous entry. Returns true if it exists, otherwise
    /// returns false if we're at the start of the doc already.
    pub(super) fn prev_entry_marker(&mut self, marker: Option<&mut I::IndexUpdate>) -> bool {
        if self.idx > 0 {
            self.idx -= 1;
            self.offset = self.get_raw_entry().len();
            // println!("prev_entry get_entry returns {:?}", self.get_entry());
            true
        } else {
            if let Some(marker) = marker {
                unsafe { self.node.as_mut() }.flush_index_update(marker);
            }
            self.traverse(false)
        }
    }

    pub(super) fn prev_entry(&mut self) -> bool {
        self.prev_entry_marker(None)
    }

    /// Go to the next entry marker and update the (optional) flush marker.
    /// Returns true if successful, or false if we've reached the end of the document.
    pub(super) fn next_entry_marker(&mut self, marker: Option<&mut I::IndexUpdate>) -> bool {
        // TODO: Do this without code duplication of next/prev entry marker.
        unsafe {
            if self.idx + 1 < self.node.as_ref().num_entries as usize {
                self.idx += 1;
                self.offset = 0;
                true
            } else {
                if let Some(marker) = marker {
                    self.node.as_mut().flush_index_update(marker);
                }
                self.traverse(true)
            }
        }
    }

    pub fn next_entry(&mut self) -> bool {
        self.next_entry_marker(None)
    }

    pub fn count_pos(&self) -> I::IndexValue {
        let node = unsafe { self.node.as_ref() };
        
        // let mut pos: usize = 0;
        let mut pos = I::IndexValue::default();
        // First find out where we are in the current node.
        
        // TODO: This is a bit redundant - we could find out the local position
        // when we scan initially to initialize the cursor.
        for e in &node.data[0..self.idx] {
            I::increment_offset(&mut pos, e);
            // pos += e.content_len();
        }

        // This is pretty idiosyncratic.
        // let local_len = node.data[self.idx].content_len();
        // if local_len > 0 { pos += self.offset; }
        if self.offset != 0 { // The cursor might be pointing past the end of the last element.
            I::increment_offset_partial(&mut pos, &node.data[self.idx], self.offset);
        }

        // Ok, now iterate up to the root counting offsets as we go.

        let mut parent = node.parent;
        let mut node_ptr = NodePtr::Leaf(self.node);
        loop {
            match parent {
                ParentPtr::Root(_) => { break; }, // done.

                ParentPtr::Internal(n) => {
                    let node_ref = unsafe { n.as_ref() };
                    let idx = node_ref.find_child(node_ptr).unwrap();

                    for c in &node_ref.index[0..idx] {
                        pos += *c;
                    }

                    // node_ptr = NodePtr::Internal(unsafe { NonNull::new_unchecked(node_ref as *const _ as *mut _) });
                    node_ptr = NodePtr::Internal(n);
                    parent = node_ref.parent;
                }
            }
        }

        pos
    }

    // TODO: Check if its faster if this returns by copy or byref.
    /// Note this ignores the cursor's offset.
    pub fn get_raw_entry(&self) -> E {
        let node = unsafe { self.node.as_ref() };
        node.data[self.idx]
    }

    pub(super) fn get_raw_entry_mut(&mut self) -> &mut E {
        let node = unsafe { self.node.as_mut() };
        debug_assert!(self.idx < node.len_entries());
        &mut node.data[self.idx]
    }

    /// This is a terrible name. This method modifies a cursor at the end of an entry
    /// to be a cursor to the start of the next entry - potentially in the following leaf.
    ///
    /// Returns false if the resulting cursor location points past the end of the tree.
    pub(crate) fn roll_to_next_entry(&mut self) -> bool {
        unsafe {
            // This is pretty dirty to handle the case where the cursor already points past the end
            // of the document when this method is called.
            let node = self.node.as_ref();

            if self.idx >= node.num_entries as usize {
                self.next_entry()
            } else {
                let seq_len = node.data[self.idx].len();

                debug_assert!(self.offset <= seq_len);

                if self.offset < seq_len { return true; }
                self.offset = 0;
                self.idx += 1;
                if self.idx >= node.num_entries as usize {
                    self.next_entry()
                } else { true }
            }
        }
    }

    pub fn get_item(&self) -> Option<E::Item> {
        // TODO: Optimize this. This is gross.
        let mut cursor = self.clone();
        if cursor.roll_to_next_entry() {
            Some(cursor.get_raw_entry().at_offset(cursor.offset))
        } else { None }
    }

    // TODO: This is inefficient in a loop.
    pub fn next(&mut self) -> bool {
        if !self.roll_to_next_entry() {
            return false;
        }
        self.offset += 1;
        true
    }

    pub(super) fn move_forward_by(&mut self, mut amt: usize, mut marker: Option<&mut I::IndexUpdate>) {
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
    pub(super) fn move_back_by(&mut self, mut amt: usize, mut marker: Option<&mut I::IndexUpdate>) {
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
}

impl<E: EntryTraits + CRDTItem, I: TreeIndex<E>, const IE: usize, const LE: usize> Cursor<E, I, IE, LE> {
    /// Calculate and return the predecessor ID at the cursor. This is used to calculate the CRDT
    /// location for an insert position.
    ///
    /// The cursor is not moved backwards (? mistake?) - so it must be stick_end: true.
    pub fn tell_predecessor(mut self) -> Option<E::Item> {
        while (self.offset == 0 && self.idx == 0) || self.get_raw_entry().is_deactivated() {
            // println!("\nentry {:?}", self);
            let exists = self.prev_entry();
            if !exists { return None; }
            // println!("-> prev {:?} inside {:#?}", self, unsafe { self.node.as_ref() });
            // println!();
        }

        let entry = self.get_raw_entry(); // Shame this is called twice but eh.
        Some(entry.at_offset(self.offset - 1))
    }
}

/// NOTE: This comparator will panic when cursors from different range trees are compared.
///
/// Also beware: A cursor pointing to the end of an entry will be considered less than a cursor
/// pointing to the subsequent entry.
impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> Ord for Cursor<E, I, IE, LE> {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.node == other.node {
            // We'll compare cursors directly.
            if self.idx == other.idx { self.offset.cmp(&other.offset) }
            else { self.idx.cmp(&other.idx) }
        } else {
            // Recursively walk up the trees to find the common ancestor.
            unsafe {
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
}

impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> PartialOrd for Cursor<E, I, IE, LE> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


#[cfg(test)]
mod tests {
    use crate::range_tree::*;
    use crate::order::OrderSpan;

    #[test]
    fn compare_cursors() {
        let mut tree = RangeTree::<OrderSpan, RawPositionIndex, DEFAULT_IE, DEFAULT_LE>::new();

        let mut cursor = tree.cursor_at_start();
        assert_eq!(cursor, cursor);

        tree.insert(&mut cursor, OrderSpan { order: 0, len: 1 }, |_a, _b| {});

        let c1 = tree.cursor_at_start();
        let c2 = tree.cursor_at_end();
        assert!(c1 < c2);

        // Ok now lets add a bunch of junk to make sure the tree has a bunch of internal nodes
        for i in 0..1000 {
            tree.insert(&mut tree.cursor_at_start(), OrderSpan { order: i, len: 1 }, |_a, _b| {});
        }

        let c1 = tree.cursor_at_start();
        let c2 = tree.cursor_at_end();
        assert!(c1 < c2);
    }

    #[test]
    fn empty_tree_has_empty_iter() {
        // Regression.
        let tree = RangeTree::<OrderSpan, RawPositionIndex, DEFAULT_IE, DEFAULT_LE>::new();
        for _item in tree.iter() {
            panic!("Found spurious item");
        }
    }
}