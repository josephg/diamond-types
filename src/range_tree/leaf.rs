use super::*;
use std::ptr::NonNull;
use std::mem::take;

impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> NodeLeaf<E, I, IE, LE> {
    // Note this doesn't return a Pin<Box<Self>> like the others. At the point of creation, there's
    // no reason for this object to be pinned. (Is that a bad idea? I'm not sure.)
    pub(super) unsafe fn new(next: Option<NonNull<Self>>) -> Self {
        Self::new_with_parent(ParentPtr::Root(NonNull::dangling()), next)
    }

    pub(super) fn new_with_parent(parent: ParentPtr<E, I, IE, LE>, next: Option<NonNull<Self>>) -> Self {
        Self {
            parent,
            data: [E::default(); LE],
            num_entries: 0,
            _pin: PhantomPinned,
            next,
        }
    }

    // pub fn find2(&self, loc: CRDTLocation) -> (ClientSeq, Option<usize>) {
    //     let mut raw_pos: ClientSeq = 0;

    //     for i in 0..NUM_ENTRIES {
    //         let entry = self.data[i];
    //         if entry.is_invalid() { break; }

    //         if entry.loc.client == loc.client && entry.get_seq_range().contains(&loc.seq) {
    //             if entry.len > 0 {
    //                 raw_pos += loc.seq - entry.loc.seq;
    //             }
    //             return (raw_pos, Some(i));
    //         } else {
    //             raw_pos += entry.get_text_len()
    //         }
    //     }
    //     (raw_pos, None)
    // }

    // Find a given text offset within the node
    // Returns (index, offset within entry)
    pub fn find_offset<F>(&self, mut offset: usize, stick_end: bool, entry_to_num: F) -> Option<(usize, usize)>
        where F: Fn(E) -> usize {
        for i in 0..self.len_entries() {
            // if offset == 0 {
            //     return Some((i, 0));
            // }

            let entry: E = self.data[i];
            // if !entry.is_valid() { break; }

            // let text_len = entry.content_len();
            let entry_len = entry_to_num(entry);
            if offset < entry_len || (stick_end && entry_len == offset) {
                // Found it.
                return Some((i, offset));
            } else {
                offset -= entry_len
            }
        }

        if offset == 0 { // Special case for the first inserted element - we may never enter the loop.
            Some((self.len_entries(), 0))
        } else { None }
    }

    pub fn adjacent_leaf(&self, direction_forward: bool) -> Option<NonNull<Self>> {
        // println!("** traverse called {:?} {}", self, traverse_next);
        // idx is 0. Go up as far as we can until we get to an index that has room, or we hit the
        // root.
        if direction_forward && !cfg!(debug_assertions) {
            return self.next;
        }

        let mut parent = self.parent;
        let mut node_ptr = NodePtr::Leaf(unsafe { NonNull::new_unchecked(self as *const _ as *mut _) });

        loop {
            match parent {
                ParentPtr::Root(_) => { return None; },
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
                    return Some(n);
                }
            }
        }
    }

    // pub(super) fn actually_count_entries(&self) -> usize {
    //     self.data.iter()
    //     .position(|e| e.loc.client == CLIENT_INVALID)
    //     .unwrap_or(NUM_ENTRIES)
    // }
    pub(super) fn len_entries(&self) -> usize {
        self.num_entries as usize
    }

    // Recursively (well, iteratively) ascend and update all the counts along
    // the way up. TODO: Move this - This method shouldn't be in NodeLeaf.
    pub(super) fn update_parent_count(&mut self, amt: I::IndexUpdate) {
        if amt == I::IndexUpdate::default() { return; }

        let mut child = NodePtr::Leaf(unsafe { NonNull::new_unchecked(self) });
        let mut parent = self.parent;

        loop {
            match parent {
                ParentPtr::Root(mut r) => {
                    unsafe {
                        I::update_offset_by_marker(&mut r.as_mut().count, &amt);
                        // r.as_mut().count = r.as_ref().count.wrapping_add(amt as usize); }
                    }
                    break;
                },
                ParentPtr::Internal(mut n) => {
                    let idx = unsafe { n.as_mut() }.find_child(child).unwrap();
                    let c = &mut unsafe { n.as_mut() }.index[idx];
                    // :(
                    I::update_offset_by_marker(c, &amt);
                    // *c = c.wrapping_add(amt as u32);

                    // And recurse.
                    child = NodePtr::Internal(n);
                    parent = unsafe { n.as_mut() }.parent;
                },
            };
        }
    }

    pub(super) fn flush_index_update(&mut self, marker: &mut I::IndexUpdate) {
        // println!("flush {:?}", marker);
        let amt = take(marker);
        self.update_parent_count(amt);
    }

    pub(super) fn has_root_as_parent(&self) -> bool {
        self.parent.is_root()
    }

    pub(super) fn count_items(&self) -> I::IndexValue {
        if I::CAN_COUNT_ITEMS {
            // Optimization using the index. TODO: check if this is actually faster.
            match self.parent {
                ParentPtr::Root(root) => {
                    unsafe { root.as_ref() }.count
                }
                ParentPtr::Internal(node) => {
                    let child = NodePtr::Leaf(unsafe { NonNull::new_unchecked(self as *const _ as *mut _) });
                    let idx = unsafe { node.as_ref() }.find_child(child).unwrap();
                    unsafe { node.as_ref() }.index[idx]
                }
            }
        } else {
            // Count items the boring way. Hopefully this will optimize tightly.
            let mut val = I::IndexValue::default();
            for elem in self.data[..self.num_entries as usize].iter() {
                I::increment_offset(&mut val, elem);
            }
            val
        }
    }

    /// Remove a single item from the node
    pub(super) fn splice_out(&mut self, idx: usize) {
        self.data.copy_within(idx + 1..self.num_entries as usize, idx);
        self.num_entries -= 1;
    }

    pub(super) fn clear_all(&mut self) {
        // self.data[0..self.num_entries as usize].fill(E::default());
        self.num_entries = 0;
    }
}

impl<E: EntryTraits + Searchable, I: TreeIndex<E>, const IE: usize, const LE: usize> NodeLeaf<E, I, IE, LE> {
    pub fn find(&self, loc: E::Item) -> Option<Cursor<E, I, IE, LE>> {
        for i in 0..self.len_entries() {
            let entry: E = self.data[i];

            if let Some(offset) = entry.contains(loc) {
                debug_assert!(offset < entry.len());
                // let offset = if entry.is_insert() { entry_offset } else { 0 };

                return Some(Cursor::new(
                    unsafe { NonNull::new_unchecked(self as *const _ as *mut _) },
                    i,
                    offset
                ))
            }
        }
        None
    }
}
