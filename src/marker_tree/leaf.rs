use super::*;
use std::mem;
use std::ptr::{self, NonNull};

impl NodeLeaf {
    // fn new() -> MaybeUninit<Self> {
    //     let leaf: MaybeUninit<Self> = MaybeUninit::uninit();
    //     unsafe { (*leaf.as_mut_ptr()).data = [INVALID_ENTRY; NUM_ENTRIES]; }
    //     leaf
    // }

    // unsafe fn bake_parent(leaf: MaybeUninit<Self>, parent: ParentPtr) -> Self {
    //     (*leaf.as_mut_ptr()).parent = parent;
    //     leaf.assume_init()
    // }

    pub(super) unsafe fn new() -> Self {
        Self::new_with_parent(ParentPtr::Root(NonNull::dangling()))
    }

    pub(super) fn new_with_parent(parent: ParentPtr) -> Self {
        Self {
            parent,
            data: [Entry::default(); NUM_ENTRIES],
            len: 0,
            _drop: PrintDropLeaf,
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

    pub fn find(&self, loc: CRDTLocation) -> Option<Cursor> {
        for i in 0..(self.len as usize) {
            let entry = self.data[i];

            if entry.loc.client == loc.client && entry.get_seq_range().contains(&loc.seq) {
                let offset = if entry.len > 0 {
                    loc.seq - entry.loc.seq
                } else { 0 };

                return Some(Cursor::new(
                    unsafe { NonNull::new_unchecked(self as *const _ as *mut _) },
                    i,
                    offset
                ))
            }
        }
        None
    }

    // Find a given text offset within the node
    // Returns (index, offset within entry)
    pub fn find_offset(&self, mut offset: u32, stick_end: bool) -> Option<(usize, u32)> {
        for i in 0..(self.len as usize) {
            // if offset == 0 {
            //     return Some((i, 0));
            // }

            let entry = self.data[i];
            if entry.loc.client == CLIENT_INVALID { break; }

            let text_len = entry.get_text_len();
            if offset < text_len || (stick_end && text_len == offset) {
                // Found it.
                return Some((i, offset));
            } else {
                offset -= text_len
            }
        }

        if offset == 0 { // Specialcase for the first inserted element - we may never enter the loop.
            Some((self.len as usize, 0))
        } else { None }
    }

    // pub(super) fn actually_count_entries(&self) -> usize {
    //     self.data.iter()
    //     .position(|e| e.loc.client == CLIENT_INVALID)
    //     .unwrap_or(NUM_ENTRIES)
    // }
    pub(super) fn count_entries(&self) -> usize {
        self.len as usize
    }

    // Recursively (well, iteratively) ascend and update all the counts along
    // the way up. TODO: Make this private.
    pub(super) fn update_parent_count(&mut self, amt: i32) {
        let mut child = NodePtr::Leaf(unsafe { NonNull::new_unchecked(self) });
        let mut parent = self.parent;

        loop {
            match parent {
                ParentPtr::Root(mut r) => {
                    unsafe { r.as_mut().count = r.as_ref().count.wrapping_add(amt as u32); }
                    break;
                },
                ParentPtr::Internal(mut n) => {
                    let idx = unsafe { n.as_mut() }.find_child(child).unwrap();
                    let c = &mut unsafe { n.as_mut() }.data[idx].0;
                    // :(
                    *c = c.wrapping_add(amt as u32);

                    // And recurse.
                    child = NodePtr::Internal(n);
                    parent = unsafe { n.as_mut() }.parent;
                },
            };
        }
    }

    pub(super) fn split_at<F>(&mut self, idx: usize, notify: &mut F) -> NonNull<NodeLeaf>
        where F: FnMut(CRDTLocation, ClientSeq, NonNull<NodeLeaf>)
    {
        unsafe {
            let mut new_node = Self::new(); // The new node has a danging parent pointer
            let new_len = self.len as usize - idx;
            ptr::copy_nonoverlapping(&self.data[idx], &mut new_node.data[0], new_len);
            new_node.len = new_len as u8;
            
            // "zero" out the old entries
            // TODO(optimization): We're currently copying / moving everything
            // *after* idx. If idx is small, we could instead move everything
            // before idx - which would save a bunch of calls to notify and save
            // us needing to fix up a bunch of parent pointers.
            let mut stolen_length = 0;
            for e in &mut self.data[idx..self.len as usize] {
                stolen_length += e.get_text_len();
                *e = Entry::default();
            }
            self.len = idx as u8;

            // eprintln!("split_at idx {} self_entries {} stolel_len {} self {:?}", idx, self_entries, stolen_length, &self);

            let mut inserted_node = Box::pin(Node::Leaf(new_node));
            // Ultimately ret is the pointer to the new item we'll end up returning.
            let new_leaf_ptr = NonNull::new_unchecked(inserted_node.unwrap_leaf_mut());

            for e in &inserted_node.unwrap_leaf().data[0..new_len] {
                notify(e.loc, e.get_seq_len(), new_leaf_ptr);
            }

            // Ok now we need to walk up the tree trying to insert. At each step
            // we will try and insert inserted_node into parent next to old_node
            // (topping out at the head).
            let mut old_node: NodePtr = NodePtr::Leaf(NonNull::new_unchecked(self));
            let mut parent = self.parent;
            loop {
                // First try and simply emplace in the new element in the parent.
                if let ParentPtr::Internal(mut n) = parent {
                    let parent_ref = n.as_ref();
                    let count = parent_ref.count_children();
                    if count < MAX_CHILDREN {
                        // Great. Insert the new node into the parent and
                        // return.
                        *inserted_node.get_parent_mut() = ParentPtr::Internal(n);
                        
                        let old_idx = parent_ref.find_child(old_node).unwrap();
                        let new_idx = old_idx + 1;

                        let parent_ref = n.as_mut();
                        parent_ref.data[old_idx].0 -= stolen_length;
                        parent_ref.splice_in(new_idx, stolen_length, inserted_node);

                        // eprintln!("1");
                        return new_leaf_ptr;
                    }
                }

                // Ok so if we've gotten here we need to make a new internal
                // node filled with inserted_node, then move and all the goodies
                // from ParentPtr.
                match parent {
                    ParentPtr::Root(mut r) => {
                        // This is the simpler case. The new root will be a new
                        // internal node containing old_node and inserted_node.
                        let new_root = Box::pin(Node::Internal(NodeInternal::new_with_parent(ParentPtr::Root(r))));
                        let mut old_root = mem::replace(&mut r.as_mut().root, new_root);
                        
                        // *inserted_node.get_parent_mut() = parent_ptr;
                        
                        let count = r.as_ref().count;
                        let new_root_ref = r.as_mut().root.unwrap_internal_mut();
                        let parent_ptr = ParentPtr::Internal(NonNull::new_unchecked(new_root_ref));
                        
                        // Reassign parents for each node
                        *old_root.get_parent_mut() = parent_ptr;
                        *inserted_node.get_parent_mut() = parent_ptr;
                        
                        new_root_ref.data[0] = (count - stolen_length, Some(old_root));
                        new_root_ref.data[1] = (stolen_length, Some(inserted_node));

                        // r.as_mut().print_ptr_tree();
                        return new_leaf_ptr;
                    },
                    ParentPtr::Internal(mut n) => {
                        // And this is the complex case. We have MAX_CHILDREN+1
                        // items (in some order) to distribute between two
                        // internal nodes (one old, one new). Then we iterate up
                        // the tree.
                        let left_sibling = n.as_ref();
                        parent = left_sibling.parent; // For next iteration through the loop.
                        debug_assert!(left_sibling.count_children() == MAX_CHILDREN);

                        // let mut right_sibling = NodeInternal::new_with_parent(parent);
                        let mut right_sibling_box = Box::pin(Node::Internal(NodeInternal::new_with_parent(parent)));
                        let right_sibling = right_sibling_box.unwrap_internal_mut();
                        let old_idx = left_sibling.find_child(old_node).unwrap();
                        
                        let left_sibling = n.as_mut();
                        left_sibling.data[old_idx].0 -= stolen_length;
                        let mut new_stolen_length = 0;
                        // Dividing this into cases makes it easier to reason
                        // about.
                        if old_idx < MAX_CHILDREN/2 {
                            // Move all items from MAX_CHILDREN/2..MAX_CHILDREN
                            // into right_sibling, then splice inserted_node into
                            // old_parent.
                            for i in 0..MAX_CHILDREN/2 {
                                let (c, e) = mem::replace(&mut left_sibling.data[i + MAX_CHILDREN/2], (0, None));
                                if let Some(mut e) = e {
                                    *e.get_parent_mut() = ParentPtr::Internal(NonNull::new_unchecked(right_sibling));
                                    new_stolen_length += c;
                                    right_sibling.data[i] = (c, Some(e));
                                }

                            }

                            let new_idx = old_idx + 1;
                            *inserted_node.get_parent_mut() = ParentPtr::Internal(NonNull::new_unchecked(left_sibling));
                            left_sibling.splice_in(new_idx, stolen_length, inserted_node);
                        } else {
                            // The new element is in the second half of the
                            // group.
                            let new_idx = old_idx - MAX_CHILDREN/2 + 1;

                            *inserted_node.get_parent_mut() = ParentPtr::Internal(NonNull::new_unchecked(right_sibling));
                            let mut new_entry = (stolen_length, Some(inserted_node));
                            new_stolen_length = stolen_length;

                            let mut src = MAX_CHILDREN/2;
                            for dest in 0..=MAX_CHILDREN/2 {
                                if dest == new_idx {
                                    right_sibling.data[dest] = mem::take(&mut new_entry);
                                } else {
                                    let (c, e) = mem::replace(&mut left_sibling.data[src], (0, None));
                                    
                                    if let Some(mut e) = e {
                                        *e.get_parent_mut() = ParentPtr::Internal(NonNull::new_unchecked(right_sibling));
                                        new_stolen_length += c;
                                        right_sibling.data[dest] = (c, Some(e));
                                        src += 1;
                                    } else { break; }
                                }
                            }
                            debug_assert!(new_entry.1.is_none());
                        }

                        old_node = NodePtr::Internal(n);
                        inserted_node = right_sibling_box;
                        stolen_length = new_stolen_length;
                        // And iterate up the tree.
                    },
                };
            }
        }
    }
}
